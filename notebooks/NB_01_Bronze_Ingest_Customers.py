# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "Olist_Lakehouse",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 🥉 NB_01 — Bronze Layer: Ingest Customers
#
# **Pipeline:** `PL_BRONZE_CUSTOMER_INGESTION`
# **Source:** `Files/raw/customers/olist_customers_dataset.csv`
# **Target:** `Tables/bronze_customers` (Delta — Idempotent Append)
#
# > ⚠️ Không transform dữ liệu ở tầng này. Dữ liệu được lưu nguyên trạng (raw as-is).

# CELL ********************

RAW_FILE_NAME = "olist_customers_dataset.csv"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Cấu hình đường dẫn

# CELL ********************

# Thay <WORKSPACE_ID> và <LAKEHOUSE_ID> bằng giá trị thực từ Fabric portal → Lakehouse → Settings
LAKEHOUSE_BASE = "abfss://<WORKSPACE_ID>@onelake.dfs.fabric.microsoft.com/<LAKEHOUSE_ID>"

RAW_PATH    = f"{LAKEHOUSE_BASE}/Files/raw/customers/{RAW_FILE_NAME}"
BRONZE_PATH = f"{LAKEHOUSE_BASE}/Tables/bronze_customers"

print(f"📂 RAW_PATH    : {RAW_PATH}")
print(f"📦 BRONZE_PATH : {BRONZE_PATH}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Import

# CELL ********************

import uuid
from datetime import datetime, timezone
from pyspark.sql.functions import (
    col, lit, current_timestamp, current_date,
    input_file_name, element_at, split
)
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Config & Logger

# CELL ********************

class BronzeConfig:
    SOURCE_SYSTEM    = "olist_ecommerce"
    PIPELINE_NAME    = "PL_BRONZE_CUSTOMER_INGESTION"
    LOAD_TYPE        = "INCREMENTAL"
    DELIMITER        = ","
    EXPECTED_COLUMNS = [
        "customer_id", "customer_unique_id",
        "customer_zip_code_prefix", "customer_city", "customer_state"
    ]


class PipelineLogger:
    def __init__(self, pipeline_run_id: str):
        self.pipeline_run_id = pipeline_run_id

    def info(self, message: str) -> None:
        print(f"{datetime.now(timezone.utc).isoformat()} [THÔNG TIN] [{self.pipeline_run_id}] - {message}")

    def error(self, message: str, error_obj: Exception = None) -> None:
        err_msg = f"{message} | Chi tiết: {str(error_obj)}" if error_obj else message
        print(f"{datetime.now(timezone.utc).isoformat()} [LỖI] [{self.pipeline_run_id}] - {err_msg}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Bronze Ingestion Processor

# CELL ********************

class BronzeIngestionProcessor:
    def __init__(self, spark, raw_path: str, bronze_path: str):
        self.spark       = spark
        self.raw_path    = raw_path
        self.bronze_path = bronze_path
        self.run_id      = str(uuid.uuid4())
        self.batch_id    = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        self.logger      = PipelineLogger(self.run_id)

    def _validate_file(self) -> None:
        self.logger.info(f"Đang xác thực tệp dữ liệu: {self.raw_path}")
        df_check = self.spark.read.text(self.raw_path).limit(1)

        if df_check.rdd.isEmpty():
            raise ValueError("Kiểm tra thất bại: Tệp dữ liệu hoàn toàn rỗng.")

        header_row = df_check.collect()[0][0]

        if BronzeConfig.DELIMITER not in header_row:
            raise ValueError(f"Kiểm tra thất bại: Sai ký tự phân cách. Yêu cầu dấu '{BronzeConfig.DELIMITER}'")

        actual_cols = [c.replace('"', '').strip() for c in header_row.split(BronzeConfig.DELIMITER)]
        missing = [c for c in BronzeConfig.EXPECTED_COLUMNS if c not in actual_cols]
        if missing:
            raise ValueError(f"Kiểm tra thất bại: Thiếu các cột bắt buộc: {missing}")

    def _ensure_idempotency(self) -> None:
        if DeltaTable.isDeltaTable(self.spark, self.bronze_path):
            self.logger.info("Đảm bảo tính lũy đẳng: Xóa các bản ghi cũ được nạp từ tệp này trước đó.")
            dt = DeltaTable.forPath(self.spark, self.bronze_path)
            file_name = self.raw_path.split('/')[-1]
            dt.delete(f"_source_file_name = '{file_name}'")

    def process(self) -> None:
        self.logger.info(f"Bắt đầu tiến trình nạp dữ liệu Bronze | Mã Batch: {self.batch_id}")
        try:
            self._validate_file()

            df_raw = (
                self.spark.read
                .option("header", "true")
                .option("quote",  '"')
                .option("escape", '"')
                .csv(self.raw_path)
            )

            df_bronze = (
                df_raw
                .withColumn("_pipeline_run_id", lit(self.run_id))
                .withColumn("_batch_id",         lit(self.batch_id))
                .withColumn("_source_system",    lit(BronzeConfig.SOURCE_SYSTEM))
                .withColumn("_load_type",        lit(BronzeConfig.LOAD_TYPE))
                .withColumn("_created_at",       current_timestamp())
                .withColumn("_ingestion_date",   current_date())
                .withColumn("_source_file_path", input_file_name())
                .withColumn("_source_file_name", element_at(split(input_file_name(), "/"), -1))
            )

            self._ensure_idempotency()

            df_bronze.write.format("delta").mode("append").save(self.bronze_path)

            total_inserted = (
                self.spark.read.format("delta").load(self.bronze_path)
                .filter(col("_pipeline_run_id") == self.run_id)
                .count()
            )
            self.logger.info(f"Hoàn tất nạp Bronze. Đã thêm thành công {total_inserted:,} dòng vào lô hiện tại.")

        except Exception as e:
            self.logger.error("Tiến trình Bronze thất bại nghiêm trọng.", e)
            raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Thực thi

# CELL ********************

processor = BronzeIngestionProcessor(
    spark       = spark,
    raw_path    = RAW_PATH,
    bronze_path = BRONZE_PATH,
)
processor.process()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Kiểm tra kết quả

# CELL ********************

df_bronze_check = spark.read.format("delta").load(BRONZE_PATH)

print(f"📊 Tổng số dòng trong Bronze: {df_bronze_check.count():,}")
df_bronze_check.groupBy("_batch_id", "_source_file_name").count().orderBy("_batch_id").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze_check.printSchema()
df_bronze_check.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
