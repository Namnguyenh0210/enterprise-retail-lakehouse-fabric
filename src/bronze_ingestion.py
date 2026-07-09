import uuid
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, current_date,
    input_file_name, element_at, split
)
from delta.tables import DeltaTable


class BronzeConfig:
    SOURCE_SYSTEM    = "olist_ecommerce"
    PIPELINE_NAME    = "PL_BRONZE_CUSTOMER_INGESTION"
    LOAD_TYPE        = "INCREMENTAL"
    DELIMITER        = ","
    EXPECTED_COLUMNS = [
        "customer_id", "customer_unique_id",
        "customer_zip_code_prefix", "customer_city", "customer_state",
    ]


class PipelineLogger:
    def __init__(self, pipeline_run_id: str):
        self.pipeline_run_id = pipeline_run_id

    def info(self, message: str) -> None:
        print(f"{datetime.now(timezone.utc).isoformat()} [THÔNG TIN] [{self.pipeline_run_id}] - {message}")

    def error(self, message: str, error_obj: Exception = None) -> None:
        err_msg = f"{message} | Chi tiết: {str(error_obj)}" if error_obj else message
        print(f"{datetime.now(timezone.utc).isoformat()} [LỖI] [{self.pipeline_run_id}] - {err_msg}")


class BronzeIngestionProcessor:
    def __init__(self, spark: SparkSession, raw_path: str, bronze_path: str):
        self.spark       = spark
        self.raw_path    = raw_path
        self.bronze_path = bronze_path
        self.run_id      = str(uuid.uuid4())
        self.batch_id    = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        self.logger      = PipelineLogger(self.run_id)

    def _validate_file(self) -> None:
        # Fail-Fast: kiểm tra file không rỗng, đúng delimiter và đủ cột trước khi xử lý
        self.logger.info(f"Đang xác thực tệp dữ liệu: {self.raw_path}")
        df_check = self.spark.read.text(self.raw_path).limit(1)

        if df_check.rdd.isEmpty():
            raise ValueError("Kiểm tra thất bại: Tệp dữ liệu hoàn toàn rỗng.")

        header_row = df_check.collect()[0][0]

        if BronzeConfig.DELIMITER not in header_row:
            raise ValueError(
                f"Kiểm tra thất bại: Sai ký tự phân cách. Yêu cầu dấu '{BronzeConfig.DELIMITER}'"
            )

        actual_cols = [c.replace('"', "").strip() for c in header_row.split(BronzeConfig.DELIMITER)]
        missing = [c for c in BronzeConfig.EXPECTED_COLUMNS if c not in actual_cols]
        if missing:
            raise ValueError(f"Kiểm tra thất bại: Thiếu các cột bắt buộc: {missing}")

    def _ensure_idempotency(self) -> None:
        # Xóa bản ghi cũ từ cùng file để pipeline có thể chạy lại an toàn
        if DeltaTable.isDeltaTable(self.spark, self.bronze_path):
            self.logger.info("Đảm bảo tính lũy đẳng: Xóa các bản ghi cũ được nạp từ tệp này trước đó.")
            dt = DeltaTable.forPath(self.spark, self.bronze_path)
            file_name = self.raw_path.split("/")[-1]
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
