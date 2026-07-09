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

# # 🥇 NB_21 — Gold Layer: Dim Customer (SCD Type 2)
#
# **Pipeline:** `PL_GOLD_DIM_CUSTOMER`
# **Source:** `Tables/silver_customers` (Delta)
# **Target:** `Tables/gold_dim_customer` (Delta — SCD Type 2)

# CELL ********************

RUN_OPTIMIZATION = True

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

SILVER_PATH = f"{LAKEHOUSE_BASE}/Tables/silver_customers"
GOLD_PATH   = f"{LAKEHOUSE_BASE}/Tables/gold_dim_customer"

print(f"🥈 SILVER_PATH : {SILVER_PATH}")
print(f"🥇 GOLD_PATH   : {GOLD_PATH}")

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
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, current_timestamp, lit, coalesce
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Logger & Processor

# CELL ********************

class PipelineLogger:
    def __init__(self, pipeline_run_id: str):
        self.pipeline_run_id = pipeline_run_id

    def info(self, message: str) -> None:
        print(f"{datetime.now(timezone.utc).isoformat()} [THÔNG TIN] [{self.pipeline_run_id}] - {message}")

    def error(self, message: str, error_obj: Exception = None) -> None:
        err_msg = f"{message} | Chi tiết: {str(error_obj)}" if error_obj else message
        print(f"{datetime.now(timezone.utc).isoformat()} [LỖI] [{self.pipeline_run_id}] - {err_msg}")


class GoldDimensionProcessor:
    def __init__(self, spark, silver_path: str, gold_path: str):
        self.spark       = spark
        self.silver_path = silver_path
        self.gold_path   = gold_path
        self.run_id      = str(uuid.uuid4())
        self.batch_id    = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        self.logger      = PipelineLogger(self.run_id)

    def process(self) -> None:
        self.logger.info(f"Bắt đầu xử lý Gold Dim (SCD Type 2) | Mã Batch: {self.batch_id}")
        try:
            df_silver: DataFrame = self.spark.read.format("delta").load(self.silver_path)

            df_dim_prep = (
                df_silver
                .withColumnRenamed("customer_id", "BusinessKey_CustomerID")
                .withColumn("SurrogateKey", sha2(col("BusinessKey_CustomerID"), 256))
                .select(
                    "SurrogateKey", "BusinessKey_CustomerID", "customer_unique_id",
                    "customer_zip_code_prefix", "customer_city", "customer_state",
                    "customer_region", "_record_hash"
                )
            )

            self._execute_scd2_merge(df_dim_prep)

            if RUN_OPTIMIZATION:
                self._optimize_storage()

            df_gold        = self.spark.read.format("delta").load(self.gold_path)
            total_records  = df_gold.count()
            active_records = df_gold.filter(col("IsCurrent") == True).count()

            self.logger.info("Hoàn tất xử lý lớp Gold thành công.")
            self.logger.info(
                f"THỐNG KÊ - Tổng số bản ghi Dimension: {total_records:,} | "
                f"Hồ sơ đang hoạt động (Active): {active_records:,}"
            )

        except Exception as e:
            self.logger.error("Tiến trình Gold thất bại.", e)
            raise e

    def _execute_scd2_merge(self, df_source: DataFrame) -> None:
        if not DeltaTable.isDeltaTable(self.spark, self.gold_path):
            self.logger.info("Chưa có bảng Gold Dimension, tiến hành khởi tạo bảng mới (Initial Load).")
            df_initial = (
                df_source
                .withColumn("Version",             lit(1))
                .withColumn("IsCurrent",           lit(True))
                .withColumn("EffectiveStartDate",  current_timestamp())
                .withColumn("EffectiveEndDate",    lit(None).cast("timestamp"))
                .withColumn("_audit_insert_batch", lit(self.batch_id))
            )
            df_initial.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(self.gold_path)
            return

        self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        target_table = DeltaTable.forPath(self.spark, self.gold_path)
        df_target    = target_table.toDF()

        df_current_versions = (
            df_target.filter(col("IsCurrent") == True)
            .select("BusinessKey_CustomerID", col("Version").alias("CurrentVersion"))
        )

        df_source_with_version = (
            df_source
            .join(df_current_versions, "BusinessKey_CustomerID", "left")
            .withColumn("NewVersion", coalesce(col("CurrentVersion") + 1, lit(1)))
            .drop("CurrentVersion")
        )

        df_target_current = df_target.filter(col("IsCurrent") == True)
        staged_updates = (
            df_source_with_version
            .join(df_target_current, "BusinessKey_CustomerID")
            .filter(df_source_with_version["_record_hash"] != df_target_current["_record_hash"])
            .select(df_source_with_version["*"])
            .withColumn("mergeKey", col("BusinessKey_CustomerID"))
        )

        staged_inserts = df_source_with_version.withColumn("mergeKey", lit(None).cast("string"))
        staged_data    = staged_updates.unionByName(staged_inserts)

        self.logger.info("Thực thi thao tác MERGE lưu lịch sử (SCD Type 2).")
        (target_table.alias("target")
         .merge(staged_data.alias("source"), "target.BusinessKey_CustomerID = source.mergeKey")
         .whenMatchedUpdate(
             condition="target.IsCurrent = true AND target._record_hash != source._record_hash",
             set={
                 "IsCurrent":           lit(False),
                 "EffectiveEndDate":    current_timestamp(),
                 "_audit_update_batch": lit(self.batch_id),
             }
         )
         .whenNotMatchedInsert(
             values={
                 "SurrogateKey":             "source.SurrogateKey",
                 "BusinessKey_CustomerID":   "source.BusinessKey_CustomerID",
                 "customer_unique_id":       "source.customer_unique_id",
                 "customer_zip_code_prefix": "source.customer_zip_code_prefix",
                 "customer_city":            "source.customer_city",
                 "customer_state":           "source.customer_state",
                 "customer_region":          "source.customer_region",
                 "_record_hash":             "source._record_hash",
                 "Version":                  "source.NewVersion",
                 "IsCurrent":                lit(True),
                 "EffectiveStartDate":        current_timestamp(),
                 "EffectiveEndDate":          lit(None).cast("timestamp"),
                 "_audit_insert_batch":       lit(self.batch_id),
             }
         )
         .execute())

    def _optimize_storage(self) -> None:
        self.logger.info("Chạy quy trình tối ưu hóa bộ nhớ cho bảng Gold.")
        self.spark.sql(f"OPTIMIZE delta.`{self.gold_path}` ZORDER BY (customer_region, customer_state)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Thực thi

# CELL ********************

processor = GoldDimensionProcessor(
    spark       = spark,
    silver_path = SILVER_PATH,
    gold_path   = GOLD_PATH,
)
processor.process()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Kiểm tra kết quả

# CELL ********************

df_gold = spark.read.format("delta").load(GOLD_PATH)

print(f"📊 Tổng Gold records  : {df_gold.count():,}")
print(f"✅ Active (IsCurrent) : {df_gold.filter(col('IsCurrent') == True).count():,}")
print(f"📜 History records    : {df_gold.filter(col('IsCurrent') == False).count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold.groupBy("Version", "IsCurrent").count().orderBy("Version").show()
df_gold.filter(col("IsCurrent") == True).show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold.filter(col("IsCurrent") == False) \
    .select("BusinessKey_CustomerID", "customer_city", "customer_state",
            "Version", "IsCurrent", "EffectiveStartDate", "EffectiveEndDate") \
    .show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
