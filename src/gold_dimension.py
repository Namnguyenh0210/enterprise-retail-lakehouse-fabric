import uuid
from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sha2, current_timestamp, lit, coalesce
from delta.tables import DeltaTable


class GoldConfig:
    PIPELINE_NAME = "PL_GOLD_DIM_CUSTOMER"


class PipelineLogger:
    def __init__(self, pipeline_run_id: str):
        self.pipeline_run_id = pipeline_run_id

    def info(self, message: str) -> None:
        print(f"{datetime.now(timezone.utc).isoformat()} [THÔNG TIN] [{self.pipeline_run_id}] - {message}")

    def error(self, message: str, error_obj: Exception = None) -> None:
        err_msg = f"{message} | Chi tiết: {str(error_obj)}" if error_obj else message
        print(f"{datetime.now(timezone.utc).isoformat()} [LỖI] [{self.pipeline_run_id}] - {err_msg}")


class GoldDimensionProcessor:
    def __init__(self, spark: SparkSession, silver_path: str, gold_path: str):
        self.spark       = spark
        self.silver_path = silver_path
        self.gold_path   = gold_path
        self.run_id      = str(uuid.uuid4())
        self.batch_id    = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        self.logger      = PipelineLogger(self.run_id)

    def process(self) -> None:
        # Chuyển đổi Silver → Dimension chuẩn hóa cho Data Warehouse (SCD Type 2)
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
        # SCD Type 2: đóng bản ghi cũ (IsCurrent=False) và chèn bản ghi mới để lưu toàn bộ lịch sử thay đổi
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

        # staged_updates: customer đã tồn tại và hash thay đổi → cần đóng bản ghi cũ
        df_target_current = df_target.filter(col("IsCurrent") == True)
        staged_updates = (
            df_source_with_version
            .join(df_target_current, "BusinessKey_CustomerID")
            .filter(df_source_with_version["_record_hash"] != df_target_current["_record_hash"])
            .select(df_source_with_version["*"])
            .withColumn("mergeKey", col("BusinessKey_CustomerID"))
        )

        # staged_inserts: toàn bộ source với mergeKey=NULL → luôn vào nhánh NOT MATCHED → INSERT
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
        # Z-ORDER tối ưu hóa vật lý theo khu vực và bang cho BI Dashboard
        self.logger.info("Chạy quy trình tối ưu hóa bộ nhớ cho bảng Gold.")
        self.spark.sql(f"OPTIMIZE delta.`{self.gold_path}` ZORDER BY (customer_region, customer_state)")
