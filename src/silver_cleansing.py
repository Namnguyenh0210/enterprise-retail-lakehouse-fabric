import uuid
from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, trim, upper, lit, current_timestamp,
    when, md5, concat_ws, length, row_number
)
from delta.tables import DeltaTable


class SilverConfig:
    PIPELINE_NAME = "PL_SILVER_CUSTOMER_CLEANSING"


class PipelineLogger:
    def __init__(self, pipeline_run_id: str):
        self.pipeline_run_id = pipeline_run_id

    def info(self, message: str) -> None:
        print(f"{datetime.now(timezone.utc).isoformat()} [THÔNG TIN] [{self.pipeline_run_id}] - {message}")

    def error(self, message: str, error_obj: Exception = None) -> None:
        err_msg = f"{message} | Chi tiết: {str(error_obj)}" if error_obj else message
        print(f"{datetime.now(timezone.utc).isoformat()} [LỖI] [{self.pipeline_run_id}] - {err_msg}")


class SilverCleansingProcessor:
    def __init__(self, spark: SparkSession, bronze_path: str, silver_path: str, quarantine_path: str):
        self.spark           = spark
        self.bronze_path     = bronze_path
        self.silver_path     = silver_path
        self.quarantine_path = quarantine_path
        self.run_id          = str(uuid.uuid4())
        self.batch_id        = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        self.logger          = PipelineLogger(self.run_id)
        self.metrics = {
            "total_read":          0,
            "valid_records":       0,
            "quarantined_records": 0,
            "duplicates_dropped":  0,
        }

    def process(self) -> None:
        # Làm sạch, chuẩn hóa, kiểm tra chất lượng (DQ), lọc trùng lặp và phân tách dữ liệu
        self.logger.info(f"Bắt đầu làm sạch dữ liệu Silver | Mã Batch: {self.batch_id}")
        try:
            df_bronze: DataFrame = self.spark.read.format("delta").load(self.bronze_path)
            df_bronze.cache()
            self.metrics["total_read"] = df_bronze.count()

            df_std = (
                df_bronze
                .withColumn("customer_city",            upper(trim(col("customer_city"))))
                .withColumn("customer_state",           upper(trim(col("customer_state"))))
                .withColumn("customer_zip_code_prefix", trim(col("customer_zip_code_prefix")))
                .withColumn("customer_region",
                    when(col("customer_state").isin("AC", "AP", "AM", "PA", "RO", "RR", "TO"), lit("Norte"))
                    .when(col("customer_state").isin("AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"), lit("Nordeste"))
                    .when(col("customer_state").isin("DF", "GO", "MT", "MS"), lit("Centro-Oeste"))
                    .when(col("customer_state").isin("ES", "MG", "RJ", "SP"), lit("Sudeste"))
                    .when(col("customer_state").isin("PR", "RS", "SC"), lit("Sul"))
                    .otherwise(lit("Unknown"))
                )
            )

            df_cdc = df_std.withColumn(
                "_record_hash",
                md5(concat_ws("||", col("customer_zip_code_prefix"), col("customer_city"), col("customer_state")))
            )

            dq_rules = (
                col("customer_id").isNotNull() & col("customer_unique_id").isNotNull() &
                (length(col("customer_state")) == 2) & col("customer_zip_code_prefix").rlike("^[0-9]+$")
            )

            df_valid      = df_cdc.filter(dq_rules).withColumn("_silver_update_ts", current_timestamp())
            df_quarantine = (
                df_cdc.filter(~dq_rules)
                .withColumn("_dq_error_reason",  lit("Lỗi DQ: ID rỗng, Mã bang != 2 ký tự, hoặc ZIP không hợp lệ"))
                .withColumn("_quarantine_ts",    current_timestamp())
                .withColumn("_quarantine_run_id", lit(self.run_id))
            )

            window_spec = Window.partitionBy("customer_id").orderBy(col("_created_at").desc())
            df_dedup = (
                df_valid
                .withColumn("rn", row_number().over(window_spec))
                .filter(col("rn") == 1)
                .drop("rn")
            )

            self.metrics["quarantined_records"] = df_quarantine.count()
            self.metrics["valid_records"]       = df_dedup.count()
            self.metrics["duplicates_dropped"]  = (
                self.metrics["total_read"] - self.metrics["valid_records"] - self.metrics["quarantined_records"]
            )

            self._upsert_silver(df_dedup)
            self._handle_quarantine(df_quarantine)
            self._optimize_storage()

            self.logger.info("Hoàn tất xử lý lớp Silver thành công.")
            self.logger.info(
                f"THỐNG KÊ - Đọc vào: {self.metrics['total_read']:,} | "
                f"Hợp lệ: {self.metrics['valid_records']:,} | "
                f"Cách ly lỗi: {self.metrics['quarantined_records']:,} | "
                f"Bỏ qua trùng lặp: {self.metrics['duplicates_dropped']:,}"
            )
            df_bronze.unpersist()

        except Exception as e:
            self.logger.error("Tiến trình Silver thất bại.", e)
            raise e

    def _upsert_silver(self, df_source: DataFrame) -> None:
        # MERGE vào Silver dựa trên CDC hash — chỉ update khi nội dung thực sự thay đổi
        if not DeltaTable.isDeltaTable(self.spark, self.silver_path):
            self.logger.info("Chưa có bảng Silver, tiến hành khởi tạo bảng mới.")
            df_source.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(self.silver_path)
            return

        self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        target_table = DeltaTable.forPath(self.spark, self.silver_path)

        (target_table.alias("target")
         .merge(df_source.alias("source"), "target.customer_id = source.customer_id")
         .whenMatchedUpdate(
             condition="target._record_hash != source._record_hash",
             set={
                 "customer_zip_code_prefix": "source.customer_zip_code_prefix",
                 "customer_city":            "source.customer_city",
                 "customer_state":           "source.customer_state",
                 "customer_region":          "source.customer_region",
                 "_record_hash":             "source._record_hash",
                 "_silver_update_ts":        "source._silver_update_ts",
                 "_batch_id":                "source._batch_id",
             }
         )
         .whenNotMatchedInsertAll()
         .execute())

    def _handle_quarantine(self, df_quarantine: DataFrame) -> None:
        # Lưu bản ghi lỗi DQ vào Quarantine để kỹ sư kiểm tra thủ công
        if self.metrics["quarantined_records"] > 0:
            df_quarantine.write.format("delta").mode("append").option("mergeSchema", "true").save(self.quarantine_path)

    def _optimize_storage(self) -> None:
        # Z-ORDER tối ưu hóa truy vấn vật lý theo customer_state
        self.logger.info("Chạy quy trình tối ưu hóa bộ nhớ cho bảng Silver.")
        self.spark.sql(f"OPTIMIZE delta.`{self.silver_path}` ZORDER BY (customer_state)")
