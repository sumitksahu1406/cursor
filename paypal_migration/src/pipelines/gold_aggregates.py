from pyspark.sql import functions as F

from src.utils.io import read_parquet, write_parquet
from src.utils.spark import get_spark


def run(
    silver_dir: str = "data/silver",
    gold_dir: str = "data/gold",
) -> None:
    spark = get_spark("gold_aggregates")

    tx = read_parquet(spark, f"{silver_dir}/transactions_enriched")

    tx_day = tx.withColumn("event_date", F.to_date("event_ts"))

    metrics = (
        tx_day.groupBy("event_date", "merchant_id", "merchant_name").agg(
            F.sum(F.when(F.col("status") == "CAPTURED", F.col("amount_in_usd")).otherwise(0.0)).alias("gmv_usd"),
            F.sum(F.when(F.col("status") == "AUTHORIZED", 1).otherwise(0)).alias("auth_count"),
            F.sum(F.when(F.col("status") == "CAPTURED", 1).otherwise(0)).alias("capture_count"),
            F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0)).alias("refund_count"),
            F.sum(F.when(F.col("status") == "CHARGEBACK", 1).otherwise(0)).alias("chargeback_count"),
        )
    )

    metrics = metrics.withColumn(
        "conversion_rate", F.when(F.col("auth_count") > 0, F.col("capture_count") / F.col("auth_count")).otherwise(None)
    ).withColumn(
        "refund_rate", F.when(F.col("capture_count") > 0, F.col("refund_count") / F.col("capture_count")).otherwise(None)
    ).withColumn(
        "chargeback_rate", F.when(F.col("capture_count") > 0, F.col("chargeback_count") / F.col("capture_count")).otherwise(None)
    )

    write_parquet(metrics, f"{gold_dir}/merchant_daily_metrics")


if __name__ == "__main__":
    run()