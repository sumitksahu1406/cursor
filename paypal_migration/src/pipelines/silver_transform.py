from pyspark.sql import functions as F
from pyspark.sql import Window

from src.utils.io import read_parquet, write_parquet
from src.utils.quality import assert_non_null
from src.utils.spark import get_spark


VALID_STATUSES = ["AUTHORIZED", "CAPTURED", "REFUNDED", "CHARGEBACK"]


def run(
    bronze_dir: str = "data/bronze",
    silver_dir: str = "data/silver",
    target_currency: str = "USD",
) -> None:
    spark = get_spark("silver_transform")

    tx = read_parquet(spark, f"{bronze_dir}/transactions")
    fx = read_parquet(spark, f"{bronze_dir}/fx_rates")
    merchants = read_parquet(spark, f"{bronze_dir}/merchants")
    users = read_parquet(spark, f"{bronze_dir}/users")

    # Filter to valid statuses
    tx = tx.filter(F.col("status").isin(VALID_STATUSES))

    # Deduplicate by transaction_id using latest updated_at
    window = Window.partitionBy("transaction_id").orderBy(F.col("updated_at").desc())
    tx = (
        tx.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    # Data quality: non-null keys and positive amount
    assert_non_null(tx, ["transaction_id", "merchant_id", "amount", "currency", "event_ts"])
    tx = tx.filter(F.col("amount") > 0)

    # Currency normalization to target_currency (e.g., USD)
    fx_to_target = (
        fx.filter(F.col("to_currency") == target_currency)
          .select(F.col("date"), F.col("from_currency").alias("currency"), F.col("rate").alias("fx_rate"))
    )

    tx = tx.withColumn("event_date", F.to_date("event_ts"))

    # Compute per-transaction latest FX rate up to event_date using non-equi join and window
    tx_key = tx.select("transaction_id", "currency", "event_date").alias("t")
    fx_alias = fx_to_target.alias("fx")
    joined = tx_key.join(
        fx_alias,
        (F.col("t.currency") == F.col("fx.currency")) & (F.col("fx.date") <= F.col("t.event_date")),
        "left",
    )
    w_fx = Window.partitionBy(F.col("t.transaction_id")).orderBy(F.col("fx.date").desc())
    tx_rate = (
        joined.withColumn("rn", F.row_number().over(w_fx))
              .filter(F.col("rn") == 1)
              .select(F.col("t.transaction_id").alias("transaction_id"), F.col("fx.fx_rate").alias("fx_rate_final"))
    )

    tx = tx.join(tx_rate, on="transaction_id", how="left")
    tx = tx.withColumn("amount_in_usd", F.col("amount") * F.col("fx_rate_final"))

    # Join reference data (drop duplicate audit columns to avoid duplicate names)
    merchants_sel = merchants.drop("ingestion_ts")
    users_sel = users.drop("ingestion_ts").select("user_id", "country_code")

    tx = (
        tx.join(merchants_sel, on="merchant_id", how="left")
          .join(users_sel, on="user_id", how="left")
    )

    write_parquet(tx, f"{silver_dir}/transactions_enriched")


if __name__ == "__main__":
    run()