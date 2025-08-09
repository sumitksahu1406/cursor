from pyspark.sql import Row
from pyspark.sql import functions as F

from src.pipelines.silver_transform import run as run_silver
from src.utils.io import write_parquet, read_parquet


def test_dedup_and_fx_join(spark, tmp_path):
    bronze_dir = str(tmp_path / "bronze")
    silver_dir = str(tmp_path / "silver")

    # Create bronze transactions with dup transaction_id, later updated_at should win
    tx = spark.createDataFrame(
        [
            Row(transaction_id="t1", merchant_id="m1", user_id="u1", amount=100.0, currency="EUR", status="CAPTURED", event_ts="2024-01-05T12:00:00Z", updated_at="2024-01-05T12:05:00Z"),
            Row(transaction_id="t1", merchant_id="m1", user_id="u1", amount=100.0, currency="EUR", status="CAPTURED", event_ts="2024-01-05T12:00:00Z", updated_at="2024-01-05T12:10:00Z"),
            Row(transaction_id="t2", merchant_id="m1", user_id="u2", amount=200.0, currency="USD", status="AUTHORIZED", event_ts="2024-01-06T10:00:00Z", updated_at="2024-01-06T10:01:00Z"),
        ]
    ).withColumn("event_ts", F.to_timestamp("event_ts")).withColumn("updated_at", F.to_timestamp("updated_at"))

    fx = spark.createDataFrame(
        [
            Row(date="2024-01-04", from_currency="EUR", to_currency="USD", rate=1.1),
            Row(date="2024-01-05", from_currency="EUR", to_currency="USD", rate=1.2),
        ]
    ).withColumn("date", F.to_date("date"))

    merchants = spark.createDataFrame([Row(merchant_id="m1", merchant_name="Acme Inc")])
    users = spark.createDataFrame([Row(user_id="u1", country_code="DE", created_at="2020-01-01T00:00:00Z"), Row(user_id="u2", country_code="US", created_at="2020-02-02T00:00:00Z")]).withColumn("created_at", F.to_timestamp("created_at"))

    write_parquet(tx, f"{bronze_dir}/transactions")
    write_parquet(fx, f"{bronze_dir}/fx_rates")
    write_parquet(merchants, f"{bronze_dir}/merchants")
    write_parquet(users, f"{bronze_dir}/users")

    run_silver(bronze_dir=bronze_dir, silver_dir=silver_dir)

    out = read_parquet(spark, f"{silver_dir}/transactions_enriched")

    # Dedup: only one row for t1
    assert out.filter(F.col("transaction_id") == "t1").count() == 1

    # FX: EUR on 2024-01-05 should use 1.2
    row_t1 = out.filter(F.col("transaction_id") == "t1").select("amount_in_usd").collect()[0]
    assert abs(row_t1[0] - 120.0) < 1e-6