from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.utils.io import read_csv, write_parquet
from src.utils.spark import get_spark


TRANSACTIONS_SCHEMA = T.StructType(
    [
        T.StructField("transaction_id", T.StringType(), False),
        T.StructField("merchant_id", T.StringType(), False),
        T.StructField("user_id", T.StringType(), True),
        T.StructField("amount", T.DoubleType(), False),
        T.StructField("currency", T.StringType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("event_ts", T.StringType(), False),  # ISO string
        T.StructField("updated_at", T.StringType(), False),  # ISO string for deduping
    ]
)

FX_SCHEMA = T.StructType(
    [
        T.StructField("date", T.StringType(), False),  # YYYY-MM-DD
        T.StructField("from_currency", T.StringType(), False),
        T.StructField("to_currency", T.StringType(), False),
        T.StructField("rate", T.DoubleType(), False),
    ]
)

MERCHANTS_SCHEMA = T.StructType(
    [
        T.StructField("merchant_id", T.StringType(), False),
        T.StructField("merchant_name", T.StringType(), False),
    ]
)

USERS_SCHEMA = T.StructType(
    [
        T.StructField("user_id", T.StringType(), False),
        T.StructField("country_code", T.StringType(), True),
        T.StructField("created_at", T.StringType(), True),
    ]
)


def run(
    input_dir: str = "data/input",
    bronze_dir: str = "data/bronze",
) -> None:
    spark = get_spark("bronze_ingest")

    transactions = read_csv(spark, f"{input_dir}/transactions.csv", schema=TRANSACTIONS_SCHEMA)
    fx_rates = read_csv(spark, f"{input_dir}/fx_rates.csv", schema=FX_SCHEMA)
    merchants = read_csv(spark, f"{input_dir}/merchants.csv", schema=MERCHANTS_SCHEMA)
    users = read_csv(spark, f"{input_dir}/users.csv", schema=USERS_SCHEMA)

    ingestion_ts = F.current_timestamp()

    transactions = (
        transactions
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("ingestion_ts", ingestion_ts)
    )

    fx_rates = (
        fx_rates
        .withColumn("date", F.to_date("date"))
        .withColumn("ingestion_ts", ingestion_ts)
    )

    merchants = merchants.withColumn("ingestion_ts", ingestion_ts)
    users = (
        users
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("ingestion_ts", ingestion_ts)
    )

    write_parquet(transactions, f"{bronze_dir}/transactions")
    write_parquet(fx_rates, f"{bronze_dir}/fx_rates")
    write_parquet(merchants, f"{bronze_dir}/merchants")
    write_parquet(users, f"{bronze_dir}/users")


if __name__ == "__main__":
    run()