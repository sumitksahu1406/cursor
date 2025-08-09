from pyspark.sql import SparkSession


def get_spark(app_name: str = "paypal_migration", shuffle_partitions: int = 1) -> SparkSession:
    """Create a local SparkSession configured for deterministic local execution."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    spark = builder.getOrCreate()
    return spark