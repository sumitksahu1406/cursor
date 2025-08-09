from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def read_csv(
    spark: SparkSession,
    path: str,
    schema: Optional[StructType] = None,
    header: bool = True,
    infer_schema: bool = False,
    sep: str = ",",
) -> DataFrame:
    reader = spark.read.option("header", str(header).lower()).option("sep", sep)
    if schema is not None:
        reader = reader.schema(schema)
    else:
        reader = reader.option("inferSchema", str(infer_schema).lower())
    return reader.csv(path)


def write_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    df.write.mode(mode).parquet(path)


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)