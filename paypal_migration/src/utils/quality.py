from typing import Iterable

from pyspark.sql import DataFrame


def assert_non_null(df: DataFrame, columns: Iterable[str]) -> None:
    for column in columns:
        null_count = df.filter(df[column].isNull()).count()
        if null_count > 0:
            raise AssertionError(f"Column '{column}' has {null_count} nulls")


def assert_unique(df: DataFrame, columns: Iterable[str]) -> None:
    total = df.count()
    distinct_total = df.select(list(columns)).dropDuplicates().count()
    if total != distinct_total:
        raise AssertionError(
            f"Columns {list(columns)} are not unique: total={total}, distinct={distinct_total}"
        )


def assert_values_in_set(df: DataFrame, column: str, allowed_values: Iterable[str]) -> None:
    invalid = df.select(column).distinct().exceptAll(
        df.sparkSession.createDataFrame([(v,) for v in allowed_values], df.schema[column].dataType.simpleString())
    )
    if invalid.count() > 0:
        values = [row[0] for row in invalid.collect()]
        raise AssertionError(
            f"Column '{column}' has invalid values not in {set(allowed_values)}: {values}"
        )