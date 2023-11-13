from typing import Union, List

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import struct, to_json, col, array, lit
from pyspark.sql.types import BinaryType


def join_dataframe(
    df1: DataFrame,
    df2: DataFrame,
    on: Union[str, list[str], Column, list[Column], None] = None,
    how: str = "inner",
    column_name: str = "DF2"
):
    """
    Join dataframes by putting all information from the second dataframe into a single column.

    Parameters
    ----------
    df1 : DataFrame
        Dataframe 1

    df2 : DataFrame
        Dataframe 2

    on: Union[str, list[str], Column, list[Column], None]
        Clause to join dataframes

    how: str
        How to join dataframes (inner, left, right, full ...)

    column_name: str
        Column that will contain dataframe 2 information
    """

    columns: List[str] = []
    columns.extend(map(lambda column: f"df1.{column}", df1.columns))
    columns.append(column_name)

    return (
        df1.alias("df1").join(df2.alias("df2"), on, how)
            .withColumn(column_name, struct([f"df2.{column}" for column in df2.columns]))
            .select(columns)
    )


def build_kafka_dataframe(
    dataframe_value: DataFrame,
    dataframe_header: DataFrame = None
) -> DataFrame:

    value = dataframe_value.select(
        to_json(
            struct(
                [col(column) for column in dataframe_value.columns]
            )
        ).alias("value")
    )

    if dataframe_header is not None:

        header = dataframe_header.first()

        return value.withColumn("headers", array(
            [struct(lit(column).alias("key"), lit(header[column]).cast(BinaryType()).alias("value")) for column in dataframe_header.columns]
        ))

    return value

