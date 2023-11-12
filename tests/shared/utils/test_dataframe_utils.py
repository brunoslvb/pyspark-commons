import json

import pytest
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType, IntegerType

from tests.utils.spark_fixtures import spark_session, spark_conf
from src.shared.utils.dataframe_utils import join_dataframe, build_kafka_dataframe


class TestDataframeUtils:

    class TestJoinDataframe:

        @pytest.fixture(scope="class")
        def mock_1(self, spark_session):
            return spark_session.createDataFrame([
                (1, "Name 1"),
                (2, "Name 2")
            ], ["ID", "NAME"])

        @pytest.fixture(scope="class")
        def mock_2(self, spark_session):
            return spark_session.createDataFrame([
                (1, 18),
                (3, 29)
            ], ["ID", "AGE"])

        def test_join_dataframe_inner(self, mock_1, mock_2):
            dataframe_joined = join_dataframe(
                mock_1, mock_2, ["ID"]
            )

            row = dataframe_joined.first()

            assert dataframe_joined.count() == 1
            assert dataframe_joined.columns == ["ID", "NAME", "DF2"]
            assert row["ID"] == 1
            assert row["NAME"] == "Name 1"
            assert row["DF2"]["ID"] == 1
            assert row["DF2"]["AGE"] == 18

        def test_join_dataframe_left(self, mock_1, mock_2):

            column_name = "TEST"

            dataframe_joined = join_dataframe(
                mock_1, mock_2, ["ID"], "left", column_name
            )

            rows = dataframe_joined.collect()

            assert dataframe_joined.count() == 2
            assert dataframe_joined.columns == ["ID", "NAME", column_name]
            assert rows[0]["ID"] == 1
            assert rows[0]["NAME"] == "Name 1"
            assert rows[0][column_name]["ID"] == 1
            assert rows[0][column_name]["AGE"] == 18
            assert rows[1]["ID"] == 2
            assert rows[1]["NAME"] == "Name 2"
            assert rows[1][column_name]["ID"] is None
            assert rows[1][column_name]["AGE"] is None

        def test_join_dataframe_right(self, mock_1, mock_2):
            column_name = "TEST"

            dataframe_joined = join_dataframe(
                mock_1, mock_2, ["ID"], "right", column_name
            )

            rows = dataframe_joined.collect()

            assert dataframe_joined.count() == 2
            assert dataframe_joined.columns == ["ID", "NAME", column_name]
            assert rows[0]["ID"] == 1
            assert rows[0]["NAME"] == "Name 1"
            assert rows[0][column_name]["ID"] == 1
            assert rows[0][column_name]["AGE"] == 18
            assert rows[1]["ID"] is None
            assert rows[1]["NAME"] is None
            assert rows[1][column_name]["ID"] == 3
            assert rows[1][column_name]["AGE"] == 29

    class TestBuildKafkaDataframe:

        @pytest.fixture(scope="class")
        def dataframe_value(self, spark_session):
            schema = StructType([
                StructField("userId", LongType()),
                StructField("name", StringType()),
                StructField("addresses", ArrayType(
                    StructType([
                        StructField("street", StringType()),
                        StructField("zipcode", IntegerType())
                    ])
                )),
                StructField("emergencyContact", StructType([
                    StructField("name", StringType()),
                    StructField("value", StringType())
                ]))
            ])

            return spark_session.createDataFrame([
                (1, "Name 1", [("Street 1", 11111000)], (("Contact 1", "11111-1111"))),
                (2, "Name 2", [("Street 1", 22222000), ("Street 2", 33333000)], (("Contact 1", "22222-2222")))
            ], schema)

        @pytest.fixture(scope="class")
        def dataframe_header(self, spark_session):
            schema = StructType([
                StructField("ce_correlationId", StringType()),
                StructField("ce_source", StringType()),
                StructField("ce_specversion", StringType())
            ])

            return spark_session.createDataFrame([
                ("60b8a0a238ab97ca3c6346d7f2bc2c6f", "example", "1.0")
            ], schema)

        @pytest.fixture(scope="class")
        def expected_value_1(self):
            return {
                "userId": 1,
                "name": "Name 1",
                "addresses": [
                    {
                        "street": "Street 1",
                        "zipcode": 11111000
                    }
                ],
                "emergencyContact": {
                    "name": "Contact 1",
                    "value": "11111-1111"
                }
            }

        @pytest.fixture(scope="class")
        def expected_value_2(self):
            return {
                "userId": 2,
                "name": "Name 2",
                "addresses": [
                    {
                        "street": "Street 1",
                        "zipcode": 22222000
                    },
                    {
                        "street": "Street 2",
                        "zipcode": 33333000
                    }
                ],
                "emergencyContact": {
                    "name": "Contact 1",
                    "value": "22222-2222"
                }
            }

        @pytest.fixture(scope="class")
        def expected_header(self):
            return [
                {
                    "key": "ce_correlationId",
                    "value": b"60b8a0a238ab97ca3c6346d7f2bc2c6f"
                },
                {
                    "key": "ce_source",
                    "value": b"example"
                },
                {
                    "key": "ce_specversion",
                    "value": b"1.0"
                }
            ]

        def test_build_kafka_dataframe_without_header(self, dataframe_value, expected_value_1, expected_value_2):

            dataframe_built = build_kafka_dataframe(dataframe_value)

            rows = dataframe_built.collect()

            assert dataframe_built.columns == ["value"]
            assert dataframe_built.count() == dataframe_value.count()
            assert json.loads(rows[0]["value"]) == expected_value_1
            assert json.loads(rows[1]["value"]) == expected_value_2

        def test_build_kafka_dataframe_with_header(self, dataframe_value, dataframe_header, expected_value_1, expected_value_2, expected_header):

            dataframe_built = build_kafka_dataframe(dataframe_value, dataframe_header)

            rows = dataframe_built.collect()

            assert dataframe_built.columns == ["value", "headers"]
            assert dataframe_built.count() == dataframe_value.count()
            assert json.loads(rows[0]["value"]) == expected_value_1
            assert [row.asDict() for row in rows[0]["headers"]] == expected_header
            assert json.loads(rows[1]["value"]) == expected_value_2
            assert [row.asDict() for row in rows[1]["headers"]] == expected_header



