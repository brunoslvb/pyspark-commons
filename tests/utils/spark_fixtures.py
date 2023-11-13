

from pyspark import SparkConf
from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope="session")
def spark_conf() -> SparkConf:
	return (
		SparkConf()
			.setAppName("Pytest-SparkApplication")
			.set("spark.default.parallelism", "1")
			.set("spark.dynamicAllocation.enable", "false")
			.set("spark.executor.cores", "1")
			.set("spark.executor.instances", "1")
			.set("spark.io.compression.codec", "lz4")
			.set("spark.rdd.compress", "false")
			.set("spark.sql.shuffle.partitions", "1")
			.set("spark.shuffle.compress", "false")
			.set("spark.sql.catalogImplementation", "hive")
	)

@pytest.fixture(scope="session")
def spark_session(spark_conf: SparkConf) -> SparkSession:
	spark_session = (
		SparkSession
			.builder
			.config(conf=spark_conf)
			.getOrCreate()
	)

	yield spark_session

	spark_session.stop()

