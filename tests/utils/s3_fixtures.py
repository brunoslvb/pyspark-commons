import boto3
import pytest
from botocore.client import BaseClient

from src.shared.utils.s3_utils import S3Utils


@pytest.fixture(scope="session")
def s3_client() -> BaseClient:
	return boto3.client("s3", region_name="us-east-1")


@pytest.fixture(scope="session")
def s3_utils() -> S3Utils:
	return S3Utils()

