import logging
import os
from logging import Logger
from typing import List, Dict, Any

import boto3
from botocore.client import BaseClient
from botocore.paginate import PageIterator


class S3Utils:

    def __init__(self) -> None:
        self.__application_logger: Logger = logging.getLogger('S3Utils')
        self.__s3_client: BaseClient = self.__boto_workaround()[1]('s3')

    def __boto_workaround(self):
        """
        Boto does not work when it is an imported from a zipfile. This method
        unzips it to a tmp dir and provides session access with the correctly-configured data search paths.

        Solution found here: https://github.com/boto/boto3/issues/749

        To perform tests using boto3 locally and outside of a zip file, set ssl to false as shown below:

        `
        sess = boto3.Session()
        return (
            lambda name: sess.resource(name, use_ssl=False, verify=False),
            lambda name: sess.client(name, use_ssl=False, verify=False)
        )
        `
        """

        import zipfile

        def get_zip_path(path):
            if path == os.path.sep or path == "C:\\":
                return None
            elif zipfile.is_zipfile(path):
                return path
            else:
                return get_zip_path(os.path.dirname(path))

        base_extract_path = "/tmp/boto_zip_cache"
        sess = boto3.Session()
        kwargs = {}
        zp = get_zip_path(boto3.__file__)

        region_name = "us-east-1"

        self.__application_logger.info("Boto region is: %s", region_name)

        if zp:
            self.__application_logger.info("Boto was instance from uncompressed file")
            with zipfile.ZipFile(zp) as z:
                for n in z.namelist():
                    if n.startswith("boto"):
                        z.extract(member=n, path=base_extract_path)

            extra_paths = [os.path.join(base_extract_path, p) for p in ["botocore/data", "boto3/data"]]

            self.__application_logger.info("Boto uncompressed extra paths: %s", extra_paths)

            sess._loader._search_paths.extend(extra_paths)

        return (
            lambda name: sess.resource(name, **kwargs, region_name=region_name),
            lambda name: sess.client(name, **kwargs, region_name=region_name)
        )

    def copy_object(
        self,
        bucket_name_source: str,
        key_source: str,
        bucket_name_destination: str,
        key_destination: str
    ) -> bool:

        self.__application_logger.info(
            "Copying '%s' to '%s'",
            f"{bucket_name_source}/{key_source}", f"{bucket_name_destination}/{key_destination}"
        )

        try:
            self.__s3_client.copy_object(
                Bucket=bucket_name_destination,
                CopySource=f"{bucket_name_source}/{key_source}",
                Key=key_destination
            )
            self.__application_logger.info(
                "Object '%s' copied to '%s'",
                f"{bucket_name_source}/{key_source}", f"{bucket_name_destination}/{key_destination}"
            )
            return True
        except Exception as ex:
            self.__application_logger.error(
                "Failed to copy object '%s' to '%s': %s",
                f"{bucket_name_source}/{key_source}", f"{bucket_name_destination}/{key_destination}", str(ex)
            )
            raise

    def move_object(
        self,
        bucket_name_source: str,
        key_source: str,
        bucket_name_destination: str,
        key_destination: str
    ) -> bool:

        if self.copy_object(bucket_name_source, key_source, bucket_name_destination, key_destination):
            if self.delete_object(bucket_name_source, key_source):
                return True

    def delete_object(self, bucket_name: str, key: str) -> bool:

        self.__application_logger.info("Removing object '%s'", f"{bucket_name}/{key}")

        try:
            self.__s3_client.delete_object(
                Bucket=bucket_name,
                Key=key
            )
            self.__application_logger.info("Object '%s' removed successfully", f"{bucket_name}/{key}")
            return True
        except Exception as ex:
            self.__application_logger.error("Failed to remove object '%s': %s", f"{bucket_name}/{key}", str(ex))
            raise

    def delete_directory(self, bucket_name: str, directory: str) -> bool:

        files: List[Dict[str, str]] = []

        paginated_objects = self.list_objects_paginated(bucket_name, directory)

        for page in paginated_objects:
            for item in page["Contents"]:
                files.append({"Key": item["Key"]})

        try:
            self.__s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": files}
            )
            self.__application_logger.info("Directory '%s' removed successfully", f"{bucket_name}/{directory}")
            return True
        except Exception as ex:
            self.__application_logger.error(
                "Failed to remove directory '%s': %s",
                f"{bucket_name}/{directory}", str(ex)
            )
            raise

    def merge_objects(
        self,
        bucket_name: str,
        directory: str,
        files_extension: str,
        key_destination: str,
        remove_directory: bool = False
    ) -> bool:

        paginated_objects = self.list_objects_paginated(bucket_name, directory)

        try:
            files = []

            for page in paginated_objects:
                for item in page['Contents']:
                    if item['Key'].endswith(f".{files_extension}"):
                        files.append(item['Key'])

            output = []

            self.__application_logger.info("Reading listed objects")

            for file in files:
                file_content = self.__s3_client.get_object(Bucket=bucket_name, Key=file)["Body"].read().decode("UTF-8")
                output.append(file_content)

            content = "".join(output)

            self.__application_logger.info("Merging read objects")

            self.__s3_client.put_object(Bucket=bucket_name, Key=key_destination, Body=content)
            self.__application_logger.info("Objects merged")
            self.__application_logger.info("Complete file was uploaded in '%s'", f"{bucket_name}/{key_destination}")

            if remove_directory:
                self.delete_directory(bucket_name, directory)

            return True
        except Exception as ex:
            self.__application_logger.error(
                "Failed to merge objects from directory '%s': %s",
                f"{bucket_name}/{key_destination}", str(ex)
            )
            raise

    def list_objects(self, bucket_name: str, directory: str, max_keys: int = None) -> Any:

        self.__application_logger.info("Listing objects localized in '%s'", f"{bucket_name}/{directory}")

        try:
            if max_keys is not None:
                return self.__s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory, MaxKeys=max_keys)
            return self.__s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory)
        except Exception as ex:
            self.__application_logger.error(
                "Failed to list objects in '%s': %s",
                f"{bucket_name}/{directory}", str(ex)
            )
            raise

    def list_objects_paginated(self, bucket_name: str, directory: str, max_keys: int = None) -> PageIterator:

        self.__application_logger.info("Listing objects localized in '%s'", f"{bucket_name}/{directory}")

        try:
            paginator = self.__s3_client.get_paginator("list_objects_v2")

            params = {"Bucket": bucket_name, "Prefix": directory}

            if max_keys is not None:
                params["MaxKeys"] = max_keys

            return paginator.paginate(**params)
        except Exception as ex:
            self.__application_logger.error(
                "Failed to list objects in '%s': %s",
                f"{bucket_name}/{directory}", str(ex)
            )
            raise
