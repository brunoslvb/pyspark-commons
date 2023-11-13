import pytest
from botocore.client import BaseClient
from moto.s3 import mock_s3

from src.shared.utils.s3_utils import S3Utils
from tests.utils.s3_fixtures import s3_utils, s3_client
from botocore.exceptions import ClientError


class TestS3Utils:

    class TestListObjects:

        __bucket_name = "bucket-lab.dev"
        __directory = "dir1/subdir1/subdir2"

        @pytest.fixture(scope="class", autouse=True)
        def setup_class(self, s3_client: BaseClient):
            print("Starting moto server ...")
            with mock_s3():
                s3_client.create_bucket(Bucket=self.__bucket_name)
                s3_client.put_object(Bucket=self.__bucket_name, Key=f"{self.__directory}/part-A0000.txt", Body="A 0")
                s3_client.put_object(Bucket=self.__bucket_name, Key=f"{self.__directory}/part-A0001.txt", Body="A 1")
                s3_client.put_object(Bucket=self.__bucket_name, Key=f"{self.__directory}/part-B0000.txt", Body="B 0")
                yield
            print("Stopping moto server ...")

        def test_list_objects(self, s3_utils: S3Utils):

            objects = s3_utils.list_objects(
                bucket_name=self.__bucket_name,
                directory=self.__directory
            )

            assert objects["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert objects["KeyCount"] == 3

        def test_list_objects_with_prefix(self, s3_utils: S3Utils):

            objects = s3_utils.list_objects(
                bucket_name=self.__bucket_name,
                directory=f"{self.__directory}/part-A"
            )

            assert objects["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert objects["KeyCount"] == 2

        def test_list_objects_with_max_keys(self, s3_utils: S3Utils):

            objects = s3_utils.list_objects(
                bucket_name=self.__bucket_name,
                directory=self.__directory,
                max_keys=1
            )

            assert objects["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert objects["KeyCount"] == 1

        def test_list_objects_bucket_not_found(self, s3_utils: S3Utils):

            with pytest.raises(ClientError):
                s3_utils.list_objects(
                    bucket_name=f"{self.__bucket_name}.test",
                    directory=self.__directory,
                    max_keys=1
                )

        def test_list_objects_paginated(self, s3_utils: S3Utils):

            paginated_objects = s3_utils.list_objects_paginated(
                bucket_name=self.__bucket_name,
                directory=self.__directory
            )

            pages = list(paginated_objects)

            assert len(pages) == 1
            assert pages[0]["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert pages[0]["KeyCount"] == 3

        def test_list_objects_paginated_with_prefix(self, s3_utils: S3Utils):

            paginated_objects = s3_utils.list_objects_paginated(
                bucket_name=self.__bucket_name,
                directory=f"{self.__directory}/part-A"
            )

            pages = list(paginated_objects)

            assert len(pages) == 1
            assert pages[0]["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert pages[0]["KeyCount"] == 2

        def test_list_objects_paginated_with_max_keys(self, s3_utils: S3Utils):

            paginated_objects = s3_utils.list_objects_paginated(
                bucket_name=self.__bucket_name,
                directory=self.__directory,
                max_keys=2
            )

            pages = list(paginated_objects)

            assert len(pages) == 2
            assert pages[0]["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert pages[0]["KeyCount"] == 2
            assert pages[1]["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert pages[1]["KeyCount"] == 1

    class TestCopyObject:

        __bucket_name = "bucket-lab.dev"
        __key_source = "dir1/subdir1/subdir2/test-file.txt"

        @pytest.fixture(scope="function", autouse=True)
        def setup_copy_object(self, s3_client: BaseClient):
            print("Starting moto server ...")
            with mock_s3():
                s3_client.create_bucket(Bucket=self.__bucket_name)
                s3_client.put_object(Bucket=self.__bucket_name, Key=f"{self.__key_source}", Body="TEST")
                yield
            print("Stopping moto server ...")

        def test_copy_object(self, s3_utils: S3Utils, s3_client: BaseClient):

            directory_destination = "dir2/subdir1"
            filename_destination = "test-file.txt"

            response = s3_utils.copy_object(
                bucket_name_source=self.__bucket_name,
                bucket_name_destination=self.__bucket_name,
                key_source=self.__key_source,
                key_destination=f"{directory_destination}/{filename_destination}"
            )

            assert response

            s3_client.head_object(
                Bucket=self.__bucket_name,
                Key=self.__key_source
            )

            s3_client.head_object(
                Bucket=self.__bucket_name,
                Key=f"{directory_destination}/{filename_destination}"
            )

        def test_copy_object_bucket_not_found(self, s3_utils: S3Utils):

            directory_destination = "dir2/subdir1"
            filename_destination = "test-file.txt"

            with pytest.raises(ClientError):
                s3_utils.copy_object(
                    bucket_name_source=f"{self.__bucket_name}.test",
                    bucket_name_destination=self.__bucket_name,
                    key_source=self.__key_source,
                    key_destination=f"{directory_destination}/{filename_destination}"
                )

    class TestMoveObject:

        __bucket_name = "bucket-lab.dev"
        __key_source = "dir1/subdir1/subdir2/test-file.txt"

        @pytest.fixture(scope="function", autouse=True)
        def setup_move_object(self, s3_client: BaseClient):
            print("Starting moto server ...")
            with mock_s3():
                s3_client.create_bucket(Bucket=self.__bucket_name)
                s3_client.put_object(Bucket=self.__bucket_name, Key=f"{self.__key_source}", Body="TEST")
                yield
            print("Stopping moto server ...")

        def test_move_object(self, s3_utils: S3Utils, s3_client: BaseClient):

            directory_destination = "dir2/subdir1"
            filename_destination = "test-file.txt"

            response = s3_utils.move_object(
                bucket_name_source=self.__bucket_name,
                bucket_name_destination=self.__bucket_name,
                key_source=self.__key_source,
                key_destination=f"{directory_destination}/{filename_destination}"
            )

            assert response

            with pytest.raises(ClientError) as ex:
                s3_client.head_object(
                    Bucket=self.__bucket_name,
                    Key=self.__key_source
                )

            s3_client.head_object(
                Bucket=self.__bucket_name,
                Key=f"{directory_destination}/{filename_destination}"
            )

    class TestDeleteObject:

        __bucket_name = "bucket-lab.dev"
        __key_source = "dir1/subdir1/subdir2/test-file.txt"

        @pytest.fixture(scope="function", autouse=True)
        def setup_delete_object(self, s3_client: BaseClient):
            print("Starting moto server ...")
            with mock_s3():
                s3_client.create_bucket(Bucket=self.__bucket_name)
                s3_client.put_object(Bucket=self.__bucket_name, Key=f"{self.__key_source}", Body="TEST")
                yield
            print("Stopping moto server ...")

        def test_delete_object(self, s3_utils: S3Utils, s3_client: BaseClient):

            response = s3_utils.delete_object(
                bucket_name=self.__bucket_name,
                key=self.__key_source
            )

            assert response

            with pytest.raises(ClientError) as ex:
                s3_client.head_object(
                    Bucket=self.__bucket_name,
                    Key=self.__key_source
                )

        def test_delete_object_bucket_not_found(self, s3_utils: S3Utils):

            with pytest.raises(ClientError):
                s3_utils.delete_object(
                    bucket_name=f"{self.__bucket_name}.test",
                    key=self.__key_source
                )

    class TestDeleteDirectory:

        __bucket_name = "bucket-lab.dev"
        __directory = "dir1/subdir1"
        __subdirectory = "subdir2"

        @pytest.fixture(scope="function", autouse=True)
        def setup_delete_directory(self, s3_client: BaseClient):
            print("Starting moto server ...")
            with mock_s3():
                s3_client.create_bucket(Bucket=self.__bucket_name)
                s3_client.put_object(Bucket=self.__bucket_name, Key=f"{self.__directory}/test-file.txt", Body="TEST")
                s3_client.put_object(Bucket=self.__bucket_name, Key=f"{self.__directory}/{self.__subdirectory}/part-0000.txt", Body="TEST")
                s3_client.put_object(Bucket=self.__bucket_name, Key=f"{self.__directory}/{self.__subdirectory}/part-0001.txt", Body="TEST")
                s3_client.put_object(Bucket=self.__bucket_name, Key=f"{self.__directory}/{self.__subdirectory}/part-0002.txt", Body="TEST")
                s3_client.put_object(Bucket=self.__bucket_name, Key=f"{self.__directory}/{self.__subdirectory}/part-0003.txt", Body="TEST")
                yield
            print("Stopping moto server ...")

        def test_delete_directory(self, s3_utils: S3Utils, s3_client: BaseClient):

            response = s3_utils.delete_directory(
                bucket_name=self.__bucket_name,
                directory=f"{self.__directory}/{self.__subdirectory}"
            )

            assert response

            objects = s3_client.list_objects_v2(
                Bucket=self.__bucket_name,
                Prefix=f"{self.__directory}"
            )

            assert objects["KeyCount"] == 1

        def test_delete_directory_bucket_not_found(self, s3_utils: S3Utils):

            with pytest.raises(ClientError):
                s3_utils.delete_directory(
                    bucket_name=f"{self.__bucket_name}.test",
                    directory=f"{self.__directory}/{self.__subdirectory}"
                )

    class TestMergeObjects:

        __bucket_name = "bucket-lab.dev"
        __directory = "dir1/subdir1"
        __subdirectory = "parts"

        __header_content = f"{'HEADER'.ljust(10)}\n"
        __detail_1_content = f"{'DETAIL 1'.ljust(10)}\n"
        __detail_2_content = f"{'DETAIL 2'.ljust(10)}\n"
        __trailer_content = f"{'TRAILER'.ljust(10)}\n"

        @pytest.fixture(scope="function", autouse=True)
        def setup_merge_objects(self, s3_client: BaseClient):

            print("Starting moto server ...")
            with mock_s3():
                s3_client.create_bucket(Bucket=self.__bucket_name)
                s3_client.put_object(Bucket=self.__bucket_name, Key=f"{self.__directory}/{self.__subdirectory}/part-0000.txt", Body=self.__header_content)
                s3_client.put_object(Bucket=self.__bucket_name, Key=f"{self.__directory}/{self.__subdirectory}/part-0001.txt", Body=self.__detail_1_content)
                s3_client.put_object(Bucket=self.__bucket_name, Key=f"{self.__directory}/{self.__subdirectory}/part-0002.txt", Body=self.__detail_2_content)
                s3_client.put_object(Bucket=self.__bucket_name, Key=f"{self.__directory}/{self.__subdirectory}/part-0003.txt", Body=self.__trailer_content)
                yield
            print("Stopping moto server ...")

        @pytest.fixture(scope="function")
        def expected_text_content(self):
            return f"{self.__header_content}{self.__detail_1_content}{self.__detail_2_content}{self.__trailer_content}"

        def test_merge_text_objects_keeping_parts_directory(self, s3_utils: S3Utils, s3_client: BaseClient, expected_text_content):

            key_destination = f"{self.__directory}/file-merged.txt"

            response = s3_utils.merge_objects(
                bucket_name=self.__bucket_name,
                directory=f"{self.__directory}/{self.__subdirectory}",
                files_extension="txt",
                key_destination=key_destination
            )

            assert response

            content = s3_client.get_object(
                Bucket=self.__bucket_name,
                Key=key_destination
            )["Body"].read().decode("UTF-8")

            assert expected_text_content == content

            objects = s3_client.list_objects_v2(
                Bucket=self.__bucket_name,
                Prefix=f"{self.__directory}/{self.__subdirectory}"
            )

            assert objects["KeyCount"] == 4

        def test_merge_text_objects_removing_parts_directory(self, s3_utils: S3Utils, s3_client: BaseClient, expected_text_content):

            key_destination = f"{self.__directory}/file-merged.txt"

            response = s3_utils.merge_objects(
                bucket_name=self.__bucket_name,
                directory=f"{self.__directory}/{self.__subdirectory}",
                files_extension="txt",
                key_destination=key_destination,
                remove_directory=True
            )

            assert response

            content = s3_client.get_object(
                Bucket=self.__bucket_name,
                Key=key_destination
            )["Body"].read().decode("UTF-8")

            assert expected_text_content == content

            objects = s3_client.list_objects_v2(
                Bucket=self.__bucket_name,
                Prefix=f"{self.__directory}/{self.__subdirectory}"
            )

            assert objects["KeyCount"] == 0

        def test_merge_text_objects_bucket_not_found(self, s3_utils: S3Utils):

            key_destination = f"{self.__directory}/file-merged.txt"

            with pytest.raises(ClientError):
                s3_utils.merge_objects(
                    bucket_name=f"{self.__bucket_name}.test",
                    directory=f"{self.__directory}/{self.__subdirectory}",
                    files_extension="txt",
                    key_destination=key_destination,
                    remove_directory=True
                )
