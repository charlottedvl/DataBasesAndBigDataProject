import os

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3Manager:
    def __init__(self, aws_con_id, bucket_name):
        self.bucket_name = bucket_name
        self.s3 = S3Hook(aws_conn_id=aws_con_id)

    def upload_file(self, path, file_name):
        try:
            self.s3.load_file(file_name, path, self.bucket_name, replace=True)
        except Exception as e:
            print(f"Error uploading file: {e}")
            return False
        return True

    def upload_directory(self, path):
        try:
            for file in os.listdir(path):
                full_path = path + "/" + file
                self.upload_file(path, full_path)

        except Exception as e:
            print(f"Error uploading directory: {e}")
            return False
        return True

    def download_file(self, file_path, file):
        try:
            self.s3.get_key(file_path + "/" + file, self.bucket_name).download_file(file_path)
        except Exception as e:
            print(f"Error downloading file: {e}")
            return False
        return True

