import os

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3Manager:
    def __init__(self):

        self.aws_conn_id = os.environ.get("AWS_CONN_ID")
        self.bucket_name = os.environ.get("BUCKET_NAME")
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id)

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
                print(path)
                print(file)
                full_path = path + "/" + file
                self.upload_file(path, full_path)

        except Exception as e:
            print(f"Error uploading directory: {e}")
            return False
        return True

    def download_file(self, file_path):
        try:
            self.s3.download_file(file_path, self.bucket_name, file_path)
        except Exception as e:
            print(f"Error downloading file: {e}")
            return False
        return True

    def list_objects(self, bucket, prefix=''):
        """
        List objects in an S3 bucket.

        :param bucket: Bucket name
        :param prefix: Prefix for filtering objects
        :return: List of object keys
        """
        s3_client = self.get_client()
        try:
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            return [obj['Key'] for obj in response.get('Contents', [])]
        except Exception as e:
            print(f"Error listing objects: {e}")
            return []


if __name__ == '__main__':
    s3 = S3Manager()
    s3.upload_file('/', './monfile')
