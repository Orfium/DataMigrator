import boto3
from config import AWS_REGION, AWS_BUCKET_RESULTS, AWS_ACCESS_KEY, AWS_SECRET_KEY


class S3Executor:
    def __init__(self):
        self.s3 = boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
        )
        self.bucket_results = AWS_BUCKET_RESULTS

    def download_file(self, object_name, save_location):
        self.s3.download_file(self.bucket_results, object_name, save_location)
