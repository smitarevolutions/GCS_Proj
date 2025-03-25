from google.cloud import storage
import os
import random

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"


bucket_name = 'bkt-demo-2'
source_file_path = '/Users/himanshugaurav/Desktop/python_proj/gcp_python/resources/uploaded_file.txt'
destination_blob_name = 'uploaded_file1.txt'


def create_bucket(bucket_name):
    client = storage.Client()
    bucket = client.create_bucket(bucket_name)
    print(f"Bucket {bucket.name} created.")

create_bucket(bucket_name)


def upload_blob(bucket_name, source_file_path, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_path)
    print(f"File {source_file_path} uploaded to {destination_blob_name} in {bucket_name}.")

upload_blob(bucket_name, source_file_path, destination_blob_name)
