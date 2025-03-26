from google.cloud import storage
import filepath
import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = filepath.service_account_path

def create_bucket(bucket_name):
    client = storage.Client()
    bucket = client.create_bucket(bucket_name)
    print(f"Bucket {bucket.name} created.")

create_bucket(filepath.bucket_name)


def upload_blob(bucket_name, source_file_path, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_path)
    print(f"File {source_file_path} uploaded to {destination_blob_name} in {bucket_name}.")

upload_blob(filepath.bucket_name, filepath.source_file_path, filepath.blob_name_csv)
