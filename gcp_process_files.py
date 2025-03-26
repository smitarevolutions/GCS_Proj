from google.cloud import storage
import os
import pandas as pd
import certifi
import filepath


os.environ["SSL_CERT_FILE"] = certifi.where()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = filepath.service_account_path

print("GOOGLE_APPLICATION_CREDENTIALS:", os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))


def read_csv_from_gcs(bucket_name, file_path):
    client = storage.Client()
    gcs_file_path = f"gs://{bucket_name}/{file_path}"
    print(gcs_file_path)
   
    buckets = list(client.list_buckets())
    print("Buckets:", [bucket.name for bucket in buckets])
    
    try:
        df = pd.read_csv(gcs_file_path, storage_options={"token": "google_default" , "verify":False})
        print (df.head(10))
        return df
    except Exception as e:
        print(f"Error connecting: {e}")
        return 
    
   


df = read_csv_from_gcs(filepath.bucket_name, filepath.blob_name_csv)

def write_parquet_to_gcs(df, bucket_name, output_path):
    client = storage.Client()
    gcs_file_path = f"gs://{bucket_name}/{output_path}"

    # Save DataFrame as a Parquet file
    df.to_parquet(gcs_file_path, storage_options={"token": filepath.service_account_path}, engine="pyarrow")
    
    print(f"File saved to {gcs_file_path}")

write_parquet_to_gcs(df, filepath.bucket_name, "processed_data1.parquet")


def upload_multiple_parquet_files(local_folder, bucket_name, gcs_folder):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for filename in os.listdir(local_folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(local_folder, filename)
            df = pd.read_csv(file_path)

            # Converting CSV to Parquet
            parquet_filename = filename.replace(".csv", ".parquet")
            parquet_path = os.path.join(local_folder, parquet_filename)
            
            #checking if the file already exists
            if os.path.exists(parquet_path):
                print(f"The file '{parquet_path}' exists.")

            else:
                 print(f"The file '{parquet_path}' does not exist.")
                 df.to_parquet(parquet_path, engine="pyarrow")
            
            # Uploading Parquet to GCS
            blob = bucket.blob(f"{gcs_folder}/{parquet_filename}")
            blob.upload_from_filename(parquet_path)
            print(f"Uploaded {parquet_filename} to gs://{bucket_name}/{gcs_folder}")



 
upload_multiple_parquet_files(filepath.local_csv_path, filepath.bucket_name, filepath.gcs_parquet_folder)

