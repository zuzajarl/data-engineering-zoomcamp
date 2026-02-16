import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time

# Change this to your bucket name
BUCKET_NAME = "my-unique-bucket-name-12345"

# If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "gcs.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

BASE_URL_GREEN = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/"
BASE_URL_YELLOW = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"

MONTHS = [ i for i in range(1, 13) ]
YEARS = ["2019", "2020"]
DOWNLOAD_DIR = "."

CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
bucket = client.bucket(BUCKET_NAME)

def download_file(url, filename):
    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, filename)
        print(f"Downloaded: {filename}")
        return filename
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None

def create_bucket(bucket_name):
    try:
        bucket = client.get_bucket(bucket_name)
        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            print(f"Bucket '{bucket_name}' exists.")
        else:
            print(f"Bucket '{bucket_name}' exists but not in your project.")
            sys.exit(1)

    except NotFound:
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        print(f"Bucket name '{bucket_name}' is taken and inaccessible.")
        sys.exit(1)

def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    create_bucket(BUCKET_NAME)

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if storage.Blob(bucket=bucket, name=blob_name).exists(client):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")

        except Exception as e:
            print(f"Retry failed: {e}")
        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")

if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    # Download green taxi data
    green_paths = []
    for year in YEARS:
        for month in MONTHS:
            filename = f"green_tripdata_{year}-{month}.csv.gz"
            url = f"{BASE_URL_GREEN}{filename}"
            path = download_file(url, filename)
            if path:
                green_paths.append(path)
    # # Download yellow taxi data
    yellow_paths = []
    for year in YEARS:
        for month in MONTHS:
            filename = f"yellow_tripdata_{year}-{month}.csv.gz"
            url = f"{BASE_URL_YELLOW}{filename}"
            path = download_file(url, filename)
            if path:
                yellow_paths.append(path)

    # Upload all downloaded files
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, green_paths+yellow_paths)

    print("Done!")
