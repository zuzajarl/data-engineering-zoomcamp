import os
import sys
import urllib.request
import gzip
import shutil
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time

# -------------------------
# CONFIGURATION
# -------------------------

BUCKET_NAME = "hmwrk4bucket"
CREDENTIALS_FILE = "gcs.json"

BASE_URL_FHV = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/"

MONTHS = [i for i in range(2, 13)]
YEAR = "2019"

DOWNLOAD_DIR = "./fhv_data"
CHUNK_SIZE = 8 * 1024 * 1024

# -------------------------

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bucket = client.bucket(BUCKET_NAME)


def create_bucket(bucket_name):
    try:
        client.get_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' exists.")
    except NotFound:
        client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        print(f"Bucket name '{bucket_name}' is taken and inaccessible.")
        sys.exit(1)


def download_and_extract(url, filename_gz):
    try:
        print(f"Downloading {url}...")
        gz_path = os.path.join(DOWNLOAD_DIR, filename_gz)
        urllib.request.urlretrieve(url, gz_path)

        print(f"Extracting {filename_gz}...")
        csv_filename = filename_gz.replace(".csv.gz", ".csv")
        csv_path = os.path.join(DOWNLOAD_DIR, csv_filename)

        with gzip.open(gz_path, 'rb') as f_in:
            with open(csv_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        os.remove(gz_path)  # remove compressed file
        print(f"Extracted to {csv_filename}")

        return csv_path

    except Exception as e:
        print(f"Failed processing {url}: {e}")
        return None


def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {blob_name} (Attempt {attempt+1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")
            return
        except Exception as e:
            print(f"Retry failed: {e}")
            time.sleep(5)

    print(f"Failed to upload {blob_name} after {max_retries} attempts.")


if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    fhv_paths = []

    for month in MONTHS:
        filename_gz = f"fhv_tripdata_{YEAR}-{month:02d}.csv.gz"
        url = f"{BASE_URL_FHV}{filename_gz}"

        path = download_and_extract(url, filename_gz)
        if path:
            fhv_paths.append(path)

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, fhv_paths)

    print("All FHV files uploaded as uncompressed CSV!")
