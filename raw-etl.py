import logging
import os
from datetime import datetime
import json

import pandas as pd
from sqlalchemy import create_engine
from google.cloud import storage

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
file_handler = logging.FileHandler('logs.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Database connection details
DB_HOST = os.environ.get("FEN_DB_HOST")
DB_NAME = os.environ.get("FEN_DB_NAME")
DB_USER = os.environ.get("FEN_DB_USER")
DB_PASSWORD = os.environ.get("FEN_DB_PASSWORD")
TABLE_NAME = "Ad_Data_Adam"

# GCS bucket details
BUCKET_NAME = "samatel-dealer.appspot.com"
GOOGLE_PROJECT_ID = os.environ.get("GOOGLE_PROJECT_ID")

# Create SQLAlchemy engine
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")


def get_threshold_time(filename='threshold_time.txt'):
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            stored_time = file.read()
            if stored_time:
                return float(stored_time)
    return datetime.now().timestamp()


def set_threshold_time(timestamp, filename='threshold_time.txt'):
    with open(filename, 'w') as file:
        file.write(str(timestamp))


def list_new_files(project_name, bucket_name, threshold_time):
    client = storage.Client(project=project_name)
    bucket = client.bucket(bucket_name)

    blobs = bucket.list_blobs()

    new_files = []

    for blob in blobs:
        blob.reload()
        updated_time = blob.updated.timestamp()
        if updated_time > threshold_time:
            new_files.append(blob.name)

    return new_files


def download_file_from_gcs(project_id, bucket_name, file_path) -> str:
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    local_file_path = f"temp/downloaded_{file_path}"
    blob = bucket.blob(file_path)
    blob.download_to_filename(local_file_path)

    return local_file_path


def process_file(local_file_path) -> bool:
    df = pd.read_csv(local_file_path, compression="gzip")

    df['Time'] = pd.to_datetime(df['Time'])

    # TODO: maybe we remove duplicate records here
    # df = df.drop_duplicates()

    with open("schema.json", "r") as f:
        schema_data = json.load(f)

    column_names = schema_data["columns"]
    df = df[column_names]

    try:
        conn = engine.connect()
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        return False

    # Insert the data into the table
    try:
        df.to_sql(TABLE_NAME, conn, if_exists="append", index=False, method="multi")
        logger.info("Data successfully loaded into PostgreSQL..")
    except Exception as e:
        logger.error(f"Data insertion failed: {str(e)}")
        return False

    finally:
        conn.close()
        os.remove(f"{local_file_path}")

    return True


def main():
    # Checking for new files
    # Get last run time
    # TODO add command line argument to accept point in time
    threshold_time = get_threshold_time()

    new_files = list_new_files(GOOGLE_PROJECT_ID, BUCKET_NAME, threshold_time)

    for file in new_files:
        logger.info(f"Processing {file}")
        local_file_path = download_file_from_gcs(GOOGLE_PROJECT_ID, BUCKET_NAME, file)
        success = process_file(local_file_path)
        if success:
            logger.info(f"Successfully processed {file}")
        else:
            # TODO use a cloud watch logger or equivalent in GCP
            logger.info(f"Failed to process {file}")

    # Last run time is now
    set_threshold_time(datetime.now().timestamp())


if __name__ == "__main__":
    logger.info("Doing a run at: " + str(datetime.now()))
    main()
