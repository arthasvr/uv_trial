from google.cloud import storage
from src.main.utility.logging_config import logger
import traceback
import os


def download_files(client, bucket_name, local_directory, blobs_list):
    logger.info(f"Running download files for these files : {blobs_list}")
    bucket = client.get_bucket(bucket_name)
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    for blob_name in blobs_list:
        blob = bucket.blob(blob_name)
        logger.info(f"File name {os.path.basename(blob_name)}")
        local_file_path = os.path.join(local_directory, os.path.basename(blob_name))
        try:
            blob.download_to_filename(local_file_path)
        except Exception as e:
            error_message = f"Error downloading file '{blob_name}': {str(e)}"
            traceback_message = traceback.format_exc()
            print(error_message)
            print(traceback_message)
            raise e
