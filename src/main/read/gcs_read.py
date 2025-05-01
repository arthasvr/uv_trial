from src.main.utility.logging_config import logger
import traceback


def list_files(client, bucket_name, folder_path):
    try:
        bucket = client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=folder_path)
        if blobs:
            files = [blob.name for blob in blobs if not blob.name.endswith("/")]
            logger.info(
                f"Total files available in {folder_path} of {bucket_name} : {len(files)}"
            )
            return files
        else:
            return []
    except Exception as e:
        error_message = f"Error listing files: {e}"
        traceback_message = traceback.format_exc()
        logger.error("Got this error : %s", error_message)
        print(traceback_message)
        raise
