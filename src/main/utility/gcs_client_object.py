from google.cloud import storage
from src.main.utility.logging_config import logger
import traceback


def create_gcs_client(json_file):
    try:
        client = storage.Client.from_service_account_json(json_file)
        return client
    except Exception as e:
        logger.error(f"Error creating gcs client  : {str(e)}")
        traceback_message = traceback.format_exc()
        print(traceback_message)
        raise e
