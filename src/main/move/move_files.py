from src.main.utility.logging_config import logger
import traceback


def move_file_gcs_to_gcs(
    gcs_client, bucket_name, source_prefix, destination_prefix, file_name=None
):
    """
    Move files from one GCS bucket to another GCS bucket.

    Args:
        gcs_client: The GCS client object.
        bucket_name: The name of the GCS bucket.
        source_prefix: The source prefix (folder path) in the GCS bucket.
        destination_prefix: The destination prefix (folder path) in the GCS bucket.
        file_name: Optional; the name of the file to move. If not provided, all files in the source prefix will be moved.

    Returns:
        None
    """
    try:
        # Get the source and destination buckets
        source_bucket = gcs_client.get_bucket(bucket_name)
        destination_bucket = gcs_client.get_bucket(bucket_name)

        # List blobs in the source prefix, but the order in which it returns is not guaranteed
        blobs = source_bucket.list_blobs(prefix=source_prefix)

        if file_name:
            expected_blob_path = source_prefix + file_name
            for blob in blobs:
                if blob.name == expected_blob_path:
                    new_blob = destination_bucket.copy_blob(
                        blob, destination_bucket, destination_prefix + file_name
                    )
                    blob.delete()
                    logger.info(f"Moved {blob.name} to {new_blob.name}")
                    break
            else:
                logger.info(f"No file named {file_name} found in {source_prefix}.")

        else:
            # Move all files in the source prefix
            for blob in blobs:
                new_blob = destination_bucket.copy_blob(
                    blob,
                    destination_bucket,
                    destination_prefix + blob.name.split("/")[-1],
                )
                blob.delete()
                logger.info(f"Moved {blob.name} to {new_blob.name}")

    except Exception as e:
        error_message = f"Error moving files: {e}"
        traceback_message = traceback.format_exc()
        logger.error("Got this error : %s", error_message)
        print(traceback_message)
        raise e
