from src.main.utility.gcs_client_object import create_gcs_client
import os
import datetime
from dotenv import dotenv_values

config = dotenv_values(".env")


# print("inside move_file_to_GCS file")


def move_file_to_GCS(gcs_client, gcs_directory, bucket_name, local_file_path):
    print("inside move_file_to_GCS function")
    current_epoch = int(datetime.datetime.now().timestamp()) * 1000
    bucket = gcs_client.bucket(bucket_name)
    # blob = bucket.blob("test1/" + destination_blob_name)
    # blob.upload_from_filename(file_path)
    print(f"{local_file_path=}")
    gcs_prefix = f"{gcs_directory}{current_epoch}"
    try:
        for root, dirs, files in os.walk(local_file_path):
            for file in files:
                print(file)
                local_file_path = os.path.join(root, file)
                gcs_key = f"{gcs_prefix}{file}"
                print(f"{local_file_path=}\n{bucket.name=}\n{gcs_key=}\n\n\n")
                # storage_client.upload_file(local_file_path, bucket, gcs_key)
                blob = bucket.blob(gcs_key)
                blob.upload_from_filename(local_file_path)
    except Exception as e:
        raise e


if __name__ == "__main__":
    gcs_client = create_gcs_client(config["GCP_KEY_FILE_PATH"])
    move_file_to_GCS(
        gcs_client,
        config["gcs_source_directory"],
        config["GCS_BUCKET_NAME"],
        config["local_directory"],
    )
