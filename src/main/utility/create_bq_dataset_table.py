from google.cloud import bigquery
from dotenv import dotenv_values


config = dotenv_values(".env")
print(config)
for key in config:
    print(key, config[key])

print(bigquery.Client.from_service_account_json)


def create_bq_dataset_table():
    bigquery_client = bigquery.Client.from_service_account_json(
        config["GCP_KEY_FILE_PATH"]
    )

    # This line creates a reference to a BigQuery dataset — but doesn’t create or fetch the dataset yet. It’s just a pointer.
    # Return Type : <google.cloud.bigquery.dataset.DatasetReference>
    # its a light weight object holding project id and dataset id <DatasetReference('my-gcp-project', 'my_dataset')>
    dataset_ref = bigquery_client.dataset(config.get("GCP_BIGQUERY_DATASET_NAME", "NA"))

    try:
        bigquery_client.get_dataset(dataset_ref)
        print("Dataset {} already exists".format(config["GCP_BIGQUERY_DATASET_NAME"]))
    except Exception as e:
        print("Creating dataset {}".format(config["GCP_BIGQUERY_DATASET_NAME"]))

        # Below You're creating a new Dataset object using the previously defined dataset_ref.
        # At this point, it’s just a local Python object representing the dataset and its metadata — it does not yet exist in BigQuery.
        # Returns A Dataset object — this is a full dataset resource, not just a reference.

        dataset = bigquery.Dataset(dataset_ref)

        # This line sends a request to BigQuery to create the dataset in the actual cloud project.
        # If successful, BigQuery returns the created Dataset resource with updated info (like id, etag, timestamps, etc).
        # Returns A Dataset object — the same object, but now returned from BigQuery after creation.

        dataset = bigquery_client.create_dataset(dataset)
        print("Dataset {} created".format(config["GCP_BIGQUERY_DATASET_NAME"]))

    # This creates a LoadJobConfig object, which is a configuration object used when loading data into BigQuery.

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True
    )

    # This line starts a BigQuery load job — basically telling BigQuery:
    # “Hey, go fetch this file from Cloud Storage and load it into this BigQuery table using the settings I provided.”
    # This line kicks off the load job and returns a LoadJob object: <google.cloud.bigquery.job.LoadJob>
    # But it hasn’t finished yet — it just started.
    # To wait for it to complete, the next line in your code is important: load_job.result()

    load_job = bigquery_client.load_table_from_uri(
        source_uris=config.get("GCS_FILE_URI", "NA"),
        destination=dataset_ref.table("employees"),
        job_config=job_config,
    )

    # Below line blocks until the job is done and raises an error if it fails.

    load_job.result()

    print(
        "Loaded {} rows into {}:{}.".format(
            load_job.output_rows, config["GCP_BIGQUERY_DATASET_NAME"], "employees"
        )
    )
