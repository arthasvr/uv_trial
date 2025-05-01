from dotenv import dotenv_values
from google.cloud import bigquery
from src.main.utility import my_sql_session

config = dotenv_values(".env")


def load_data():
    bigquery_client = bigquery.Client.from_service_account_json(
        config["GCP_KEY_FILE_PATH"]
    )

    bq_cursor = bigquery_client.query(
        f"SELECT * FROM `{config["GCP_PROJECT_ID"]}.{config["GCP_BIGQUERY_DATASET_NAME"]}.{config["GCP_BIGQUERY_TABLE_NAME"]}`"
    )
    bq_data = bq_cursor.result()

    mysql_connection = my_sql_session.get_connection()
    mysql_cursor = mysql_connection.cursor()
    query = (
        f"INSERT INTO {config['MYSQL_TABLE_NAME']} VALUES (%s, %s, %s, %s, %s, %s, %s)"
    )

    for row in bq_data:
        data = (
            row.first_name,
            row.last_name,
            row.email,
            row.phone_number,
            row.job,
            row.department,
            row.salary,
        )
        mysql_cursor.execute(query, data)

    mysql_connection.commit()
    mysql_cursor.close()
    mysql_connection.close()
    print("loaded data from BigQuery to MySQL")
