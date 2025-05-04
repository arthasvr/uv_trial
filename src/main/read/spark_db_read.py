from src.main.utility.logging_config import logger
import traceback
import dotenv

config = dotenv.dotenv_values(".env")


def read_db_spark(
    spark,
    table_name,
    db_url=config["MYSQL_URL"],
    db_properties={
        "user": config["MYSQL_USER"],
        "password": config["MYSQL_PASSWORD"],
        "driver": config["MYSQL_DRIVER"],
    },
):
    try:
        df = spark.read.jdbc(url=db_url, table=table_name, properties=db_properties)
        return df
    except Exception as e:
        error_message = f"Error reading table {table_name} from database: {e}"
        traceback_message = traceback.format_exc()
        logger.error("Got this error : %s", error_message)
        print(traceback_message)
        raise
