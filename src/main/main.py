from src.main.utility.my_sql_session import get_connection
from src.main.utility.logging_config import logger
from src.main.utility.gcs_client_object import create_gcs_client
from src.main.read.gcs_read import list_files
from src.main.download.gcs_file_download import download_files
from src.main.utility.spark_session import spark_session
from src.main.move.move_files import move_file_gcs_to_gcs
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    IntegerType,
    FloatType,
    DateType,
)
from dotenv import dotenv_values
import os
import sys
import json
import shutil
import datetime

config = dotenv_values(".env")
# print(config)

# for key, value in config.items():
#     print(f"{key} : {value}")

# generate_sample_data.write_to_csv(5)
# time.sleep(30)
# move_file_to_GCS.move_file_to_GCS(
#     "F:\\python_projects\\uv_trial\\resources\\data\\employee_data.csv",
#     config["GCS_BUCKET_NAME"],
#     "employee_data.csv",
# )
# time.sleep(30)
# create_bq_dataset_table.create_bq_dataset_table()
# time.sleep(30)
# load_data_bq_to_mysql.load_data()

gcs_client = create_gcs_client(config["GCP_KEY_FILE_PATH"])
# print(type(gcs_client))


# print(f"{config.keys()=}")
csv_files = [
    file for file in os.listdir(config["local_directory"]) if file.endswith(".csv")
]
connection = get_connection()
cursor = connection.cursor()

# print(f"{csv_files=}")
# print(f"{config["mandatory_columns"]=}")

total_csv_files = []

if csv_files:
    for file in csv_files:
        total_csv_files.append(file)
        # print(f"{total_csv_files=}")
        # print(f"{total_csv_files[1:-1]=}")
    statement = f"""
select distinct file_name from 
{config["MYSQL_DATABASE"]}.{config["product_staging_table"]} 
where file_name in ({str(total_csv_files)[1:-1]}) and status='A'
"""
    logger.info(f"Dynamically statement created {statement}")
    cursor.execute(statement)
    data = cursor.fetchall()
    # print(data)
    if data:
        logger.info("Your last run failed. Please check")
    else:
        logger.info("No record found")
else:
    logger.info("Last run was successful")

try:
    gcs_absolute_file_path = list_files(
        gcs_client, config["GCS_BUCKET_NAME"], config["gcs_source_directory"]
    )
    # print(f"{gcs_absolute_file_path=}")
    logger.info(f"blob path on gcs bucket for csv file: {gcs_absolute_file_path}")
    if not gcs_absolute_file_path:
        logger.info(f"No files available at {config['gcs_source_directory']}")
        raise Exception("No data available to process")
except Exception as e:
    logger.error("Exited with error {e}")
    raise e


# prefix = f"gs://{config['GCS_BUCKET_NAME']}/"
logger.info(
    f"File path available on gcs under {config['GCS_BUCKET_NAME']} bucket and folder name is {gcs_absolute_file_path}"
)

try:
    download_files(
        gcs_client,
        config["GCS_BUCKET_NAME"],
        config["local_directory"],
        gcs_absolute_file_path,
    )
except Exception as e:
    logger.error(f"File download error: {e}")
    sys.exit()


# Get a list of all files in the local directory
all_files: list[str] = os.listdir(config["local_directory"])
logger.info(
    f"list of files present in my local directory after download are {all_files}"
)

# Filter files with ".csv" in their name and create absolute paths
if all_files:
    csv_files = []
    error_files = []
    for file in all_files:
        if (
            isinstance(file, str)
            and file.endswith(".csv")
            and isinstance(config["local_directory"], str)
        ):
            csv_files.append(
                os.path.abspath(os.path.join(config["local_directory"], file))
            )
            # print(
            #     f"output of os.path.join({config['local_directory']},{file}) is {os.path.abspath(os.path.join(config['local_directory'], file))}"
            # )
            # print(
            #     f"absolute path is {os.path.abspath(os.path.join(config['local_directory'], file))}"
            # )
        else:
            error_files.append(
                os.path.abspath(os.path.join(str(config["local_directory"]), file))
            )
            # print(
            #     f"From noncsv branch {os.path.abspath(os.path.join(str(config['local_directory']), file))}"
            # )

    if not csv_files:
        logger.error("No csv data available to process the request")
        raise Exception("No csv data available to process the request")

else:
    logger.error("There is no data to process")
    raise Exception("There is no data to process")

logger.info("*******************Listing the csv files*******************")
logger.info(f"List of files that needs to be processed {csv_files}")

# print(
#     f"{set(
#     json.loads(
#         str(config["mandatory_columns"])
#         )
#         )=}"
# )

logger.info("*******************Creating Spark session*******************")
spark = spark_session()

logger.info("*******************Spark Session created*******************")

# check the required columns are present in the schema of the csv file
# if not required columns keep it in a list or error files
# else union all the data into one dataframe

logger.info(
    "*******************checking schema for data loaded in gcs*******************"
)

correct_files = []
for file in csv_files:
    data_schema = spark.read.format("csv").option("header", "true").load(file).columns
    logger.info(f"Schema for {file} is {data_schema}")
    logger.info(f"mandatory columns schema is {config['mandatory_columns']}")
    missing_columns = set(json.loads(str(config["mandatory_columns"]))) - set(
        data_schema
    )
    logger.info(f"missing columns are {missing_columns}")

    if not missing_columns:
        correct_files.append(file)
        logger.info(f"File {file} has all mandatory columns")
    else:
        error_files.append(file)
        logger.error(f"File {file} does not have all mandatory columns")

logger.info(
    f"*******************List of correct files*******************\n{correct_files}"
)
logger.info(f"*******************List of error files*******************\n{error_files}")
logger.info(
    "*******************Moving error files to error directory if any*******************"
)


# Move the error files to error directory on local
if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            error_file_path = os.path.join(
                str(config["error_folder_path_local"]), file_name
            )

            shutil.move(file_path, error_file_path)
            logger.info(
                f"Moved {file_path} from local directory to error local directory {error_file_path}"
            )

            move_file_gcs_to_gcs(
                gcs_client,
                config["GCS_BUCKET_NAME"],
                config["gcs_source_directory"],
                config["gcs_error_directory"],
                file_name,
            )

        else:
            logger.error(
                f"File {file_path} does not exist. Cannot move to error directory."
            )

else:
    logger.info(
        "*******************There are no error files available at our dataset*******************"
    )


# Additional columns need to be taken care of
# Determine extra columns

# Before running the process
# staging table needs to be updated with status as Active (A) or Inactive (I)

logger.info(
    "*******************Updating the product staging table that we have started the process*******************"
)

insert_statements = []
formatted_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if correct_files:
    for file_path in correct_files:
        file_name = os.path.basename(file_path)
        statement = f"""
Insert into {config["MYSQL_DATABASE"]}.{config["product_staging_table"]} \
(file_name, file_location, created_date, status) \
values ('{file_name}','{file_path}','{formatted_date}', 'A')
"""
        insert_statements.append(statement)

    logger.info(f"Insert statements created for staging table --- {insert_statements}")
    logger.info("*******************Connecting with Mysql server*******************")
    for statement in insert_statements:
        logger.info(f"Executing statement {statement}")
        cursor.execute(statement)
        connection.commit()
    connection.close()
    cursor.close()
else:
    logger.info("*******************There are no files to process*******************")
    raise Exception(
        "*******************No data available with correct files*******************"
    )

logger.info("*******************Staging table updated successfully*******************")
logger.info(
    "*******************Fixing extra columns coming from source*******************"
)


schema = StructType(
    [
        StructField("customer_id", IntegerType(), True),
        StructField("store_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("sales_date", DateType(), True),
        StructField("sales_person_id", IntegerType(), True),
        StructField("price", FloatType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_cost", FloatType(), True),
        StructField("additional_columns", StringType(), True),
    ]
)


logger.info("*******************Creating empty Dataframe*******************")

# we were trying to create empty dataframe with schema using below command but it was not working
# but chatgpt proposed this solution and it worked
# final_df_to_process = spark.createDataFrame([], schema=schema)

# the issue was with the empty list [] and it was not working
# so we created empty rdd and then created dataframe with schema

# # Why This Works
# [] is interpreted as a list of Row objects, but it’s empty, so PySpark can’t figure out how to map it.

# spark.sparkContext.emptyRDD() creates an explicitly empty distributed dataset, which works properly with a defined schema.


final_df_to_process = spark.createDataFrame(
    spark.sparkContext.emptyRDD(), schema=schema
)
final_df_to_process.printSchema()
final_df_to_process.show()
