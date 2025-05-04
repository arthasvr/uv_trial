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
from pyspark.sql import functions as F
from src.main.read.spark_db_read import read_db_spark
from src.main.transformations.jobs.dimension_tables_join import dimensions_tables_join
from src.main.write.dataframe_writer import write_df
from src.main.upload.move_file_to_GCS import move_file_to_GCS
from src.main.transformations.jobs.customer_mart_sql_transform_write import (
    customer_mart_calculation_table_write,
)
from src.main.transformations.jobs.sales_mart_sql_transform_write import (
    sales_mart_calculation_table_write,
)
from src.main.delete.local_file_delete import delete_local_file
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


for data in correct_files:
    data_df = spark.read.format("csv").option("header", "true").load(data)
    data_columns = data_df.columns
    extra_columns = list(
        set(data_columns) - set(json.loads(str(config["mandatory_columns"])))
    )
    print(f"{type(extra_columns)=}")
    logger.info(f" Extra columns present at file {data} is {extra_columns}")

    if extra_columns:
        # Here the values of the extra_columns are concatenated with comma and get added to the dataframe.
        # If there was only one extracolumn, then you will not see any commas.
        # But if there were 2 columns lets say "payment_mode" and "location" then their values will be concatenated and you will see value as for example "UPI,Chennai".
        data_df = data_df.withColumn(
            "additional_columns", F.concat_ws(",", *extra_columns)
        ).select(
            "customer_id",
            "store_id",
            "product_name",
            "sales_date",
            "sales_person_id",
            "price",
            "quantity",
            "total_cost",
            "additional_columns",
        )
        logger.info(f"Processed file {data} and added 'additional columns'")
    else:
        data_df = data_df.withColumn("additional_columns", F.lit(None)).select(
            "customer_id",
            "store_id",
            "product_name",
            "sales_date",
            "sales_person_id",
            "price",
            "quantity",
            "total_cost",
            "additional_columns",
        )

    final_df_to_process = final_df_to_process.union(data_df)

logger.info("Final dataframe from source which will be going to processing")
final_df_to_process.show()
final_df_to_process.filter(F.col("additional_columns").isNull()).show()


# Enrich the data from all dimension tables
# also create a datamart for sales_team and their incentive, address and all
# another datamart for customer who bought how much each days of month
# for every month there should be a file and inside that
# there should be a store_id segregation
# Read the data from parquet and generate a csv file
# in which there will be a sales_person_name, sales_person_store_id
# sales_person_total_billing_done_for_each_month, total_incentive


# Creating df for all tables
# customer table
logger.info(
    "*******************Loading customer table into customer_table_df*******************"
)
customer_table_df = read_db_spark(spark, config["customer_table_name"])
print("customer table loaded successfully")
customer_table_df.printSchema()

# product table
logger.info(
    "*******************Loading product table into product_table_df*******************"
)
product_table_df = read_db_spark(spark, config["product_table"])
print("product table loaded successfully")
product_table_df.printSchema()

# product_staging_table
logger.info(
    "*******************Loading staging table into product_staging_table_df*******************"
)
product_staging_table_df = read_db_spark(spark, config["product_staging_table"])
product_staging_table_df.printSchema()
print("product staging table loaded successfully")

# sales_team table
logger.info(
    "*******************Loading sales team table into sales_team_table_df*******************"
)
sales_team_table_df = read_db_spark(spark, config["sales_team_table"])
sales_team_table_df.printSchema()
print("sales team table loaded successfully")

# store table
logger.info(
    "*******************Loading store table into store_table_df*******************"
)
store_table_df = read_db_spark(spark, config["store_table"])
store_table_df.printSchema()
print("store table loaded successfully")


gcp_customer_store_sales_df_join = dimensions_tables_join(
    final_df_to_process, customer_table_df, store_table_df, sales_team_table_df
)


# Final enriched data
logger.info("*******************Final enriched data*******************")
gcp_customer_store_sales_df_join.show()

# Write the customer data into customer datamart in parquet format
# file will be written to local first
# move the raw data to gcs bucket for reporting tool
# write reporting data into mysql table also
logger.info(
    "*******************Write the data into customer datamart*******************"
)
final_customer_data_mart_df = gcp_customer_store_sales_df_join.select(
    "ct.customer_id",
    "ct.first_name",
    "ct.last_name",
    "ct.address",
    "ct.pincode",
    "phone_number",
    "sales_date",
    "total_cost",
)
logger.info("*******************Final data for customer Data mart*******************")
final_customer_data_mart_df.show()


write_df(final_customer_data_mart_df, config["customer_data_mart_local_file"])

logger.info(
    f"*******************customer data written to local disk at {config['customer_data_mart_local_file']}*******************"
)

# Move data to gcs bucket for customer_data_mart
logger.info(
    "*******************Data movement from local to gcs for customer data mart*******************"
)


move_file_to_GCS(
    gcs_client,
    config["gcs_customer_datamart_directory"],
    config["GCS_BUCKET_NAME"],
    config["customer_data_mart_local_file"],
)


# sales_team Data mart
logger.info(
    "*******************Write the data into sales team data mart*******************"
)
final_sales_team_data_mart_df = gcp_customer_store_sales_df_join.select(
    "store_id",
    "sales_person_id",
    "sales_person_first_name",
    "sales_person_last_name",
    "store_manager_name",
    "manager_id",
    "is_manager",
    "sales_person_address",
    "sales_person_pincode",
    "sales_date",
    "total_cost",
    F.expr("SUBSTRING(sales_date, 1, 7) as sales_month"),
)

logger.info("*******************Final data for Sales team data mart*******************")
final_sales_team_data_mart_df.show()

write_df(final_sales_team_data_mart_df, config["sales_team_data_mart_local_file"])


logger.info(
    f"*******************Sales team data written to local disk at {config['sales_team_data_mart_local_file']}*******************"
)


# Move data on gcs bucket for sales_data_mart
move_file_to_GCS(
    gcs_client,
    config["gcs_sales_datamart_directory"],
    config["GCS_BUCKET_NAME"],
    config["sales_team_data_mart_local_file"],
)


# Also writing the data into partitions
final_sales_team_data_mart_df.write.format("parquet").option("header", True).mode(
    "overwrite"
).partitionBy("sales_month", "store_id").option(
    "path", config["sales_team_data_mart_partitioned_local_file"]
).save()


# Move data on s3 for partitioned folder
gcp_prefix = "sales_partitioned_data_mart"
current_epoch = int(datetime.datetime.now().timestamp()) * 1000
bucket = gcs_client.get_bucket(config["GCS_BUCKET_NAME"])
for root, dirs, files in os.walk(
    str(config["sales_team_data_mart_partitioned_local_file"])
):
    for file in files:
        print(file)
        local_file_path = os.path.join(root, file)
        relative_file_path = os.path.relpath(
            local_file_path, str(config["sales_team_data_mart_partitioned_local_file"])
        )
        print(f"{relative_file_path=}")
        gcp_key = f"{gcp_prefix}/{current_epoch}/{relative_file_path}"
        print(f"{gcp_key=}")
        blob = bucket.blob(gcp_key)
        blob.upload_from_filename(local_file_path)
        logger.info(
            f"Uploaded {local_file_path} to gs://{config['GCS_BUCKET_NAME']}/{gcp_key}"
        )


logger.info(
    "*******************Calculating customer every month purchased amount*******************"
)
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info(
    "*******************Calculation of customer mart done and written into the table*******************"
)


# calculation for sales team mart
# find out the total sales done by each sales person every month
# Give the top performer 1% incentive of total sales of the month
# Rest of the sales persons will get no incentive
# write the data into MYSQL table
logger.info("******Calculating sales every month billed amount******")
sales_mart_calculation_table_write(final_sales_team_data_mart_df)
logger.info("****** Calculation of sales mart done and written into the table ******")


# *************** Last Step *********************
# Move the file on S3 into processed folder and delete the local files
move_file_gcs_to_gcs(
    gcs_client,
    config["GCS_BUCKET_NAME"],
    config["gcs_source_directory"],
    config["gcs_processed_directory"],
)


logger.info("******* Deleting sales data from local *******")
delete_local_file(config["local_directory"])
logger.info("******* Deleted sales data from local *******")

logger.info("******* Deleting customer data from local *******")
delete_local_file(config["customer_data_mart_local_file"])
logger.info("******* Deleted customer data from local *******")

logger.info("******* Deleting sales team data from local *******")
delete_local_file(config["sales_team_data_mart_local_file"])
logger.info("******* Deleted sales team data from local *******")

logger.info("******* Deleting sales team data mart from local *******")
delete_local_file(config["sales_team_data_mart_partitioned_local_file"])
logger.info("******* Deleted sales team data mart from local *******")


# update the status of staging table
formatted_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
update_statements = []
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements = f"""UPDATE {config["MYSQL_DATABASE"]}.{config["product_staging_table"]} 
        SET status = 'I', updated_date = '{formatted_date}' 
        where file_name = '{filename}'"""
        update_statements.append(statements)

    logger.info(f"Update statements created for staging table --- {update_statements}")
    logger.info("****** Connecting with MYSQL Server ******")
    connection = get_connection()
    cursor = connection.cursor()
    logger.info("****** MYSQL server connection established successfully ******")
    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error(
        "****** There were no correct_files. There seems to be some error in between ******"
    )
    sys.exit()


input("Press enter to terminate")
