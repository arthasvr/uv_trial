from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from src.main.utility.spark_session import spark_session

spark: SparkSession = spark_session()

df1: DataFrame = spark.read.csv(
    "F:\\manish_project_local_dirs\\manually_generated_csv\\sales_data.csv", header=True
)

df1.printSchema()

df1.withColumn("product_name", F.upper(df1.product_name)).show(5, False)
