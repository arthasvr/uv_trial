import findspark


# from pyspark.sql import *
from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import *

findspark.init()


from src.main.utility.logging_config import *


def spark_session() -> "SparkSession":
    #  The builder in SparkSession.builder is a class property of SparkSession which returns a instance of Builder() class
    # Then the master, appName, config methods all set the configs of the builder instance
    #  once all the configs are set, the getOrCreate() method of the earlier created Builder instance,
    # returns an instance of SparkSession class (basically a sparksession)
    spark: SparkSession = (
        SparkSession.builder.master("local[*]")  # type: ignore
        .appName("manish_spark2")
        .config(
            "spark.driver.extraClassPath", "C:\\my_sql_jar\\mysql-connector-j-9.0.0.jar"
        )
        .getOrCreate()
    )
    logger.info("spark session %s", spark)
    print(f"spark session {spark}")
    print(type(spark))
    return spark
