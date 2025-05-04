from pyspark.sql.functions import *
from pyspark.sql.window import Window
from dotenv import dotenv_values
from src.main.write.dataframe_writer import write_df

config = dotenv_values(".env")


def customer_mart_calculation_table_write(
    final_customer_data_mart_df: DataFrame,
) -> None:
    print("from inside customer_mart_calculation_table_write function")

    final_customer_data_mart_df.show()

    window = Window.partitionBy("customer_id", "sales_date_month")
    final_customer_data_mart = (
        final_customer_data_mart_df.withColumn(
            "sales_date_month", substring(col("sales_date"), 1, 7)
        )
        .withColumn(
            "total_sales_every_month_by_each_customer", sum("total_cost").over(window)
        )
        .select(
            "customer_id",
            concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"),
            "address",
            "phone_number",
            "sales_date_month",
            col("total_sales_every_month_by_each_customer").alias("total_sales"),
        )
        .distinct()
    )

    final_customer_data_mart.show()

    write_df(final_customer_data_mart_df, config["customer_data_mart_table"])
