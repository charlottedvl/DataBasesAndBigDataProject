import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_utc_timestamp

datalake_root_folder = "./datalake/"


def convert_raw_to_formatted(group_name, table_name, current_day, file_name, date_column):
    path = group_name + "/" + table_name + "/" + current_day + "/"

    raw_path = datalake_root_folder + "raw/" + path + file_name
    formatted_path = datalake_root_folder + "formatted/" + path

    if not os.path.exists(formatted_path):
        os.makedirs(formatted_path)

    spark = SparkSession.builder \
        .appName("FormatData") \
        .getOrCreate()

    df = spark.read.option("sep", "\t").json(raw_path)

    print(df.columns)

    df = df.withColumn(date_column, to_utc_timestamp(col(date_column), "UTC"))

    parquet_file_name = formatted_path + file_name.replace(".json", ".snappy.parquet")

    df.write.mode("overwrite").parquet(parquet_file_name)

    spark.stop()
