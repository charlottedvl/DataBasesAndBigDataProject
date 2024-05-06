import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_utc_timestamp, concat_ws

datalake_root_folder = "./datalake/"


def convert_raw_to_formatted_themuse(current_day, file_name):
    path = "themuse/job/" + current_day + "/"

    raw_path = datalake_root_folder + "raw/" + path + file_name
    formatted_path = datalake_root_folder + "formatted/" + path

    if not os.path.exists(formatted_path):
        os.makedirs(formatted_path)

    spark = SparkSession.builder \
        .appName("FormatData") \
        .getOrCreate()

    df = spark.read.option("sep", "\t").json(raw_path)

    new_df = df.select(col('publication_date').alias('date'),
                       col('name'),
                       concat_ws(', ', col('locations.name')).alias('location'),
                       col('tags'),
                       col('company.name').alias('company'),
                       col('contents').alias('content'))

    new_df = new_df.withColumn('date', to_utc_timestamp(col('date'), "UTC"))

    new_df.printSchema()
    # new_df.show()
    save_as_parquet(new_df, formatted_path, file_name, spark)
    spark.stop()


def convert_raw_to_formatted_findwork(current_day, file_name):
    path = "findwork/job/" + current_day + "/"

    raw_path = datalake_root_folder + "raw/" + path + file_name
    formatted_path = datalake_root_folder + "formatted/" + path

    if not os.path.exists(formatted_path):
        os.makedirs(formatted_path)

    spark = SparkSession.builder \
        .appName("FormatData") \
        .getOrCreate()

    df = spark.read.option("sep", "\t").json(raw_path)

    new_df = df.select(col('date_posted').alias('date'),
                       col('role').alias('name'),
                       col('location'),
                       col('keywords').alias('tags'),
                       col('company_name').alias('company'),
                       col('text').alias('content'))

    new_df = new_df.withColumn('date', to_utc_timestamp(col('date'), "UTC"))

    new_df.printSchema()
    # new_df.show()
    save_as_parquet(new_df, formatted_path, file_name, spark)
    spark.stop()


def save_as_parquet(df, formatted_path, file_name, spark):
    parquet_file_name = formatted_path + file_name.replace(".json", ".snappy.parquet")
    df.write.save(parquet_file_name, mode="overwrite")

