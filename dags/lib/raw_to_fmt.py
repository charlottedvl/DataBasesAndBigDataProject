import os
from pyspark.sql.functions import col, to_utc_timestamp, concat_ws, regexp_replace, concat, lower, date_format

from pyspark.sql import SparkSession
import boto3
from botocore.exceptions import NoCredentialsError


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

    # df = spark.read.parquet(f"s3://{bucket_name}/{s3_path}")
    df = spark.read.option("sep", "\t").json(raw_path)

    new_df = df.select(col('publication_date').alias('date'),
                       col('name'),
                       concat_ws(', ', col('locations.name')).alias('location'),
                       col('tags'),
                       col('company.name').alias('company'),
                       col('contents').alias('content'))

    new_df.printSchema()
    formatted_df = format_df(new_df)

    save_as_parquet(formatted_df, formatted_path, file_name, spark)
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

    new_df.printSchema()
    # new_df.show()
    formatted_df = format_df(new_df)
    save_as_parquet(formatted_df, formatted_path, file_name, spark)
    spark.stop()


def save_as_parquet(df, formatted_path, file_name, spark):
    parquet_file_name = formatted_path + file_name.replace(".json", ".snappy.parquet")
    df.write.save(parquet_file_name, mode="overwrite")

    """
    bucket_name = os.environ.get("BUCKET_NAME")
    s3_path = os.environ.get("S3_PATH") + parquet_file_name
    df.write.parquet(f"s3://{bucket_name}/{s3_path}", mode='overwrite')
    """

    spark.stop()



def format_df(df):
    new_df = df.withColumn('date', date_format(to_utc_timestamp(col('date'), "UTC"), "yyyy-MM-dd HH:mm:ss"))

    new_df = create_id(new_df)

    new_df = remove_html(new_df)

    return new_df


def create_id(df):
    return df.withColumn('id', lower(regexp_replace(concat('date', 'name'), '[^a-zA-Z0-9]', '')))


def remove_html(df):
    return df.withColumn('description', regexp_replace('content', '<[^>]*>', ''))
