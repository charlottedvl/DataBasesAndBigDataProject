import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_utc_timestamp, concat_ws, regexp_replace, explode, split, desc, collect_list, \
    create_map, lit, concat, monotonically_increasing_id, lower, date_format

from bertopic import BERTopic
from bertopic.representation import KeyBERTInspired
from sklearn.datasets import fetch_20newsgroups
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline
import sparknlp
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.clustering import LDA
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

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

    new_df.printSchema()
    new_df = format_df(new_df)

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

    new_df.printSchema()
    # new_df.show()
    format_df(new_df)
    save_as_parquet(new_df, formatted_path, file_name, spark)
    spark.stop()


def save_as_parquet(df, formatted_path, file_name, spark):
    parquet_file_name = formatted_path + file_name.replace(".json", ".snappy.parquet")
    df.write.save(parquet_file_name, mode="overwrite")
    spark.stop()


def format_df(df):
    new_df = df.withColumn('date', date_format(to_utc_timestamp(col('date'), "UTC"), "yyyy-MM-dd HH:mm:ss"))

    new_df = create_id(new_df)

    new_df = remove_html(new_df)

    return new_df


def create_id(df):
    return df.withColumn('id', lower(regexp_replace(concat('date', 'name'), '[^a-zA-Z0-9]', '')))


def remove_html(df):
    return df.withColumn('clean_content', regexp_replace('content', '<[^>]*>', ''))
