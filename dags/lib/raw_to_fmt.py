import os
from pyspark.sql.functions import col, to_utc_timestamp, concat_ws, regexp_replace, concat, lower, udf

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

datalake_root_folder = "datalake/"


def convert_raw_to_formatted_themuse(current_day, file_name, s3):

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
    formatted_df = format_df(new_df)

    save_as_parquet(formatted_df, formatted_path, file_name, s3)
    spark.stop()


def convert_raw_to_formatted_findwork(current_day, file_name, s3):

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
    save_as_parquet(formatted_df, formatted_path, file_name, s3)
    spark.stop()


def save_as_parquet(df, formatted_path, file_name, s3):
    parquet_file_name = formatted_path + file_name.replace(".json", ".snappy.parquet")
    df.write.save(parquet_file_name, mode="overwrite")

    if s3.upload_directory(parquet_file_name):
        print(f"Successfully uploaded {file_name} to {formatted_path}")
    else:
        print(f"Failed to upload {file_name} to {formatted_path}")



def format_df(df):
    new_df = df.withColumn('date', to_utc_timestamp(col('date'), "UTC"))

    new_df = create_id(new_df)

    new_df = remove_html(new_df)

    new_df = format_location(new_df)

    return new_df


def create_id(df):
    return df.withColumn('_id', lower(regexp_replace(concat('date', 'name'), '[^a-zA-Z0-9]', '')))


def remove_html(df):
    return df.withColumn('description', regexp_replace('content', '<[^>]*>', ''))


def find_country(location):
    us_states = {
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS',
        'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY',
        'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV',
        'WI', 'WY', "DC"
    }
    print("location: " + str(location))
    if location is None:
        return None
    if 'united states' in location.lower() or 'US' in location or "USA" in location:
        return 'USA'
    elif 'united kingdom' in location.lower() or 'UK' in location:
        return 'UK'
    else:
        try:
            parts = location.split(', ')
        except:
            return None
    try:
        if len(parts) == 2 and parts[1] in us_states:
            return 'USA'
        else:
            return parts[-1]
    except:
        return None


find_country_udf = udf(find_country, StringType())


def format_location(df):
    return df.withColumn('location', find_country_udf(df['location']))
