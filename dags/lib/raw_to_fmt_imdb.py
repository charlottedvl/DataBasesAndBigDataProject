import os
import pandas as pd

DATALAKE_ROOT_FOLDER = "./datalake/"
def convert_raw_to_formatted(file_name, current_day):
    RATING_PATH = DATALAKE_ROOT_FOLDER + "raw/imdb/MovieRating/" + current_day + "/" + file_name
    FORMATTED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/imdb/MovieRating/" + current_day + "/"
    if not os.path.exists(FORMATTED_RATING_FOLDER):
        os.makedirs(FORMATTED_RATING_FOLDER)
    df = pd.read_csv(RATING_PATH, sep='\t')
    parquet_file_name = file_name.replace(".tsv.gz", ".snappy.parquet")
    df.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name)
