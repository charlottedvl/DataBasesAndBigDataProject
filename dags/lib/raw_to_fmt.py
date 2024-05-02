import os
import pandas as pd

datalake_root_folder = "./datalake/"


def convert_raw_to_formatted(group_name, table_name, current_day, file_name):
    path = group_name + "/" + table_name + "/" + current_day + "/"

    raw_path = datalake_root_folder + "raw/" + path + file_name
    formatted_path = datalake_root_folder + "formatted/" + path

    if not os.path.exists(formatted_path):
        os.makedirs(formatted_path)

    df = pd.read_csv(raw_path, sep='\t')
    parquet_file_name = file_name.replace(".tsv.gz", ".snappy.parquet")
    df.to_parquet(formatted_path + parquet_file_name)
