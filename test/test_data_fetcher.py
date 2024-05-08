from dags.lib.data_fetcher import fetch_data_from_themuse, fetch_data_from_findwork
from dags.lib.raw_to_fmt import convert_raw_to_formatted_themuse, convert_raw_to_formatted_findwork
from dags.lib.combine_data import combine_data_findwork, combine_data
from datetime import date

current_day = date.today().strftime("%Y%m%d")
filetofind = "announce.json"
group = "themuse"
table = "job"
# date_column = "publication_date"
findwork_date_column = "date_posted"
themuse_date_column = "publication_date"

parquet_file = "announce.snappy.parquet"

#fetch_data_from_findwork()
#fetch_data_from_themuse()
#convert_raw_to_formatted_themuse(current_day, filetofind)
#convert_raw_to_formatted_findwork(current_day, filetofind)
combine_data(current_day)
