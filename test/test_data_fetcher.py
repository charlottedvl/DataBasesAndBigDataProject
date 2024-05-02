from dags.lib.data_fetcher import fetch_data_from_themuse, fetch_data_from_findwork
from dags.lib.raw_to_fmt import convert_raw_to_formatted
from datetime import date

current_day = date.today().strftime("%Y%m%d")
filetofind = "announce.json"
group = "findwork"
table = "job"
# date_column = "publication_date"
date_column = "date_posted"

convert_raw_to_formatted(group, table, current_day, filetofind, date_column)
