from dags.lib.data_fetcher import fetch_data_from_themuse, fetch_data_from_findwork
from dags.lib.raw_to_fmt_imdb import convert_raw_to_formatted
from dags.lib.combine_data import combine_data
from datetime import date

current_day = date.today().strftime("%Y%m%d")
filetofind = "title.ratings.tsv.gz"


fetch_data_from_themuse()
fetch_data_from_findwork()
