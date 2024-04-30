import os
from datetime import date
import requests

DATALAKE_ROOT_FOLDER = "./datalake/"


def fetch_data_from_imdb(**kwargs):
    current_day = date.today().strftime("%Y%m%d")

    TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/imdb/MovieRating/" + current_day + "/"

    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)
    url = 'https://datasets.imdbws.com/title.ratings.tsv.gz'
    r = requests.get(url, allow_redirects=True)
    open(TARGET_PATH + 'title.ratings.tsv.gz', 'wb').write(r.content)
