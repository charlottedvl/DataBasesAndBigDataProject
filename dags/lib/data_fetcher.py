import os
from datetime import date
import requests
from dotenv import load_dotenv, dotenv_values

datalake_root_folder = "./datalake/"


def fetch_data_from_themuse(**kwargs):
    current_day = date.today().strftime("%Y%m%d")

    target_path = datalake_root_folder + "raw/themuse/job/" + current_day + "/"

    if not os.path.exists(target_path):
        os.makedirs(target_path)
    url = 'https://www.themuse.com/api/public/jobs?page=1'
    r = requests.get(url, allow_redirects=True)
    open(target_path + 'jobs.announce.themuse.tsv.gz', 'wb').write(r.content)


def fetch_data_from_findwork(**kwargs):
    current_day = date.today().strftime("%Y%m%d")

    target_path = datalake_root_folder + "raw/findwork/job/" + current_day + "/"
    # Load the environment variables
    load_dotenv()

    if not os.path.exists(target_path):
        os.makedirs(target_path)

    url = 'https://findwork.dev/api/jobs/?sort_by=relevance'
    authorization = 'Token ' + os.environ.get("API_KEY")
    headers = {
        'Authorization': authorization
    }
    r = requests.get(url, allow_redirects=True, headers=headers)
    open(target_path + 'jobs.announce.findwork.tsv.gz', 'wb').write(r.content)
