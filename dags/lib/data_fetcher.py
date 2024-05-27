import json
import os
from datetime import date
import requests
from dotenv import load_dotenv


datalake_root_folder = "datalake/"


def fetch_data_from_themuse(s3):
    current_day = date.today().strftime("%Y%m%d")

    target_path = datalake_root_folder + "raw/themuse/job/" + current_day

    if not os.path.exists(target_path):
        os.makedirs(target_path)
    url = 'https://www.themuse.com/api/public/jobs?page=1'
    r = requests.get(url, allow_redirects=True).json()

    save_as_json(s3, target_path, r)


def fetch_data_from_findwork(s3):

    current_day = date.today().strftime("%Y%m%d")

    target_path = datalake_root_folder + "raw/findwork/job/" + current_day
    # Load the environment variables
    load_dotenv()

    if not os.path.exists(target_path):
        os.makedirs(target_path)

    url = 'https://findwork.dev/api/jobs/?sort_by=relevance'
    authorization = 'Token ' + os.environ.get("API_KEY")
    headers = {
        'Authorization': authorization
    }
    r = requests.get(url, allow_redirects=True, headers=headers).json()

    save_as_json(s3, target_path, r)


def save_as_json(s3, target_path, response):

    with open(target_path + '/offers.json', 'w') as file:
        json.dump(response.get("results"), file)

    s3.upload_file(target_path, target_path + '/offers.json')
