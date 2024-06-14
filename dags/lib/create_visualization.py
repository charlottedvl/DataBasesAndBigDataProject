import os

import requests
import json

from dotenv import load_dotenv


def create_dashboard():
    load_dotenv()

    kibana_url = 'https://localhost:5601'

    import_url = f'{kibana_url}/api/saved_objects/_import'

    username = os.environ.get("ELASTIC_USERNAME")
    password = os.environ.get("ELASTIC_PASSWORD")
    auth = (username, password)

    headers = {
        'kbn-xsrf': 'true'
    }

    path = "../dashboards/dashboard.ndjson"
    with open(path, 'r') as f:
        dashboard_ndjson = f.read()

    files = {
        'file': ('dashboard.ndjson', dashboard_ndjson, 'application/ndjson')
    }

    response = requests.post(import_url, headers=headers, auth=auth, files=files, verify=False)

    if response.status_code == 200:
        print("Dashboard successfully imported !")
    else:
        print(f"Error while importing dashboard: {response.status_code}")
        print(response.text)

