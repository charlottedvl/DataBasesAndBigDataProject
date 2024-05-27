import os

import pyarrow.parquet as pq

from elasticsearch import Elasticsearch, helpers

datalake_root_folder = "datalake/"


def index_data(current_day, username, password):

    path_to_file = datalake_root_folder + "combined/job/" + current_day + "/offers.snappy.parquet/"
    client = Elasticsearch([{'host': '192.168.68.102', 'port': 9200, 'scheme': 'https'}],
                           basic_auth=(username, password),
                           verify_certs=False)

    parquet_table = pq.read_table(path_to_file)
    df = parquet_table.to_pandas()

    docs = df.to_dict(orient='records')

    if not client.indices.exists(index='job-test'):
        client.indices.create(index='job-test')
    helpers.bulk(client, docs, index="job-test")
    print(f"{len(docs)} documents indexed !")
