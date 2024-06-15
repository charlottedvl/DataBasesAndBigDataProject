import pyarrow.parquet as pq

from elasticsearch import Elasticsearch, helpers

datalake_root_folder = "datalake/"


def index_data(current_day, username, password, s3):

    path_to_file = datalake_root_folder + "combined/job/" + current_day + "/offers.snappy.parquet/"

    parquet_table = s3.download_file(path_to_file)

    records = parquet_table.to_pydict()
    docs = [{key: records[key][i] for key in records} for i in range(len(records[next(iter(records))]))]

    client = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'https'}],
                           basic_auth=(username, password),
                           verify_certs=False)

    if not client.indices.exists(index='jobs'):
        client.indices.create(index='jobs')
    helpers.bulk(client, docs, index="jobs")
    print(f"{len(docs)} documents indexed !")
