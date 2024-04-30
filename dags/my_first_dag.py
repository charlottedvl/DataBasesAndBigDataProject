from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.data_fetcher import fetch_data_from_x


with DAG(
        'my_first_dag',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A first DAG',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False, tags=['example'],
) as dag:
    dag.doc_md = """ 
    This is my first DAG in airflow. I can write documentation in Markdown here with **bold text** or __bold text__. """

    task = PythonOperator(
        task_id='fetch_data_from_x',
        python_callable=fetch_data_from_x,
        provide_context=True,
        op_kwargs={'task_number': 'task1'}
    )
