import os
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.index_data import index_data
from lib.combine_data import combine_data
from lib.raw_to_fmt import convert_raw_to_formatted_themuse, convert_raw_to_formatted_findwork
from lib.data_fetcher import fetch_data_from_themuse, fetch_data_from_findwork
from lib.s3_manager import S3Manager
from dotenv import load_dotenv

load_dotenv()

current_day = date.today().strftime("%Y%m%d")
s3 = S3Manager( os.environ.get("AWS_CONN_ID"), os.environ.get("BUCKET_NAME"))
elastic_user = os.environ.get("ELASTIC_USERNAME")
elastic_passwd = os.environ.get("ELASTIC_PASSWORD")

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


    def launch_task(**kwargs):
        print("Hello Airflow - This is Task with task_number:", kwargs['task_number'])
        print("kwargs['dag_run']", kwargs["dag_run"].execution_date)

    produce_usage_task = PythonOperator(
        task_id='produce_usage',
        python_callable=combine_data,
        provide_context=True,
        op_kwargs={'task_number': 'task',
                   "current_day": current_day,
                   's3': s3
                   }
    )

    index_to_elastic_task = PythonOperator(
        task_id='index_to_elastic',
        python_callable=index_data,
        provide_context=True,
        op_kwargs={'task_number': 'task',
                   "current_day": current_day,
                   'username': elastic_user,
                   'password': elastic_passwd,
    }
    )

    # Data from themuse
    first_task_themuse = PythonOperator(
        task_id='source_to_raw_themuse',
        python_callable=fetch_data_from_themuse,
        provide_context=True,
        op_kwargs={'task_number': 'task1', 's3': s3},
    )

    second_task_themuse = PythonOperator(
        task_id='raw_to_formatted_themuse',
        python_callable=convert_raw_to_formatted_themuse,
        provide_context=True,
        op_kwargs={'task_number': 'task2',
                   "current_day": current_day,
                   "file_name": "offers.json",
                   "s3": s3,
                   },
    )

    first_task_themuse.set_downstream(second_task_themuse)
    second_task_themuse.set_downstream(produce_usage_task)

    # Data from findwork
    first_task_findwork = PythonOperator(
        task_id='source_to_raw_findwork',
        python_callable=fetch_data_from_findwork,
        provide_context=True,
        op_kwargs={'task_number': 'task3', 's3': s3},
    )

    second_task_findwork = PythonOperator(
        task_id='raw_to_formatted_findwork',
        python_callable=convert_raw_to_formatted_findwork,
        provide_context=True,
        op_kwargs={'task_number': 'task4',
                   "current_day": current_day,
                   "file_name": "offers.json",
                   "s3": s3
                   },
    )

    first_task_findwork.set_downstream(second_task_findwork)
    second_task_findwork.set_downstream(produce_usage_task)

    produce_usage_task.set_downstream(index_to_elastic_task)


