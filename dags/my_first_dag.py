from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


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


    tasks = []
    TASKS_COUNT = 2

    produce_usage_task = PythonOperator(
        task_id='produce_usage',
        python_callable=launch_task,
        provide_context=True,
        op_kwargs={'task_number': 'task'}
    )

    index_to_elastic_task = PythonOperator(
        task_id='index_to_elastic',
        python_callable=launch_task,
        provide_context=True,
        op_kwargs={'task_number': 'task'}
    )

    for i in range(TASKS_COUNT):
        first_task = PythonOperator(
            task_id='source_to_raw' + str(i + 1),
            python_callable=launch_task,
            provide_context=True,
            op_kwargs={'task_number': 'task' + str(i + 1)}
        )

        second_task = PythonOperator(
            task_id='raw_to_formatted' + str(i + 1),
            python_callable=launch_task,
            provide_context=True,
            op_kwargs={'task_number': 'task' + str(i + 1)}
        )

        first_task.set_downstream(second_task)
        second_task.set_downstream(produce_usage_task)

    produce_usage_task.set_downstream(index_to_elastic_task)


