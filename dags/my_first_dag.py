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


    for i in range(TASKS_COUNT):
        first_task = PythonOperator(
            task_id='source_to_raw' + str(i),
            python_callable=launch_task,
            provide_context=True,
            op_kwargs={'task_number': 'task' + str(i)}
        )

        second_task = PythonOperator(
            task_id='raw_to_formatted' + str(i),
            python_callable=launch_task,
            provide_context=True,
            op_kwargs={'task_number': 'task' + str(i)}
        )
        first_task.set_downstream(second_task)

        tasks.append(second_task)

    produce_usage_task = PythonOperator(
        task_id='produce_usage' + str(i),
        python_callable=launch_task,
        provide_context=True,
        op_kwargs={'task_number': 'task' + str(i)}
    )
    # In python [-1] get the last element in an array



    index_to_elastic_task = PythonOperator(
        task_id='index_to_elastic' + str(i),
        python_callable=launch_task,
        provide_context=True,
        op_kwargs={'task_number': 'task' + str(i)}
    )

    for i in range(TASKS_COUNT - 1):
        tasks[i].set_downstream(produce_usage_task)

    produce_usage_task.set_downstream(index_to_elastic_task)


