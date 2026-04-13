from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {"owner": "ahmed", "retries": 5, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="elt_dag",
    default_args=default_args,
    description="A DAG for ELT process",
    start_date=datetime(2022, 2, 2),
    schedule=None,
) as dag:
    task1 = BashOperator(task_id="first_task", bash_command="echo 'I AM TASK ONE!'")

    task2 = BashOperator(task_id="second_task", bash_command="echo 'I AM TASK TWO!!'")

    task3 = BashOperator(task_id="third_task", bash_command="echo 'I AM TASK THREE!!!'")

    task1 >> [task2, task3]
