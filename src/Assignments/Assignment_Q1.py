from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 25),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG object
dag = DAG(
    'Simple_DAG_Demo_Py',
    default_args=default_args,
    description='simple DAG',
    schedule_interval=timedelta(days=1),
)

# First task on 'DummyOperator'
task1 = DummyOperator(
    task_id='task1',
    dag=dag,
)

# Second task on 'Python'
def print_hello():
    print("Hello, This the basic airflow code!")

task2 = PythonOperator(
    task_id='task2',
    python_callable=print_hello,
    dag=dag,
)

# Dependencies between tasks:
task1 >> task2
