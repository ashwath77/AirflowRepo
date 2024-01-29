from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Assignment5',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

# Define tasks
def odd_numbers():
    l = [11, 22, 33, 44, 55]
    odd = [i for i in l if i % 2 != 0]
    return odd


task1 = PythonOperator(
    task_id="my_pyhton",
    python_callable=odd_numbers,
    dag=dag,
)

def task2():
    print("Hello, Executing the task2")

task2 = PythonOperator(
    task_id='task2',
    python_callable=task2,
    retries=3,
    retry_delay=timedelta(minutes=5),
    trigger_rule='all_success',
    dag=dag,
)

task1.set_downstream(task2)


