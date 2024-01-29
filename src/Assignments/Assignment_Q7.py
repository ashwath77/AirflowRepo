from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 9),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'scheduling_dag',
    default_args=default_args,
    description='DAG with different scheduling options',
    schedule_interval='0 0 * * *',
    catchup=False,
)

def my_function():
    print("This is the python function")


task_c1 = PythonOperator(
    task_id='task_c1',
    python_callable=my_function,
    dag=dag,
)


task_i2 = PythonOperator(
    task_id='task_i2',
    python_callable=my_function,
    dag=dag,
)


task_c1 >> task_i2


