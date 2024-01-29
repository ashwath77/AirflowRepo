from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 4,
    'start_date': datetime(2023, 12, 12),
    'retries_delay': timedelta(minutes=5)
}

def odd_numbers():
    l = [5, 6, 7, 8, 9]
    odd = [i for i in l if i % 2 != 0]
    print(odd)


with DAG(
        default_args=default_args,
        dag_id="PythonOperator",
        description="This is the python operator",
        start_date=datetime(2023, 7, 12, 22),
        schedule_interval='@daily'
) as dag:

    task1 = PythonOperator(
        task_id="my_pyhton",
        python_callable=odd_numbers,
    )


    task1