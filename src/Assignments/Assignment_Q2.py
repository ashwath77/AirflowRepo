from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Default parameters:
default_args = {
    'owner': 'ashwath',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 12),
    'retry_delay': timedelta(minutes=5),
}

# DAG object
dag = DAG(
    'Example_on_Bash_Operator',
    default_args=default_args,
    description='Example using BashOperator',
    schedule_interval=timedelta(days=1),
)

# First task
task_message = BashOperator(
    task_id='print_message',
    bash_command='echo "Hello, Ashwath!"',
    dag=dag,
)

# Second Task
task_dir_create = BashOperator(
    task_id='create_directory',
    bash_command='mkdir /tmp/airflow_example2',
    dag=dag,
)

#Dependencies
task_message >> task_dir_create

