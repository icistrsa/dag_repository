from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('my_dag', default_args=default_args)

def my_task():
    # do something

task = PythonOperator(
    task_id='my_task',
    python_callable=my_task,
    retries=5,
    retry_delay=timedelta(minutes=10),
    dag=dag)
