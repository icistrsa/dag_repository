from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import random
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.operators.python import get_current_context

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),

}

dag = DAG('example_retry',
    default_args=default_args,
    catchUp=False
    )

def random_exception_task():
    context = get_current_context()
    val = random.choice(range(3))
    print(f"val : {val}")
    if val == 1:
        raise AirflowFailException(f"Fail task {context['ti'].task_id}")
    raise Exception()
    
    # do something

task = PythonOperator(
    task_id='sample_retry',
    python_callable=random_exception_task,
    retries=5,
    retry_delay=timedelta(minutes=3),
    dag=dag
)

task2 = PythonOperator(
    task_id='sample_retry2',
    python_callable=random_exception_task,
    retries=5,
    retry_delay=timedelta(minutes=3),
    dag=dag
)

task >> task2