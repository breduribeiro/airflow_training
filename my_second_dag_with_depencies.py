from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import datetime
import time

my_dag = DAG(
    dag_id='my_second_dag_depencies',
    description='A DAG with two tasks',
    tags=['tutorial', 'datascientest'],
    schedule_interval=None,
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(2),
    }
)

# definition of the function to execute


def print_hello_task_1():
    print(datetime.datetime.now())
    print('Hello from task 1')


def print_hello_task_2():
    time.sleep(30)
    print(datetime.datetime.now())
    print('Hello from task 2')


task1 = PythonOperator(
    task_id='second_dag_task1',
    python_callable=print_hello_task_1,
    dag=my_dag
)

task2 = PythonOperator(
    task_id='second_dag_task2',
    python_callable=print_hello_task_2,
    dag=my_dag
)

task1 >> task2
