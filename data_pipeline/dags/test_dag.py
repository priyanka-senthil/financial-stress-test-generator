from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello from test DAG!")
    return "Success"

dag = DAG(
    'test_simple_dag',
    description='Simple test DAG',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

task = PythonOperator(
    task_id='hello_task',
    python_callable=hello,
    dag=dag,
)