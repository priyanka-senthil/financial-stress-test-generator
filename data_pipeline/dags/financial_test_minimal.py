"""
Minimal Financial Stress Test DAG - For Testing
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello from minimal financial DAG!")
    return "success"

dag = DAG(
    dag_id='financial_test_minimal',
    description='Minimal test',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test'],
)

task = PythonOperator(
    task_id='hello',
    python_callable=hello,
    dag=dag,
)

print("✓ Minimal DAG loaded successfully")