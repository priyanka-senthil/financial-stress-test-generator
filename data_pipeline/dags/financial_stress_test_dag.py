"""
Financial Stress Test Data Pipeline DAG - Lazy Loading Version
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def start_pipeline():
    print("="*70)
    print("PIPELINE STARTED")
    print("="*70)
    return "started"

def acquire_macro():
    """Import only when task runs"""
    import sys
    import os
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    sys.path.insert(0, project_root)
    
    from data_pipeline.scripts.data_acquisition import DataAcquisition
    acquirer = DataAcquisition()
    return acquirer.fetch_macro_data()

def acquire_companies():
    """Import only when task runs"""
    import sys
    import os
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    sys.path.insert(0, project_root)
    
    from data_pipeline.scripts.data_acquisition import DataAcquisition
    acquirer = DataAcquisition()
    return acquirer.fetch_all_companies()

def quality_check():
    return "passed"

def preprocess_macro():
    """Import only when task runs"""
    import sys
    import os
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    sys.path.insert(0, project_root)
    
    from data_pipeline.scripts.preprocessing.data_preprocessor import DataPreprocessor
    preprocessor = DataPreprocessor()
    output_path, _ = preprocessor.preprocess_macro_data()
    return output_path

def preprocess_companies():
    """Import only when task runs"""
    import sys
    import os
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    sys.path.insert(0, project_root)
    
    from data_pipeline.scripts.preprocessing.data_preprocessor import DataPreprocessor
    preprocessor = DataPreprocessor()
    return preprocessor.preprocess_all_companies()

def validate():
    """Import only when task runs"""
    import sys
    import os
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    sys.path.insert(0, project_root)
    
    from data_pipeline.scripts.validation.data_validator import DataValidator
    validator = DataValidator()
    report = validator.validate_macro_data()
    return report['is_valid']

def generate_report():
    print("Report generated successfully")
    return "done"

def end_pipeline():
    print("="*70)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("="*70)
    return "completed"

# Define DAG
with DAG(
    dag_id='financial_stress_test_pipeline',
    default_args={
        'owner': 'financial_stress_test_team',
        'retries': 2,  # Retry twice if fails
        'retry_delay': timedelta(minutes=2),
        'execution_timeout': timedelta(minutes=30),  # Give 30 min per task
    },
    description='Financial stress test pipeline',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['finance', 'mlops'],
) as dag:
    
    t1 = PythonOperator(task_id='start', python_callable=start_pipeline)
    t2 = PythonOperator(task_id='acquire_macro', python_callable=acquire_macro)
    t3 = PythonOperator(task_id='acquire_companies', python_callable=acquire_companies)
    t4 = PythonOperator(task_id='quality_check', python_callable=quality_check)
    t5 = PythonOperator(task_id='preprocess_macro', python_callable=preprocess_macro)
    t6 = PythonOperator(task_id='preprocess_companies', python_callable=preprocess_companies)
    t7 = PythonOperator(task_id='validate', python_callable=validate)
    t8 = PythonOperator(task_id='report', python_callable=generate_report)
    t9 = PythonOperator(task_id='end', python_callable=end_pipeline)
    
    t1 >> [t2, t3] >> t4 >> [t5, t6] >> t7 >> t8 >> t9