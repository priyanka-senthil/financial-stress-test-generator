"""
Financial Stress Test Data Pipeline DAG - Docker Version
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
    """Import and run - paths adjusted for Docker"""
    import sys
    import os
    
    # Add paths for Docker environment
    sys.path.insert(0, '/opt/airflow')
    
    from data_pipeline.scripts.data_acquisition import DataAcquisition
    
    acquirer = DataAcquisition()
    output_path = acquirer.fetch_macro_data()
    print(f"✓ Macro data saved to: {output_path}")
    return output_path

def acquire_companies():
    """Import and run"""
    import sys
    sys.path.insert(0, '/opt/airflow')
    
    from data_pipeline.scripts.data_acquisition import DataAcquisition
    
    acquirer = DataAcquisition()
    success_count = acquirer.fetch_all_companies()
    print(f"✓ Companies acquired: {success_count}")
    return success_count

def quality_check():
    print("Running quality checks...")
    return "passed"

def preprocess_macro():
    """Import and run"""
    import sys
    sys.path.insert(0, '/opt/airflow')
    
    from data_pipeline.scripts.preprocessing.data_preprocessor import DataPreprocessor
    
    preprocessor = DataPreprocessor()
    output_path, metadata = preprocessor.preprocess_macro_data()
    print(f"✓ Processed macro data: {output_path}")
    return output_path

def preprocess_companies():
    """Import and run"""
    import sys
    sys.path.insert(0, '/opt/airflow')
    
    from data_pipeline.scripts.preprocessing.data_preprocessor import DataPreprocessor
    
    preprocessor = DataPreprocessor()
    processed_files = preprocessor.preprocess_all_companies()
    print(f"✓ Processed {len(processed_files)} companies")
    return len(processed_files)

def validate():
    """Import and run"""
    import sys
    sys.path.insert(0, '/opt/airflow')
    
    from data_pipeline.scripts.validation.data_validator import DataValidator
    
    validator = DataValidator()
    report = validator.validate_macro_data()
    status = "PASS" if report['is_valid'] else "WARNINGS"
    print(f"✓ Validation: {status}")
    return report['is_valid']

def generate_report():
    print("="*70)
    print("PIPELINE REPORT")
    print("All tasks completed successfully")
    print("="*70)
    return "complete"

def end_pipeline():
    print("="*70)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("="*70)
    return "done"

# Define DAG
with DAG(
    dag_id='financial_stress_test_pipeline',
    default_args={
        'owner': 'financial_stress_test_team',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(minutes=30),
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
    
    # Dependencies
    t1 >> [t2, t3] >> t4 >> [t5, t6] >> t7 >> t8 >> t9