# Stop Airflow (Ctrl+C in the terminal where it's running)

cd ~/Desktop/MLOPs/financial-stress-test-generator
source Project/bin/activate
export AIRFLOW_HOME=~/Desktop/MLOPs/financial-stress-test-generator/data_pipeline

# Remove database
rm -f data_pipeline/airflow.db*

# Reinitialize
airflow db migrate

# Start Airflow
airflow standalone


# List all DAGs to confirm it exists 
airflow dags list | grep financial

# Trigger your dag via Terminal
airflow dags trigger financial_stress_test_pipeline

# Check dag status
airflow dags list-runs financial_stress_test_pipeline