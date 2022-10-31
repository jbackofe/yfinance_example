from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from modules.yfinance_example import pullUploadFinanceData

pullUploadFinanceData()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2022, 10, 30),
    'email': ['backofen.jared@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'yfinance_dag',
    default_args=default_args,
    description='Pulls data from yahoo and uploads to AWS S3',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='whole_yfinance_etl',
    python_callable=pullUploadFinanceData,
    dag=dag,
)

run_etl
