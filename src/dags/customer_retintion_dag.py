"""
DAG: еженедельное обновление витрины customer_retention (по понедельникам в 03:00).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


# Базовые настройки
POSTGRES_CONN_ID = 'postgresql_gilyazov'
SQL_FILE = 'migrations/mart.f_customer_retention.sql'


default_args = {
    'owner': 'vladislav_gilyazov',
    'email': ['octagon4469@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}


with DAG(
    dag_id='customer_retention_weekly',
    default_args=default_args,
    description='Updating the customer retention showcase',
    schedule_interval='0 3 * * 1',
    start_date=datetime.today() - timedelta(weeks=6),
    catchup=True,
    tags=['customer_analytics', 'weekly']
) as dag:

    
    mart_update = PostgresOperator(
        task_id='mart_update',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=SQL_FILE,
        params={'business_dt': '{{ ds }}'}  # Передаём дату выполнения в SQL
    )

    
    mart_update
