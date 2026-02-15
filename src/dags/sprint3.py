import time
import requests
import json
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


NICKNAME = Variable.get("sales_mart_nickname", default_var="default_nick")
COHORT = Variable.get("sales_mart_cohort", default_var="default_cohort")
API_KEY = Variable.get("sales_mart_api_key", default_var=None)
POSTGRES_CONN_ID = Variable.get("postgres_conn_id", default_var="postgres_default")

BASE_URL = Variable.get("sales_mart_base_url", default_var="https://api.example.com")


headers = {
    'X-Nickname': NICKNAME,
    'X-Cohort': COHORT,
    'X-Project': 'True',
    'X-API-KEY': API_KEY,
    'Content-Type': 'application/x-www-form-urlencoded'
}


def generate_report(ti):
    print('Making request to generate_report')
    response = requests.post(f'{BASE_URL}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f'Response: {response.content}')

def get_report(ti):
    print('Making request to get_report')
    task_id = ti.xcom_pull(key='task_id')
    report_id = None

    for i in range(20):
        response = requests.get(
            f'{BASE_URL}/get_report?task_id={task_id}',
            headers=headers
        )
        response.raise_for_status()
        print(f'Response: {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError('Failed to retrieve report_id within timeout')

    ti.xcom_push(key='report_id', value=report_id)
    print(f'Report_id: {report_id}')

def get_increment(date, ti):
    print('Making request to get_increment')
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{BASE_URL}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers
    )
    response.raise_for_status()
    print(f'Response: {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    if not increment_id:
        raise ValueError('Increment is empty. Check API call.')

    ti.xcom_push(key='increment_id', value=increment_id)
    print(f'increment_id: {increment_id}')

def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{increment_id}/{filename}'
    print(s3_filename)

    local_filename = date.replace('-', '') + '_' + filename
    print(local_filename)

    response = requests.get(s3_filename)
    response.raise_for_status()
    with open(local_filename, "wb") as f:
        f.write(response.content)
    print('File downloaded successfully')

    df = pd.read_csv(local_filename, index_col=0)
    df = df.drop_duplicates(subset=['uniq_id'])

    if 'status' not in df.columns:
        df['status'] = 'shipped'

    postgres_hook = PostgresHook(POSTGRES_CONN_ID)
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(
        pg_table,
        engine,
        schema=pg_schema,
        if_exists='append',
        index=False
    )
    print(f'{len(df)} rows inserted into {pg_schema}.{pg_table}')


_args_owner = Variable.get("dag_owner", default_var="airflow")
_args_email = Variable.get("dag_owner_email", default_var="")
args = {
    "owner": _args_owner,
    "email": [_args_email] if _args_email else [],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

business_dt = '{{ ds }}'

with DAG(
    dag_id='sales_mart',
    default_args=args,
    description='ETL pipeline for sales data mart',
    catchup=True,
    start_date=datetime.today() - timedelta(days=7),
    end_date=datetime.today() - timedelta(days=1),
    schedule_interval='@daily',
    tags=['etl', 'sales', 'mart'],
) as dag:

    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
    )

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report,
    )

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt},
    )

    add_column_to_uol = PostgresOperator(
        task_id='add_column_to_uol',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='''
            ALTER TABLE staging.user_order_log
            ADD COLUMN IF NOT EXISTS status VARCHAR(20);
            UPDATE staging.user_order_log
            SET status = 'shipped'
            WHERE status IS NULL;
        ''',
    )

    delete_same_day_data = PostgresOperator(
        task_id='delete_same_day_data',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="migrations/delete_same_day_from_uol.sql",
    )

    upload_user_order_inc = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={
            'date': business_dt,
            'filename': 'user_order_log_inc.csv',
            'pg_table': 'user_order_log',
            'pg_schema': 'staging',
        },
    )

    dimension_tasks = []
    for dim in ['d_city', 'd_item', 'd_customer']:
        dimension_tasks.append(
            PostgresOperator(
                task_id=f'update_{dim}',
                postgres_conn_id=POSTGRES_CONN_ID,
                sql=f'migrations/mart.{dim}.sql',
                dag=dag,
            )
        )

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="migrations/mart.f_sales.sql",
        parameters={"date": business_dt},
    )

    
    (
            generate_report
            >> get_report
            >> get_increment
            >> add_column_to_uol
            >> delete_same_day_data
            >> upload_user_order_inc
            >> dimension_tasks
            >> update_f_sales
    )