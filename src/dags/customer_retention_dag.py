"""
DAG: еженедельное обновление витрины customer_retention (по понедельникам в 03:00).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator


POSTGRES_CONN_ID = Variable.get("postgres_conn_id", default_var="postgres_default")
SQL_FILE = "migrations/mart.f_customer_retention.sql"

_args_owner = Variable.get("dag_owner", default_var="airflow")
_args_email = Variable.get("dag_owner_email", default_var="")

default_args = {
    "owner": _args_owner,
    "email": [_args_email] if _args_email else [],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


with DAG(
    dag_id="customer_retention_weekly",
    default_args=default_args,
    description="Updating the customer retention showcase",
    schedule_interval="0 3 * * 1",
    start_date=datetime.today() - timedelta(weeks=6),
    catchup=True,
    tags=["customer_analytics", "weekly"],
) as dag:

    mart_update = PostgresOperator(
        task_id="mart_update",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=SQL_FILE,
    )

    mart_update
