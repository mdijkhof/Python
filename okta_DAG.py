#

from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

with DAG(
    'MD_TEST',
    description='MD TEST',
    schedule_interval='0 13 * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id='MD_Test',
        bash_command='python3 ${AIRFLOW_HOME}/dags/scripts/okta.py'
    )
    t1 