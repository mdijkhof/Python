#

from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

with DAG(
    'MD_BUMBLEBEE_HEAVY',
    description='MD BUMBLEBEE HEAVY',
    schedule_interval='* * * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id='MD_Bumblebee_Heavy',
        bash_command='python3 ${AIRFLOW_HOME}/dags/scripts/bumblebee_heavy.py'
    )
    t1 