#

from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

with DAG(
    'MD_BUMBLEBEE',
    description='MD BUMBLEBEE',
    schedule_interval='10 0,2,4,6,8,10,12,14,16,18,20,22 * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id='MD_Bumblebee',
        bash_command='python3 ${AIRFLOW_HOME}/dags/scripts/bumblebee_v2.py'
    )
    t1 