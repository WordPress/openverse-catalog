from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from oauth2 import oauth2


dag = DAG(
    dag_id="oauth2_authorization",
    schedule_interval=None,
    description="Authorization workflow for all Oauth2 providers.",
    max_active_runs=1,
    catchup=False,
    default_args={
        "owner": "data-eng-admin",
        "depends_on_past": False,
        "start_date": datetime(2021, 1, 1),
        "email_on_retry": False,
        "retries": 0,
    },
)


with dag:
    PythonOperator(
        task_id="authorize_providers",
        python_callable=oauth2.authorize_providers,
    )
