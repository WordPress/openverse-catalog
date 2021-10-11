from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from oauth2 import oauth2


dag = DAG(
    dag_id="oauth2_token_refresh",
    schedule_interval="0 */12 * * *",
    description="Refresh tokens for all Oauth2 providers",
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
    for provider in oauth2.OAUTH_PROVIDERS:
        PythonOperator(
            task_id=f"refresh__{provider.name}",
            python_callable=oauth2.refresh,
            op_kwargs={"provider": provider},
        )
