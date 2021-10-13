"""
# OAuth Provider Token Refresh

**Author**: Madison Swain-Bowden
**Created**: 2021-10-13
"""
from datetime import datetime

import oauth2
from airflow.models import DAG
from airflow.operators.python import PythonOperator


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

_current_providers = [f"- {provider.name}\n" for provider in oauth2.OAUTH_PROVIDERS]

dag.doc_md = (
    __doc__
    + f"""

Iterates through all OAuth2 providers and attempts to refresh the access token using
the refresh token stored in the `{oauth2.OAUTH2_TOKEN_KEY}` Variable. This DAG will
update the tokens stored in the Variable upon successful refresh.

**Current Providers**:
{"".join(_current_providers)}
"""
)


with dag:
    for provider in oauth2.OAUTH_PROVIDERS:
        PythonOperator(
            task_id=f"refresh__{provider.name}",
            python_callable=oauth2.refresh,
            op_kwargs={"provider": provider},
        )
