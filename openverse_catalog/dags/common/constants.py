import os
from datetime import datetime, timedelta
from typing import Literal

from common import on_failure_callback


AUDIO = "audio"
IMAGE = "image"
MEDIA_TYPES = [AUDIO, IMAGE]

MediaType = Literal["audio", "image"]

CONTACT_EMAIL = os.getenv("CONTACT_EMAIL")

DAG_DEFAULT_ARGS = {
    "owner": "data-eng-admin",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 15),
    # Failure emails are on by default but the Slack failure callback will be used in
    # place of them if the DAG default args are used.
    "email_on_failure": False,
    "email_on_retry": False,
    "email": [CONTACT_EMAIL],
    "retries": int(os.getenv("DEFAULT_RETRY_COUNT", 2)),
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": on_failure_callback.integrated,
}
XCOM_PULL_TEMPLATE = "{{{{ ti.xcom_pull(task_ids='{}', key='{}') }}}}"

POSTGRES_CONN_ID = os.getenv("OPENLEDGER_CONN_ID", "postgres_openledger_testing")
OPENLEDGER_API_CONN_ID = os.getenv("OPENLEDGER_API_CONN_ID", "postgres_openledger_api")
