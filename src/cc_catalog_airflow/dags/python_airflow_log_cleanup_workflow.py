"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the task logs to avoid those getting too big. By default, this will also
clean child process logs from the 'scheduler' directory.

Can remove all log files by setting "maxLogAgeInDays" to -1.

airflow dags trigger --conf '{"maxLogAgeInDays":-1, "enableDelete": "false"}' airflow_log_cleanup
--conf options:
    maxLogAgeInDays:<INT> - Optional
{"enableDelete": "false", "maxLogAgeInDays": -1}
"""
import logging
from datetime import timedelta, datetime

import jinja2
from airflow.configuration import conf
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import util.operator_util as ops
from util import log_cleanup

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.INFO
)

logger = logging.getLogger(__name__)

DAG_ID = 'python_airflow_log_cleanup'
BASE_LOG_FOLDER = conf.get("logging", "BASE_LOG_FOLDER").rstrip("/")
# Whether the job should delete the logs or not. Included if you want to
# temporarily avoid deleting the logs
DEFAULT_MAX_LOG_AGE_IN_DAYS = 7
ENABLE_DELETE = True
CONCURRENCY = 1
# should we send someone an email when this DAG fails?
ALERT_EMAIL_ADDRESSES = ''
DAG_DEFAULT_ARGS = {
    'owner': 'data-eng-admin',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 15),
    'template_undefined': jinja2.Undefined,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}


def get_log_cleaner_operator(
        dag,
        base_log_folder,
):
    return PythonOperator(
        task_id='log_cleaner_operator',
        python_callable=log_cleanup.clean_up,
        op_args=[
            base_log_folder,
            "{{ params.get('maxLogAgeInDays') }}",
            "{{ params.get('enableDelete') }}"
        ],
        dag=dag
    )


def create_dag(
        dag_id=DAG_ID,
        args=DAG_DEFAULT_ARGS,
        concurrency=CONCURRENCY,
        max_active_runs=CONCURRENCY,
):
    dag = DAG(
        dag_id=dag_id,
        default_args=args,
        concurrency=concurrency,
        max_active_runs=max_active_runs,
        # If this was True, airflow would run this DAG in the beginning
        # for each day from the start day to now
        catchup=False,
        # Use the docstring at the top of the file as md docs in the UI
        doc_md=__doc__,
    )

    with dag:
        start_task = BashOperator(
            task_id=f'{dag.dag_id}_Starting',
            bash_command=f"echo Starting {dag.dag_id} workflow",
        )
        run_task = get_log_cleaner_operator(
            dag,
            BASE_LOG_FOLDER,
        )
        end_task = ops.get_log_operator(dag, dag.dag_id, 'Finished')

        start_task >> run_task >> end_task

    return dag


globals()[DAG_ID] = create_dag()
