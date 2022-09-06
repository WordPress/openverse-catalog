"""
This is based on a design where we tag database jobs with airflow task metadata, using
application_name, to help identify process IDs for deletion.

There are some other options we might want to explore:

- With the postgres operator (version 5.0.0 is in just shell, airflow info), we can pass
a custom db timeout with every SQL. https://airflow.apache.org/docs/apache-airflow-providers-postgres/5.0.0/operators/postgres_operator_howto_guide.html#passing-server-configuration-parameters-into-postgresoperator

- I wasn't able to recreate the issue when I used the airflow hook or operator, but that
could be my testing limitations or it could be that airflow has a solution for this and
we should just replace psycopg2 in the Python operators with the airflow postgres hook.

TO DO: If we're going to go with this design, we should see if there are additional
places to tag SQL runs for possible termination later. 
These files use airflow postgres or psycopg2, and haven't been touched yet:
- openverse_catalog/dags/common/operators/postgres_result.py
- openverse_catalog/dags/database/report_pending_reported_media.py
- openverse_catalog/dags/commoncrawl/commoncrawl_scripts/scripts/merge_cc_tags.py
"""

import logging
import os

from common import slack
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

PG_SET_NAME_TEMPLATE = "SET application_name = '{}';\n"

def get_task_app_name(task_instance_context):
    # For more information about airflow template fields, see: 
    # https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
    # TO DO: Consider including fields like ts_nodash or run_id to distinguish tasks
    # run more than once per day. Might not be worth it, since those tasks are probably
    # much faster, on average at least.
    if isinstance(task_instance_context, str):
        return task_instance_context
    elif isinstance(task_instance_context, dict):
        if task_instance_context.get("task_instance_key_str"):
            return task_instance_context.get("task_instance_key_str")
        else:
            raise IndexError("Dict must have 'task_instance_key_str' key.")
    else:
        raise TypeError("Only str or dict allowed.")

def pg_terminator(
    context:dict,
    postgres_conn_id,
):
    exception: Optional[Exception] = context.get("exception")
    task_app_name = get_task_app_name(context)
    if exception:
        logger.info(f"TERMINATOR: Checking for processes running under {task_app_name}.")
        pg = PostgresHook(postgres_conn_id)
        get_pid_sql = f"SELECT pid FROM pg_stat_activity WHERE application_name = '{task_app_name}';"
        db_response = pg.get_records(get_pid_sql)
        pid_list = [p[0] for p in db_response]
        if pid_list:
            logger.info(f"Terminating these processes: {pid_list}")
            for pid in pid_list:
                pg.run(f"SELECT pg_terminate_backend({pid});")
        else:
            logger.info(f"No running processes found. ({db_response})")
    else:
        logger.info(f"TERMINATOR: No exception, {context=}")


def integrated(context: dict):
    """ pulls together different tasks for when a job fails """
    if context.get('db_connection'):
        pg_terminator(context)
    slack.on_failure_callback(context)
