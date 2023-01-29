import logging
from datetime import timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook


logger = logging.getLogger(__name__)


def get_task_app_name(context):
    """
    Format a string to tag database queries with the task and specific run and map index
    running that db job. Use this to kill the query if needed.
    """
    if isinstance(context, dict):
        ti = context["task_instance"]
    else:
        ti = context
    app_name = (
        f"{ti.dag_id}__{ti.task_id.replace('.','_')}__"
        f"{ti.start_date.strftime('%Y%m%d')}"
    )
    if ti.key.map_index >= 0:
        app_name += f"__map{str(ti.key.map_index).zfill(4)}"
    return app_name


def get_pg_timeout_sql(context):
    if isinstance(context, dict) and "ti" in context:
        execution_timeout = context["ti"].task.execution_timeout
    elif isinstance(context, dict) and "task_instance" in context:
        execution_timeout = context["task_instance"].task.execution_timeout
    else:
        execution_timeout = context.task.execution_timeout
    if issubclass(type(execution_timeout), timedelta):
        return f"SET statement_timeout TO '{execution_timeout.total_seconds()}s';"
    elif execution_timeout is None:  # use postgres default timeout value
        return "SET statement_timeout TO '30s';"
    else:
        raise TypeError(
            f"execution_timeout is {type(execution_timeout)}, not timedelta or None."
        )


def pg_runtime_args(context: dict):
    """
    Creates SQL statements to prepend to any Airflow SQL that identifies the job with
    a particular DAG, task, and execution of the task, and sets
    the database statement_timeout to match the airflow execution_timeout.
    ti: task_instance that is calling SQL
    """
    set_timeout = get_pg_timeout_sql(context)
    set_app_name = f"SET application_name TO '{get_task_app_name(context)}';"
    return f"{set_app_name} {set_timeout}"


def pg_terminator(
    context: dict,
    postgres_conn_id,
):
    """
    Kill any jobs associated with a specific airflow task and still running in the
    postgres database.
    If needed we could integrate this with the slack calls to be the on-failure
    callback. But I really don't think we need it.
    """
    exception = context.get("exception")
    task_app_name = get_task_app_name(context)
    if exception:
        logger.info(
            f"TERMINATOR: Checking for processes running under {task_app_name}."
        )
        pg = PostgresHook(postgres_conn_id)
        get_pid_sql = (
            "SELECT pid FROM pg_stat_activity "
            + f"WHERE application_name = '{task_app_name}';"
        )
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
