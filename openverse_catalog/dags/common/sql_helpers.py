import logging
from datetime import timedelta

from airflow.models import TaskInstance

# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


logger = logging.getLogger(__name__)


class TimedPostgresHook(PostgresHook):
    """
    PostgresHook that sets the database timeout on any query to match the airflow task
    execution timeout.
    """

    def __init__(self, ti: TaskInstance, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.execution_timeout = ti.task.execution_timeout

    def get_pg_timeout_sql(self) -> str:
        return f"SET statement_timeout TO '{self.execution_timeout.total_seconds()}s';"

    def run(self, sql, autocommit=False, parameters=None, handler=None):
        super().run(self.get_pg_timeout_sql() + sql, autocommit, parameters, handler)


# class TimedSQLExecuteQueryOperator(SQLExecuteQueryOperator):
#     """
#     SQL Operator that sets the database timeout on any query to match the airflow task
#     execution timeout.
#     """


def get_task_app_name(ti: TaskInstance):
    """
    Format a string to tag database queries with the task and specific run and map index
    running that db job. Use this to kill the query if needed.
    """
    app_name = (
        f"{ti.dag_id}__{ti.task_id.replace('.','_')}__"
        f"{ti.start_date.strftime('%Y%m%d')}"
    )
    if ti.key.map_index >= 0:
        app_name += f"__map{str(ti.key.map_index).zfill(4)}"
    return app_name


def get_pg_timeout_sql(ti: TaskInstance):
    execution_timeout = ti.task.execution_timeout
    if issubclass(type(execution_timeout), timedelta):
        return f"SET statement_timeout TO '{execution_timeout.total_seconds()}s';"
    elif execution_timeout is None:  # use postgres default timeout value
        return "SET statement_timeout TO '30s';"
    else:
        raise TypeError(
            f"execution_timeout is {type(execution_timeout)}, not timedelta or None."
        )


def pg_runtime_args(ti: TaskInstance):
    """
    Creates SQL statements to prepend to any Airflow SQL that identifies the job with
    a particular DAG, task, and execution of the task, and sets
    the database statement_timeout to match the airflow execution_timeout.
    ti: task_instance that is calling SQL
    """
    set_timeout = get_pg_timeout_sql(ti)
    set_app_name = f"SET application_name TO '{get_task_app_name(ti)}';"
    return f"{set_app_name} {set_timeout}"


# def pg_terminator(
#     postgres_conn_id,
#     ti: TaskInstance,
# ):
#     """
#     Kill any jobs associated with a specific airflow task and still running in the
#     postgres database.
#     """
#     task_app_name = get_task_app_name(ti)
#     # check ti.state to make sure it timed out before getting into DB stuff?
#     # or use ti.get_truncated_error_traceback to make sure it timed out specifically?
#     logger.info(
#         f"TERMINATOR: Checking for processes running under {task_app_name}."
#     )
#     pg = PostgresHook(postgres_conn_id)
#     get_pid_sql = (
#         "SELECT pid FROM pg_stat_activity "
#         + f"WHERE application_name = '{task_app_name}';"
#     )
#     db_response = pg.get_records(get_pid_sql)
#     pid_list = [p[0] for p in db_response]
#     if pid_list:
#         logger.info(f"Terminating these processes: {pid_list}")
#         for pid in pid_list:
#             pg.run(f"SELECT pg_terminate_backend({pid});")
#     else:
#         logger.info(f"No running processes found. ({db_response})")
