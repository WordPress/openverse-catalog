import logging
from datetime import timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import (
    PostgresHook as UpstreamPostgresHook,
)
from common.constants import POSTGRES_CONN_ID


logger = logging.getLogger(__name__)


# Some functions, like inaturalist, use "copy_expert" to bulk load data to a table.
# Right now, that is not using the timeout automagically.
# https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_api/airflow/providers/postgres/hooks/postgres/index.html#airflow.providers.postgres.hooks.postgres.PostgresHook.copy_expert # noqa


class PostgresHook(UpstreamPostgresHook):
    """
    PostgresHook that sets the database timeout on any query to match the airflow task
    execution timeout or a specific timeout for a particular run.

    default_statement_timeout: number of seconds postgres should wait before canceling
        the query (note: can override this by passing statement_timeout to a the `run`
        method, but other methods like `get_records` which rely on `run` are not
        are not set up to pass an override timeout through to `run`. Not clear that it
        always works to have a `statement_timeout` that is longer than the airflow task
        `execution_timeout`.)
    see airflow.providers.postgres.hooks.postgres.PostgresHook for more on other params
    """

    def __init__(
        self,
        postgres_conn_id: str = POSTGRES_CONN_ID,
        default_statement_timeout: float = None,
        *args,
        **kwargs,
    ) -> None:
        self.default_statement_timeout = default_statement_timeout
        self.postgres_conn_id = postgres_conn_id
        if postgres_conn_id is None:
            self.postgres_conn_id = POSTGRES_CONN_ID
        super().__init__(self.postgres_conn_id, *args, **kwargs)

    def run(
        self,
        sql,
        statement_timeout: float = None,
        autocommit=False,
        parameters=None,
        handler=None,
        *args,
        **kwargs,
    ):
        statement_timeout = statement_timeout or self.default_statement_timeout

        if statement_timeout:
            sql = f"{self.get_pg_timeout_sql(statement_timeout)} {sql}"
        return super().run(sql, autocommit, parameters, handler)

    @staticmethod
    def get_execution_timeout(task) -> float:
        """
        Pull execution timeout from airflow task and format it for the hook, i.e.
        number of seconds. Use the task execution timeout, if available. If not, take
        the DAG execution timeout, if that's not available, return 0 for no timeout.
        """
        zero = timedelta(seconds=0)
        timeout = task._BaseOperator__init_kwargs.get(
            "execution_timeout", getattr(task.dag, "execution_timeout", zero)
        ).total_seconds()
        return timeout

    @staticmethod
    def get_pg_timeout_sql(statement_timeout: float) -> str:
        return f"SET statement_timeout TO '{statement_timeout}s';"


class PGExecuteQueryOperator(SQLExecuteQueryOperator):
    """
    SQL Operator that sets the database statement timeout to match the airflow task
    execution timeout. execution_timeout is a standard Airflow parameter that should be
    in the form of `datetime.timedelta`. See SQLExecuteQueryOperator for more on other
    params.
    """

    def __init__(
        self,
        *,
        postgres_conn_id: str = None,
        execution_timeout: timedelta = None,
        **kwargs,
    ) -> None:
        self.statement_timeout = None
        if execution_timeout:
            self.statement_timeout = execution_timeout.total_seconds()
        self.postgres_conn_id = postgres_conn_id or kwargs.get("conn_id")
        super().__init__(**kwargs)

    def get_db_hook(self):
        return PostgresHook(
            default_statement_timeout=self.statement_timeout,
            postgres_conn_id=self.postgres_conn_id,
            **self.hook_params,
        )
