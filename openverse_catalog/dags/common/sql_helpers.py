import logging

from airflow.models import TaskInstance
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import (
    PostgresHook as UpstreamPostgresHook,
)


logger = logging.getLogger(__name__)


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
        self, default_statement_timeout: float = None, *args, **kwargs
    ) -> None:
        self.default_statement_timeout = default_statement_timeout
        super().__init__(*args, **kwargs)

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
        super().run(sql, autocommit, parameters, handler)

    @staticmethod
    def get_execution_timeout(ti: TaskInstance) -> float:
        """
        Pull execution timeout from an airflow task instance and format it for the hook.
        """
        try:
            return ti.task.execution_timeout.total_seconds()
        except AttributeError as error:
            logger.info(f"{error}, assuming no execution timeout and proceding")
            return 0.0

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

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.statement_timeout = None
        if kwargs.get("execution_timeout"):
            self.statement_timeout = kwargs["execution_timeout"].total_seconds()

    def get_db_hook(self):
        return PostgresHook(
            default_statement_timeout=self.statement_timeout,
            postgres_conn_id=self.conn_id,
            **self.hook_params,
        )
