import os
from datetime import timedelta
from typing import Iterable

from airflow.exceptions import AirflowException
from airflow.models import DagModel, DagRun
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.utils.session import provide_session
from airflow.utils.state import State
from sqlalchemy import func


class ExternalDAGsSensorAsync(BaseSensorOperator):
    """
    Waits for a list of different DAGs to not be running, freeing up
    a worker slot while it's waiting.

    external_dag_ids: A list of dag_ids that you want to wait for
    :param check_existence: Set to `True` to check if the external task exists (when
        external_task_id is not None) or check if the DAG to wait for exists (when
        external_task_id is None), and immediately cease waiting if the external task
        or DAG does not exist (default value: False).
    """

    # TODO: Not sure if this is necessary
    template_fields = ["external_dag_ids"]

    # TODO: I don't understand what operator extra links are, and whether it's
    # necessary to implement this.
    # @property
    # def operator_extra_links(self):
    #     """Return operator extra links"""
    #     return [ExternalDAGsSensorLink()]

    def __init__(
        self,
        *,
        external_dag_ids: Iterable[str],
        check_existence: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.external_dag_ids = external_dag_ids
        self.check_existence = check_existence
        self._has_checked_existence = False

    @provide_session
    def execute(self, context, event=None, session=None):
        self.log.info(
            "Poking for DAGS %s ... ",
            self.external_dag_ids,
        )

        # Check DAG existence only once, on the first execution.
        if self.check_existence and not self._has_checked_existence:
            self._check_for_existence(session=session)

        count_running = (
            session.query(func.count())
            .filter(
                DagRun.dag_id.in_(self.external_dag_ids),
                DagRun.state == State.RUNNING,
            )
            .scalar()
        )

        # If there are running DAGs, trigger deferral of the Sensor so that
        # the worker slot will be freed and the pool does not become
        # deadlocked waiting on this task.
        if count_running == 0:
            self.defer(
                trigger=TimeDeltaTrigger(
                    timedelta(minutes=5)
                ),  # TODO what's a good delta
                method_name="execute",
            )

    def _check_for_existence(self, session) -> None:
        for dag_id in self.external_dag_ids:
            dag_to_wait = (
                session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
            )

            if not dag_to_wait:
                raise AirflowException(f"The external DAG {dag_id} does not exist.")

            if not os.path.exists(dag_to_wait.fileloc):
                raise AirflowException(f"The external DAG {dag_id} was deleted.")
        self._has_checked_existence = True
