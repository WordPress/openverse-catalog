import os
from typing import Iterable

from airflow.exceptions import AirflowException
from airflow.models import DagModel, DagRun
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.session import provide_session
from airflow.utils.state import State
from sqlalchemy import func


class ExternalDAGsSensor(BaseSensorOperator):
    """
    Waits for a list of different DAGs to not be running.

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
    def poke(self, context, session=None):
        self.log.info(
            "Poking for DAGS %s ... ",
            self.external_dag_ids,
        )

        # In poke mode this will check DAG existence only once
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

        # Success when there are no running DAGs with the provided IDs.
        return count_running == 0

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
