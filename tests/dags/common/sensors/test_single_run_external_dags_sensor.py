import logging
import unittest

import pytest
from airflow.exceptions import AirflowException
from airflow.models import DagRun, Pool, TaskInstance
from airflow.models.dag import DAG
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType
from common.sensors.single_run_external_dags_sensor import SingleRunExternalDAGsSensor


DEFAULT_DATE = datetime(2022, 1, 1)
TEST_TASK_ID = "wait_task"


@pytest.fixture(autouse=True)
def clean_db():
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()


def run_sensor(sensor):
    sensor.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)


def create_task(dag, external_dag_ids):
    return SingleRunExternalDAGsSensor(
        task_id=TEST_TASK_ID,
        external_dag_ids=[],
        check_existence=True,
        dag=dag,
        pool="test_pool",
        poke_interval=5,
        mode="reschedule",
    )


def create_dag(dag_id):
    with DAG(
        dag_id,
        default_args={
            "owner": "airflow",
            "start_date": DEFAULT_DATE,
        },
    ) as dag:
        # Create a sensor task inside the DAG
        create_task(dag, [])

    return dag


def create_dagrun(dag, dag_state):
    return dag.create_dagrun(
        run_id=f"{dag.dag_id}_test",
        start_date=DEFAULT_DATE,
        execution_date=DEFAULT_DATE,
        state=dag_state,
        run_type=DagRunType.MANUAL,
    )


class TestExternalDAGsSensor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        Pool.create_or_update_pool("test_pool", slots=1, description="test pool")

    def tearDown(self):
        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TaskInstance).delete()

    def test_fails_if_external_dag_does_not_exist(self):
        with pytest.raises(AirflowException):
            sensor = SingleRunExternalDAGsSensor(
                task_id=TEST_TASK_ID,
                external_dag_ids=[
                    "nonexistent_dag_id",
                ],
                poke_interval=5,
                mode="reschedule",
            )

            run_sensor(sensor)

    def test_fails_if_external_dag_missing_sensor_task(self):
        # Create DAG with missing sensor
        DAG(
            "unit_test_dag_without_sensor",
            default_args={
                "owner": "airflow",
                "start_date": DEFAULT_DATE,
            },
        )
        with pytest.raises(AirflowException):
            sensor = SingleRunExternalDAGsSensor(
                task_id=TEST_TASK_ID,
                external_dag_ids=[
                    "unit_test_dag_without_sensor",
                ],
                poke_interval=5,
                mode="reschedule",
            )

            run_sensor(sensor)

    def test_succeeds_if_no_running_dags(self):
        # Create some DAGs that are not considered 'running'
        successful_dag = create_dag("successful_dag")
        create_dagrun(successful_dag, State.SUCCESS)
        failed_dag = create_dag("failed_dag")
        create_dagrun(failed_dag, State.FAILED)

        # DAG in the running state, but its wait task has not been started
        queued_dag = create_dag("queued_dag")
        create_dagrun(queued_dag, State.RUNNING)

        # Create the Test DAG and sensor with dependent dag Ids
        dag = DAG(
            "test_dag_success",
            default_args={
                "owner": "airflow",
                "start_date": DEFAULT_DATE,
            },
        )
        sensor = SingleRunExternalDAGsSensor(
            task_id=TEST_TASK_ID,
            external_dag_ids=["successful_dag", "failed_dag", "queued_dag"],
            poke_interval=5,
            mode="reschedule",
            dag=dag,
            pool="test_pool",
        )

        with self.assertLogs(sensor.log, level=logging.INFO) as sensor_logs:
            run_sensor(sensor)
            assert (
                "INFO:airflow.task.operators:Poking for DAGs ['successful_dag',"
                " 'failed_dag', 'queued_dag'] ..." in sensor_logs.output
            )
            assert (
                "INFO:airflow.task.operators:0 DAGs are in the running state"
                in sensor_logs.output
            )

    def test_retries_if_running_dags_with_completed_sensor_task(self):
        # Create a DAG in the 'running' state
        running_dag = create_dag("running_dag")
        running_dagrun = create_dagrun(running_dag, State.RUNNING)

        pool = Pool.get_pool("test_pool")
        assert pool.open_slots() == 1

        # Run its sensor task and ensure that it succeeds
        ti = running_dagrun.get_task_instance(task_id=TEST_TASK_ID)
        ti.task = running_dag.get_task(task_id=TEST_TASK_ID)
        ti.run()
        assert ti.state == State.SUCCESS

        # Create a DAG that is not in the running state
        successful_dependent_dag = create_dag("success_dag")
        create_dagrun(successful_dependent_dag, State.SUCCESS)

        # Create the Test DAG and sensor and set up dependent dag Ids
        dag = DAG(
            "test_dag_failure",
            default_args={
                "owner": "airflow",
                "start_date": DEFAULT_DATE,
            },
        )
        sensor = SingleRunExternalDAGsSensor(
            task_id=TEST_TASK_ID,
            external_dag_ids=[
                "success_dag",
                "running_dag",
            ],
            poke_interval=5,
            mode="reschedule",
            dag=dag,
            pool="test_pool",
        )

        with self.assertLogs(sensor.log, level=logging.INFO) as sensor_logs:
            run_sensor(sensor)

            assert (
                "INFO:airflow.task.operators:Poking for DAGs ['success_dag',"
                " 'running_dag'] ..." in sensor_logs.output
            )
            assert (
                "INFO:airflow.task.operators:1 DAGs are in the running state"
                in sensor_logs.output
            )
