"""
# Data Refresh DAG Factory
This file generates our data refresh DAGs using a factory function.
These DAGs initiate a data refresh for a given media type and awaits the
success or failure of the refresh. Importantly, they are also configured to
ensure that no two data refresh DAGs can run concurrently, as required by
the server.

A data refresh occurs on the data refresh server in the openverse-api project.
This is a task which imports data from the upstream Catalog database into the
API, copies contents to a new Elasticsearch index, and makes the index "live".
This process is necessary to make new content added to the Catalog by our
provider DAGs available on the frontend. You can read more in the [README](
https://github.com/WordPress/openverse-api/blob/main/ingestion_server/README.md
)

The DAGs generated by this factory allow us to schedule those refreshes through
Airflow. Since no two refreshes can run simultaneously, all tasks are run in a
special `data_refresh` pool with a single worker slot. To ensure that tasks
run in an acceptable order (ie the trigger step for one DAG cannot run if a
previously triggered refresh is still running), each DAG has the following
steps:

1. The `wait_for_data_refresh` step uses a custom Sensor that will wait until
none of the `external_dag_ids` (corresponding to the other data refresh DAGs)
are 'running'. A DAG is considered to be 'running' if it is itself in the
RUNNING state __and__ its own `wait_for_data_refresh` step has completed
successfully. The Sensor suspends itself and frees up the worker slot if
another data refresh DAG is running.

2. The `trigger_data_refresh` step then triggers the data refresh by POSTing
to the `/task` endpoint on the data refresh server with relevant data. A
successful response will include the `status_check` url used to check on the
status of the refresh, which is passed on to the next task via XCom.

3. Finally the `wait_for_data_refresh` task waits for the data refresh to be
complete by polling the `status_url`. Note this task does not need to be
able to suspend itself and free the worker slot, because we want to lock the
entire pool on waiting for a particular data refresh to run.

You can find more background information on this process in the following
issues and related PRs:

- [[Feature] Data refresh orchestration DAG](
https://github.com/WordPress/openverse-catalog/issues/353)
"""
import json
import logging
import os
from datetime import datetime
from typing import Sequence
from urllib.parse import urlparse

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.dagrun import DagRun
from airflow.operators.python import BranchPythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.utils.task_group import TaskGroup
from common.constants import DAG_DEFAULT_ARGS, XCOM_PULL_TEMPLATE
from common.popularity import operators
from common.sensors.single_run_external_dags_sensor import SingleRunExternalDAGsSensor
from data_refresh.data_refresh_types import DATA_REFRESH_CONFIGS, DataRefresh
from requests import Response


logger = logging.getLogger(__name__)


DATA_REFRESH_POOL = "data_refresh"


def response_filter_data_refresh(response: Response) -> str:
    """
    Filter for the `trigger_data_refresh` task, used to grab the endpoint needed
    to poll for the status of the triggered data refresh. This information will
    then be available via XCom in the downstream tasks.
    """
    status_check_url = response.json()["status_check"]
    return urlparse(status_check_url).path


def response_check_wait_for_completion(response: Response) -> bool:
    """
    Response check to the `wait_for_completion` Sensor. Processes the response to
    determine whether the task can complete.
    """
    data = response.json()

    if data["active"]:
        # The data refresh is still running. Poll again later.
        return False

    if data["error"]:
        raise AirflowException("Error triggering data refresh.")

    logger.info(
        f"Data refresh done with {data['percent_completed']}% \
        completed."
    )
    return True


@provide_session
def _month_check(dag_id: str, media_type: str, session=None) -> str:
    """
    Checks whether there has been a previous DagRun this month. If so,
    returns the task_id for the matview refresh task; else, returns the
    task_id for the full popularity data refresh.
    """
    refresh_all_popularity_data_group_id = f"refresh_all_{media_type}_popularity_data.update_{media_type}_popularity_metrics_table"  # noqa: E501
    refresh_view_id = f"update_{media_type}_view"

    # Get the most recent dagrun for this Dag, excluding the currently
    # running dagrun
    DR = DagRun
    query = session.query(DR).filter(DR.dag_id == dag_id)
    query = query.filter(DR.state != State.RUNNING)
    query = query.order_by(DR.start_date.desc())
    latest_dagrun = query.first()

    today_date = datetime.now()
    last_dagrun_date = latest_dagrun.start_date

    # Check if the last dagrun was in the same month as the current run
    is_last_dagrun_in_current_month = (
        today_date.month == last_dagrun_date.month
        and today_date.year == last_dagrun_date.year
    )

    return (
        refresh_all_popularity_data_group_id
        if not is_last_dagrun_in_current_month
        else refresh_view_id
    )


def create_data_refresh_dag(data_refresh: DataRefresh, external_dag_ids: Sequence[str]):
    """
    This factory method instantiates a DAG that will run the data refresh for
    the given `media_type`.

    A data refresh runs for a given media type in the API DB. It imports the
    data for that type from the upstream DB in the Catalog, reindexes the data,
    and updates and reindex Elasticsearch.

    A data refresh can only be performed for one media type at a time, so the DAG
    must also use a Sensor to make sure that no two data refresh tasks run
    concurrently.

    It is intended that the data_refresh tasks, or at least the initial
    `wait_for_data_refresh` tasks, should be run in a custom pool with 1 worker
    slot. This enforces that no two `wait_for_data_refresh` tasks can start
    concurrently and enter a race condition.

    Required Arguments:

    data_refresh:     dataclass containing configuration information for the
                      DAG
    external_dag_ids: list of ids of the other data refresh DAGs. This DAG
                      will not run concurrently with any dependent DAG.
    """
    default_args = {
        **DAG_DEFAULT_ARGS,
        **data_refresh.default_args,
        "pool": DATA_REFRESH_POOL,
    }

    media_type = data_refresh.media_type

    poke_interval = int(os.getenv("DATA_REFRESH_POKE_INTERVAL", 60 * 15))
    dag = DAG(
        dag_id=data_refresh.dag_id,
        default_args=default_args,
        start_date=data_refresh.start_date,
        schedule_interval=data_refresh.schedule_interval,
        max_active_runs=1,
        catchup=False,
        doc_md=__doc__,
        tags=["data_refresh"],
    )

    with dag:
        month_check = BranchPythonOperator(
            task_id="month_check",
            python_callable=_month_check,
            op_kwargs={"dag_id": data_refresh.dag_id, "media_type": media_type},
            retries=0,
        )

        with TaskGroup(
            group_id=f"refresh_all_{media_type}_popularity_data"
        ) as refresh_all_popularity_data:
            postgres_conn_id = os.getenv(
                "OPENLEDGER_CONN_ID", "postgres_openledger_testing"
            )

            update_metrics = operators.update_media_popularity_metrics(
                postgres_conn_id,
                media_type=data_refresh.media_type,
            )
            update_constants = operators.update_media_popularity_constants(
                postgres_conn_id,
                media_type=data_refresh.media_type,
            )

            (update_metrics >> update_constants)

        update_matview = operators.update_db_view(
            postgres_conn_id, media_type=data_refresh.media_type
        )

        with TaskGroup(group_id="data_refresh") as data_refresh_group:
            # Wait to ensure that no other Data Refresh DAGs are running.
            wait_for_data_refresh = SingleRunExternalDAGsSensor(
                task_id="wait_for_data_refresh",
                external_dag_ids=external_dag_ids,
                check_existence=True,
                dag=dag,
                poke_interval=poke_interval,
                mode="reschedule",
                trigger_rule="none_failed_min_one_success",
            )

            data_refresh_post_data = {
                "model": data_refresh.media_type,
                "action": "INGEST_UPSTREAM",
            }

            # Trigger the refresh on the data refresh server.
            trigger_data_refresh = SimpleHttpOperator(
                task_id="trigger_data_refresh",
                http_conn_id="data_refresh",
                endpoint="task",
                method="POST",
                headers={"Content-Type": "application/json"},
                data=json.dumps(data_refresh_post_data),
                response_check=lambda response: response.status_code == 202,
                response_filter=response_filter_data_refresh,
                dag=dag,
            )

            # Wait for the data refresh to complete.
            wait_for_completion = HttpSensor(
                task_id="wait_for_completion",
                http_conn_id="data_refresh",
                endpoint=XCOM_PULL_TEMPLATE.format(
                    trigger_data_refresh.task_id, "return_value"
                ),
                method="GET",
                response_check=response_check_wait_for_completion,
                dag=dag,
                mode="reschedule",
                poke_interval=poke_interval,
                timeout=data_refresh.data_refresh_timeout,
            )

            wait_for_data_refresh >> trigger_data_refresh >> wait_for_completion

        month_check >> [refresh_all_popularity_data, update_matview]
        refresh_all_popularity_data >> update_matview >> data_refresh_group

    return dag


all_data_refresh_dag_ids = {refresh.dag_id for refresh in DATA_REFRESH_CONFIGS}

for data_refresh in DATA_REFRESH_CONFIGS:
    # Construct a set of all data refresh DAG ids other than the current DAG
    other_dag_ids = all_data_refresh_dag_ids - {data_refresh.dag_id}

    # Generate the DAG for this config, dependent on all the others
    globals()[data_refresh.dag_id] = create_data_refresh_dag(
        data_refresh, other_dag_ids
    )
