"""
# Provider DAG Factory
This file contains two factory functions which generate the bulk of our
provider workflow DAGs. These DAGs pull data in from a particular provider,
and produce one or several TSVs of the results.

The "simple" (non-partitioned) DAG also loads the TSVs into the catalog database.

The loading step takes the media data saved locally in TSV files, cleans it using an
intermediate database table, and saves the cleaned-up data into the main database
(also called upstream or Openledger).

In production,"locally" means on AWS EC2 instance that runs the Apache Airflow
webserver. Storing too much data there is dangerous, because if ingestion to the
database breaks down, the disk of this server gets full, and breaks all
Apache Airflow operations.

As a first step, the loader portion of the DAG saves the data gathered by
Provider API Scripts to S3 before attempting to load it to PostgreSQL, and delete
it from disk if saving to S3 succeeds, even if loading to PostgreSQL fails.

This way, we can delete data from the EC2 instance to open up disk space without
the possibility of losing that data altogether. This will allow us to recover if
we lose data from the DB somehow, because it will all be living in S3.
It's also a prerequisite to the long-term plan of saving data only to S3
(since saving it to the EC2 disk is a source of concern in the first place).

This is one step along the path to avoiding saving data on the local disk at all.
It should also be faster to load into the DB from S3, since AWS RDS instances
provide special optimized functionality to load data from S3 into tables in the DB.

Loading the data into the Database is a two-step process: first, data is saved
to the intermediate table. Any items that don't have the required fields
(media url, license, foreign landing url and foreign id), and duplicates as
determined by combination of provider and foreign_id are deleted.
Then the data from the intermediate table is upserted into the main database.
If the same item is already present in the database, we update its information
with newest (non-null) data, and merge any metadata or tags objects to preserve all
previously downloaded data, and update any data that needs updating
(eg. popularity metrics).

Provider workflows which extend the ProviderDataIngester class support a few DagRun
configuration variables:

* `skip_ingestion_errors`: When set to true, errors encountered during ingestion will
be caught to allow ingestion to continue. The `pull_data` task will still fail when
ingestion is complete, and report a summary of all encountered errors. By default
`skip_ingestion_errors` is False.
* `initial_query_params`: An optional dict of query parameters with which to begin
ingestion. This allows a user to manually force ingestion to resume from a particular
batch, for example when retrying after an error.

You can find more background information on the loading process in the following
issues and related PRs:

- [[Feature] More sophisticated merging of columns in PostgreSQL when upserting](
https://github.com/creativecommons/cccatalog/issues/378)

- [DB Loader DAG should write to S3 as well as PostgreSQL](
https://github.com/creativecommons/cccatalog/issues/333)

- [DB Loader should take data from S3, rather than EC2 to load into PostgreSQL](
https://github.com/creativecommons/cccatalog/issues/334)
"""
import logging
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from common.constants import DAG_DEFAULT_ARGS, XCOM_PULL_TEMPLATE
from common.loader import loader, reporting, s3, sql
from providers.factory_utils import (
    date_partition_for_prefix,
    generate_tsv_filenames,
    pull_media_wrapper,
)
from providers.provider_reingestion_workflows import ProviderReingestionWorkflow
from providers.provider_workflows import ProviderWorkflow


logger = logging.getLogger(__name__)


DB_CONN_ID = os.getenv("OPENLEDGER_CONN_ID", "postgres_openledger_testing")
AWS_CONN_ID = os.getenv("AWS_CONN_ID", "no_aws_conn_id")
OPENVERSE_BUCKET = os.getenv("OPENVERSE_BUCKET")
OUTPUT_DIR_PATH = os.path.realpath(os.getenv("OUTPUT_DIR", "/tmp/"))
DATE_RANGE_ARG_TEMPLATE = "{{{{ macros.ds_add(ds, -{}) }}}}"
DATE_PARTITION_ARG_TEMPLATE = "{media_type}/{provider_name}/{{{{ date_partition_for_prefix(dag.schedule_interval, dag_run.logical_date) }}}}"  # noqa


def create_ingestion_workflow(conf: ProviderWorkflow, day_shift: int = 0):
    """
    Creates a TaskGroup that performs the ingestion tasks, first pulling and then
    loading data. Returns the TaskGroup, and a dictionary of reporting metrics.

    Required Arguments:

    conf: ProviderWorkflow configuration object.

    Optional Arguments:

    day_shift: integer giving the number of days before the current execution date
               for which ingestion should run (if `conf.dated==True`).
    """

    def append_day_shift(id_str):
        # Appends the day_shift to an id if it is non-zero
        return f"{id_str}{f'_{day_shift}' if day_shift else ''}"

    with TaskGroup(group_id=append_day_shift("ingest_data")) as ingest_data:
        media_type_name = "mixed" if len(conf.media_types) > 1 else conf.media_types[0]
        provider_name = conf.dag_id.replace("_workflow", "")
        identifier = f"{provider_name}_{{{{ ts_nodash }}}}_{day_shift}"

        ingestion_kwargs = {
            "ingestion_callable": conf.ingestion_callable,
            "media_types": conf.media_types,
        }
        if conf.dated:
            ingestion_kwargs["args"] = [DATE_RANGE_ARG_TEMPLATE.format(day_shift)]

        generate_filenames = PythonOperator(
            task_id=append_day_shift(f"generate_{media_type_name}_filename"),
            python_callable=generate_tsv_filenames,
            op_kwargs=ingestion_kwargs,
        )

        pull_data = PythonOperator(
            task_id=append_day_shift(f"pull_{media_type_name}_data"),
            python_callable=pull_media_wrapper,
            op_kwargs={
                **ingestion_kwargs,
                # Note: this is assumed to match the order of media_types exactly
                "tsv_filenames": [
                    XCOM_PULL_TEMPLATE.format(
                        generate_filenames.task_id, f"{media_type}_tsv"
                    )
                    for media_type in conf.media_types
                ],
            },
            depends_on_past=False,
            execution_timeout=conf.pull_timeout,
            # If the data pull fails, we want to load all data that's been retrieved
            # thus far before we attempt again
            retries=0,
        )

        load_tasks = []
        record_counts_by_media_type: reporting.MediaTypeRecordMetrics = {}
        for media_type in conf.media_types:
            with TaskGroup(
                group_id=append_day_shift(f"load_{media_type}_data")
            ) as load_data:
                create_loading_table = PythonOperator(
                    task_id=append_day_shift("create_loading_table"),
                    python_callable=sql.create_loading_table,
                    op_kwargs={
                        "postgres_conn_id": DB_CONN_ID,
                        "identifier": identifier,
                        "media_type": media_type,
                    },
                    trigger_rule=TriggerRule.NONE_SKIPPED,
                    doc_md="Create a temporary loading table for "
                    f"ingesting {media_type} data from a TSV",
                )
                copy_to_s3 = PythonOperator(
                    task_id=append_day_shift("copy_to_s3"),
                    python_callable=s3.copy_file_to_s3,
                    op_kwargs={
                        "tsv_file_path": XCOM_PULL_TEMPLATE.format(
                            generate_filenames.task_id, f"{media_type}_tsv"
                        ),
                        "s3_bucket": OPENVERSE_BUCKET,
                        "s3_prefix": DATE_PARTITION_ARG_TEMPLATE.format(
                            media_type=media_type,
                            provider_name=provider_name,
                        ),
                        "aws_conn_id": AWS_CONN_ID,
                    },
                    trigger_rule=TriggerRule.NONE_SKIPPED,
                )
                load_from_s3 = PythonOperator(
                    task_id=append_day_shift("load_from_s3"),
                    execution_timeout=conf.load_timeout,
                    retries=1,
                    python_callable=loader.load_from_s3,
                    op_kwargs={
                        "bucket": OPENVERSE_BUCKET,
                        "key": XCOM_PULL_TEMPLATE.format(copy_to_s3.task_id, "s3_key"),
                        "postgres_conn_id": DB_CONN_ID,
                        "media_type": media_type,
                        "tsv_version": XCOM_PULL_TEMPLATE.format(
                            copy_to_s3.task_id, "tsv_version"
                        ),
                        "identifier": identifier,
                    },
                )
                drop_loading_table = PythonOperator(
                    task_id=append_day_shift("drop_loading_table"),
                    python_callable=sql.drop_load_table,
                    op_kwargs={
                        "postgres_conn_id": DB_CONN_ID,
                        "identifier": identifier,
                        "media_type": media_type,
                    },
                    trigger_rule=TriggerRule.ALL_DONE,
                )
                [create_loading_table, copy_to_s3] >> load_from_s3
                load_from_s3 >> drop_loading_table

                record_counts_by_media_type[media_type] = XCOM_PULL_TEMPLATE.format(
                    load_from_s3.task_id, "return_value"
                )
                load_tasks.append(load_data)

        generate_filenames >> pull_data >> load_tasks

        if conf.create_preingestion_tasks:
            preingestion_tasks = conf.create_preingestion_tasks()
            preingestion_tasks >> pull_data

        if conf.create_postingestion_tasks:
            postingestion_tasks = conf.create_postingestion_tasks()
            pull_data >> postingestion_tasks

    ingestion_metrics = {
        "duration": XCOM_PULL_TEMPLATE.format(pull_data.task_id, "duration"),
        "record_counts_by_media_type": record_counts_by_media_type,
    }

    return ingest_data, ingestion_metrics


def create_report_load_completion(
    dag_id,
    media_types,
    ingestion_metrics,
    dated,
):
    return PythonOperator(
        task_id="report_load_completion",
        python_callable=reporting.report_completion,
        op_kwargs={
            "dag_id": dag_id,
            "media_types": media_types,
            "duration": ingestion_metrics["duration"],
            "record_counts_by_media_type": ingestion_metrics[
                "record_counts_by_media_type"
            ],
            "dated": dated,
            "date_range_start": "{{ data_interval_start | ds }}",
            "date_range_end": "{{ data_interval_end | ds }}",
        },
        trigger_rule=TriggerRule.ALL_DONE,
    )


def create_provider_api_workflow_dag(conf: ProviderWorkflow):
    """
    This factory method instantiates a DAG that will run the given
    `main_function`.

    Required Arguments:

    conf: ProviderWorkflow configuration object.
    """
    default_args = {**DAG_DEFAULT_ARGS, **(conf.default_args or {})}

    dag = DAG(
        dag_id=conf.dag_id,
        default_args={**default_args, "start_date": conf.start_date},
        max_active_tasks=conf.max_active_tasks,
        max_active_runs=conf.max_active_runs,
        start_date=conf.start_date,
        schedule_interval=conf.schedule_string,
        catchup=conf.dated,  # catchup is turned on for dated DAGs to allow backfilling
        doc_md=conf.doc_md,
        tags=["provider"]
        + [f"provider: {media_type}" for media_type in conf.media_types],
        render_template_as_native_obj=True,
        user_defined_macros={"date_partition_for_prefix": date_partition_for_prefix},
    )

    with dag:
        ingest_data, ingestion_metrics = create_ingestion_workflow(conf)

        report_load_completion = create_report_load_completion(
            conf.dag_id, conf.media_types, ingestion_metrics, conf.dated
        )

        ingest_data >> report_load_completion

    return dag


def _build_partitioned_ingest_workflows(
    partitioned_reingestion_days: list[list[int]], conf: ProviderReingestionWorkflow
):
    """
    Builds a list of lists of ingestion tasks, parameterized by the given
    dag conf and a list of day shifts. Calculation is explained below.

    Required Arguments:

    conf:                          ProviderReingestionWorkflow configuration
                                   object used to configure the ingestion tasks.
    partitioned_reingestion_days:  list of lists of integers. It gives the
                                   set of days before the current execution
                                   date of the DAG for which the
                                   `main_function` should be run, and
                                   describes how the calls to the function
                                   should be prioritized.

    Calculation of ingestion dates:

    The `partitioned_reingestion_days` should have the form
        [
            [int, ..., int],
            [int, ..., int],
            ...,
            [int, ..., int]
        ]
    It's not necessary for the inner lists to be the same length. The
    task groups instantiated by this factory method will first run
    ingestion for the current execution_date, then for the current
    date minus the number of days given by integers in the first list
    (in an arbitrary order, and possibly in parallel if so configured),
    then for the dates calculated from the second list, and so on.  For
    example, given the `partitioned_reingestion_days`
        [
            [1, 2, 3],
            [8, 13, 18],
            [28, 38, 48]
        ],
    and assuming the current execution date is 2020-01-01, the
    instantiated dag will run the `main_function` with the parameters
        [
            ['2020-01-01'],
            ['2019-12-31', 2019-12-30', '2019-12-29'],
            ['2019-12-24', 2019-12-19', '2019-12-14'],
            ['2019-12-04', 2019-11-24', '2019-11-14']
        ].
    The order of the inner lists gives the order in which sets of dates
    may be run.  The order within the inner lists is not relevant.  The
    size of the inner lists does *not* set the number of simultaneous
    executions of the `main_function` allowed; that is set by the
    `max_active_tasks` parameter.
    """
    if partitioned_reingestion_days[0] != [0]:
        partitioned_reingestion_days = [[0]] + partitioned_reingestion_days

    partitioned_workflows = []
    duration_list = []
    record_counts_by_media_type_list = []

    for partition in partitioned_reingestion_days:
        workflow_list = []
        for day_shift in partition:
            ingest_data, ingestion_metrics = create_ingestion_workflow(conf, day_shift)
            workflow_list.append(ingest_data)
            duration_list.append(ingestion_metrics["duration"])
            record_counts_by_media_type_list.append(
                ingestion_metrics["record_counts_by_media_type"]
            )

        partitioned_workflows.append(workflow_list)

    total_ingestion_metrics = {
        "duration": duration_list,
        "record_counts_by_media_type": record_counts_by_media_type_list,
    }

    return partitioned_workflows, total_ingestion_metrics


def create_day_partitioned_reingestion_dag(
    conf: ProviderReingestionWorkflow, partitioned_reingestion_days: list[list[int]]
):
    """
    Given a `conf` object and `reingestion_day_list_list`, this
    factory method instantiates a DAG that will run ingestion using the
    given configuration, parameterized by a number of dates calculated
    using the reingestion day list.

    Required Arguments:

    conf:                       ProviderReingestionWorkflow configuration
                                object used to configure the ingestion tasks.
    reingestion_day_list_list:  list of lists of integers. It gives the
                                set of days before the current execution
                                date of the DAG for which the
                                `main_function` should be run, and
                                describes how the calls to the function
                                should be prioritized.
    """
    default_args = {**DAG_DEFAULT_ARGS, **(conf.default_args or {})}
    dag = DAG(
        dag_id=conf.dag_id,
        default_args={**default_args, "start_date": conf.start_date},
        max_active_tasks=conf.max_active_tasks,
        max_active_runs=conf.max_active_runs,
        dagrun_timeout=conf.dagrun_timeout,
        schedule_interval=conf.schedule_string,
        start_date=conf.start_date,
        catchup=False,
        doc_md=conf.doc_md,
        tags=["provider-reingestion"]
        + [f"provider-reingestion: {media_type}" for media_type in conf.media_types],
        render_template_as_native_obj=True,
        user_defined_macros={"date_partition_for_prefix": date_partition_for_prefix},
    )
    with dag:
        # Generate a list of lists of ingestion TaskGroups for each day of reingestion.
        (
            partitioned_ingest_workflows,
            ingestion_metrics,
        ) = _build_partitioned_ingest_workflows(partitioned_reingestion_days, conf)

        # For each 'level', make a gather task that waits for all of the reingestion
        # tasks at that level to complete.
        for i in range(len(partitioned_ingest_workflows) - 1):
            gather_operator = EmptyOperator(
                task_id=f"gather_L{i}", trigger_rule=TriggerRule.ALL_DONE
            )

            # Set gather task downstream of all ingestion TaskGroups in the ith list.
            partitioned_ingest_workflows[i] >> gather_operator

            # Set gather task upstream of all ingestion TaskGroups in the i+1th list.
            # This gates the tasks at each level.
            gather_operator >> partitioned_ingest_workflows[i + 1]

        # Create a single report_load_completion task, passing in the list of duration
        # and counts data for each completed task.
        report_load_completion = create_report_load_completion(
            conf.dag_id, conf.media_types, ingestion_metrics, conf.dated
        )

        # report_load_completion is downstream of all the ingestion TaskGroups in the
        # final list.
        partitioned_ingest_workflows[-1] >> report_load_completion

    return dag
