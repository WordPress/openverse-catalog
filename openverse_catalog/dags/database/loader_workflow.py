"""\
#### Database Loader DAG
**DB Loader Apache Airflow DAG** (directed acyclic graph) takes the media data saved
locally in TSV files, cleans it using an intermediate database table, and saves
the cleaned-up data into the main database (also called upstream or Openledger).

In production,"locally" means on AWS EC2 instance that runs the Apache Airflow
webserver. Storing too much data there is dangerous, because if ingestion to the
database breaks down, the disk of this server gets full, and breaks all
Apache Airflow operations.

As a first step, the DB Loader Apache Airflow DAG saves the data gathered by
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

You can find more background information on the loading process in the following
issues and related PRs:

- [[Feature] More sophisticated merging of columns in PostgreSQL when upserting](
https://github.com/creativecommons/cccatalog/issues/378)

- [DB Loader DAG should write to S3 as well as PostgreSQL](
https://github.com/creativecommons/cccatalog/issues/333)

- [DB Loader should take data from S3, rather than EC2 to load into PostgreSQL](
https://github.com/creativecommons/cccatalog/issues/334)

"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from common.loader import loader, paths, sql


DAG_ID = "tsv_to_postgres_loader"
DB_CONN_ID = os.getenv("OPENLEDGER_CONN_ID", "postgres_openledger_testing")
AWS_CONN_ID = os.getenv("AWS_CONN_ID", "no_aws_conn_id")
OPENVERSE_BUCKET = os.getenv("OPENVERSE_BUCKET")
MINIMUM_FILE_AGE_MINUTES = int(os.getenv("LOADER_FILE_AGE", 15))
CONCURRENCY = 5
TIMESTAMP_TEMPLATE = "{{ ts_nodash }}"

OUTPUT_DIR_PATH = os.path.realpath(os.getenv("OUTPUT_DIR", "/tmp/"))


dag = DAG(
    dag_id=DAG_ID,
    default_args={
        "owner": "data-eng-admin",
        "depends_on_past": False,
        "start_date": datetime(2020, 1, 15),
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(seconds=15),
    },
    concurrency=CONCURRENCY,
    max_active_runs=1,
    schedule_interval="* * * * *",
    catchup=False,
    doc_md=__doc__,
)

with dag:
    stage_oldest_tsv_file = PythonOperator(
        task_id="stage_oldest_tsv_file",
        python_callable=paths.stage_oldest_tsv_file,
        op_kwargs={
            "output_dir": OUTPUT_DIR_PATH,
            "identifier": TIMESTAMP_TEMPLATE,
            "minimum_file_age_minutes": MINIMUM_FILE_AGE_MINUTES,
        },
    )
    create_loading_table = PythonOperator(
        task_id="create_loading_table",
        python_callable=sql.create_loading_table,
        op_kwargs={
            "postgres_conn_id": DB_CONN_ID,
            "identifier": TIMESTAMP_TEMPLATE,
        },
    )
    copy_to_s3 = PythonOperator(
        task_id="copy_to_s3",
        python_callable=loader.copy_to_s3,
        op_kwargs={
            "output_dir": OUTPUT_DIR_PATH,
            "storage_bucket": OPENVERSE_BUCKET,
            "aws_conn_id": AWS_CONN_ID,
            "identifier": TIMESTAMP_TEMPLATE,
        },
    )
    load_s3_data = PythonOperator(
        task_id="load_s3_data",
        python_callable=loader.load_s3_data,
        op_kwargs={
            "bucket": OPENVERSE_BUCKET,
            "aws_conn_id": AWS_CONN_ID,
            "postgres_conn_id": DB_CONN_ID,
            "identifier": TIMESTAMP_TEMPLATE,
        },
    )
    delete_staged_file = PythonOperator(
        task_id="delete_staged_file",
        python_callable=paths.delete_staged_file,
        op_kwargs={"output_dir": OUTPUT_DIR_PATH, "identifier": TIMESTAMP_TEMPLATE},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    drop_loading_table = PythonOperator(
        task_id="drop_loading_table",
        python_callable=sql.drop_load_table,
        op_kwargs={"postgres_conn_id": DB_CONN_ID, "identifier": TIMESTAMP_TEMPLATE},
        trigger_rule=TriggerRule.NONE_SKIPPED,
    )
    move_staged_failures = PythonOperator(
        task_id="move_staged_failures",
        python_callable=paths.move_staged_files_to_failure_directory,
        op_kwargs={
            "output_dir": OUTPUT_DIR_PATH,
            "identifier": TIMESTAMP_TEMPLATE,
        },
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # If there is a TSV to load, copy it to S3 & create the loading table,
    # then perform the load into Postgres from S3
    stage_oldest_tsv_file >> [copy_to_s3, create_loading_table] >> load_s3_data
    # Should any of these tasks fail, move the TSV to the failed location
    [copy_to_s3, create_loading_table, load_s3_data] >> move_staged_failures
    # Once the data is loaded successfully, drop the loading table and delete
    # the staged TSV locally (since it exists in S3)
    load_s3_data >> [drop_loading_table, delete_staged_file]
