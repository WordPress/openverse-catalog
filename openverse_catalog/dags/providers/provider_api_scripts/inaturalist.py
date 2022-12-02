"""
Provider:   iNaturalist

Output:     TSV file containing the media metadata.

Notes:      [The iNaturalist API is not intended for data scraping.]
            (https://api.inaturalist.org/v1/docs/)

            [But there is a full dump intended for sharing on S3.]
            (https://github.com/inaturalist/inaturalist-open-data/tree/documentation/Metadata)

            Because these are very large normalized tables, as opposed to more document
            oriented API responses, we found that bringing the data into postgres first
            was the most effective approach. [More detail in slack here.]
            (https://wordpress.slack.com/archives/C02012JB00N/p1653145643080479?thread_ts=1653082292.714469&cid=C02012JB00N)

            We use the table structure defined [here,]
            (https://github.com/inaturalist/inaturalist-open-data/blob/main/Metadata/structure.sql)
            except for adding ancestry tags to the taxa table.
"""

import logging
import os
import zipfile
from datetime import timedelta
from pathlib import Path
from typing import Dict

import pendulum
import requests
from airflow.exceptions import AirflowNotFoundException, AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from common.constants import POSTGRES_CONN_ID, XCOM_PULL_TEMPLATE
from common.loader import provider_details, reporting, sql
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logger = logging.getLogger(__name__)

AWS_CONN_ID = os.getenv("AWS_CONN_ID", "test_conn_id")
OPENVERSE_BUCKET = os.getenv("OPENVERSE_BUCKET", "openverse-storage")
PROVIDER = provider_details.INATURALIST_DEFAULT_PROVIDER
SCRIPT_DIR = Path(__file__).parents[1] / "provider_csv_load_scripts/inaturalist"
SOURCE_FILE_NAMES = ["photos", "observations", "taxa", "observers"]
MEDIA_TYPE = "image"
LOADER_ARGS = {
    "postgres_conn_id": POSTGRES_CONN_ID,
    "identifier": "{{ ts_nodash }}",
    "media_type": MEDIA_TYPE,
}
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "/tmp/"))


class INaturalistDataIngester(ProviderDataIngester):

    providers = {"image": provider_details.INATURALIST_DEFAULT_PROVIDER}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pg = PostgresHook(POSTGRES_CONN_ID)
        self.batch_limit = 2_000_000

    def get_next_query_params(self, prev_query_params=None, **kwargs):
        if prev_query_params is None:
            return {"offset_num": 0}
        else:
            next_offset = prev_query_params["offset_num"] + self.batch_limit
            return {"offset_num": next_offset}

    def get_response_json(self, query_params: Dict):
        raise NotImplementedError("TSV files from AWS S3 processed in postgres.")

    def get_batch_data(self, response_json):
        raise NotImplementedError("TSV files from AWS S3 processed in postgres.")

    def get_record_data(self, data):
        raise NotImplementedError("TSV files from AWS S3 processed in postgres.")

    def get_media_type(self, record):
        # This provider only supports Images via S3, though they have some audio files
        # on site and in the API.
        # I'm using a constant here, which maybe should just be a class constant, but
        # it's there to work with the static methods.
        return MEDIA_TYPE

    def endpoint(self):
        raise NotImplementedError("Normalized TSV files from AWS S3 means no endpoint.")

    @staticmethod
    def load_intermediate_table(
        intermediate_table: str,
        page_length: int,
        postgres_conn_id=POSTGRES_CONN_ID,
        sql_template_file_name="transformed_table.template.sql",
    ):
        pg = PostgresHook(postgres_conn_id)
        sql_template = (SCRIPT_DIR / sql_template_file_name).read_text()
        # get the number of "pages" in the photos table
        max_id = pg.get_records("SELECT max(photo_id) FROM inaturalist.photos")[0][0]
        total_pages = int(max_id / page_length) + 1
        logger.info(f"Loading {intermediate_table}...")
        logger.info(f"Max photo_id={max_id}. Transform {page_length} ids / iteration.")
        # then cycle through the pages
        for page_start in range(0, max_id, page_length):
            page_end = page_start + page_length - 1
            page_log = f"page {int(page_start / page_length) + 1} of {total_pages}"
            logger.info(f"Starting at photo_id {page_start}, " + page_log)
            sql = sql_template.format(
                intermediate_table=intermediate_table,
                page_start=page_start,
                page_end=page_end,
            )
            total_records = pg.get_records(sql)[0][0]
            logger.info(f"Loaded {total_records} records, as of " + page_log)

    @staticmethod
    def compare_update_dates(
        last_success: pendulum.DateTime | None, s3_keys: list, aws_conn_id=AWS_CONN_ID
    ):
        # if it was never run, assume the data is new
        if last_success is None:
            return
        s3 = S3Hook(aws_conn_id=aws_conn_id)
        s3_client = s3.get_client_type()
        for key in s3_keys:
            # this will error out if the files don't exist, and bubble up as an
            # informative failure
            last_modified = s3_client.head_object(
                Bucket="inaturalist-open-data", Key=key
            )["LastModified"]
            # if any file has been updated, let's pull them all
            if last_success < last_modified:
                return
        # If no files have been updated, skip the DAG
        raise AirflowSkipException("Nothing new to ingest")

    @staticmethod
    def load_catalog_of_life_names():
        COL_URL = "https://api.checklistbank.org/dataset/9840/export.zip?format=ColDP"
        local_zip_file = "COL_archive.zip"
        name_usage_file = "NameUsage.tsv"
        vernacular_file = "VernacularName.tsv"
        # download zip file from Catalog of Life
        if (OUTPUT_DIR / local_zip_file).exists():
            logger.info(
                f"{OUTPUT_DIR}/{local_zip_file} exists, so no Catalog of Life download."
            )
        else:
            # This is a static method so that it can be used to create preingestion
            # tasks for airflow. Unfortunately, that means it does not have access to
            # the delayed requester. So, we are just using requests for now.
            with requests.get(COL_URL, stream=True) as response:
                response.raise_for_status()
                with open(OUTPUT_DIR / local_zip_file, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            logger.info(
                f"Saved Catalog of Life download: {OUTPUT_DIR}/{local_zip_file}"
            )
        # Extract specific files we need from the zip file
        if (OUTPUT_DIR / name_usage_file).exists() and (
            OUTPUT_DIR / vernacular_file
        ).exists():
            logger.info("No extract, both Catalog of Life tsv files exist.")
        else:
            with zipfile.ZipFile(OUTPUT_DIR / local_zip_file) as z:
                with open(OUTPUT_DIR / name_usage_file, "wb") as f:
                    f.write(z.read(name_usage_file))
                logger.info(f"Extracted raw file: {OUTPUT_DIR}/{name_usage_file}")
                with open(OUTPUT_DIR / vernacular_file, "wb") as f:
                    f.write(z.read(vernacular_file))
                logger.info(f"Extracted raw file: {OUTPUT_DIR}/{vernacular_file}")
        # set up for loading data
        pg = PostgresHook(POSTGRES_CONN_ID)
        COPY_SQL = (
            "COPY inaturalist.{} FROM STDIN "
            "DELIMITER E'\t' CSV HEADER QUOTE E'\b' NULL AS ''"
        )
        COUNT_SQL = "SELECT count(*) FROM inaturalist.{};"
        # upload vernacular names file to postgres
        pg.copy_expert(COPY_SQL.format("col_vernacular"), OUTPUT_DIR / vernacular_file)
        vernacular_records = pg.get_records(COUNT_SQL.format("col_vernacular"))
        if vernacular_records[0][0] == 0:
            raise AirflowNotFoundException("No Catalog of Life vernacular data loaded.")
        else:
            logger.info(
                f"Loaded {vernacular_records[0][0]} records from {vernacular_file}"
            )
        # upload name usage file to postgres
        pg.copy_expert(COPY_SQL.format("col_name_usage"), OUTPUT_DIR / name_usage_file)
        name_usage_records = pg.get_records(COUNT_SQL.format("col_name_usage"))
        if name_usage_records[0][0] == 0:
            raise AirflowNotFoundException("No Catalog of Life name usage data loaded.")
        else:
            logger.info(
                f"Loaded {name_usage_records[0][0]} records from {name_usage_file}"
            )
        # # TO DO: save source files on s3? just delete every time?
        # os.remove(OUTPUT_DIR / local_zip_file)
        # os.remove(OUTPUT_DIR / vernacular_file)
        # os.remove(OUTPUT_DIR / name_usage_file)
        return {
            "COL Name Usage Records": name_usage_records[0][0],
            "COL Vernacular Records": vernacular_records[0][0],
        }

    def create_preingestion_tasks():

        with TaskGroup(group_id="preingestion_tasks") as preingestion_tasks:

            check_for_file_updates = PythonOperator(
                task_id="check_for_file_updates",
                python_callable=INaturalistDataIngester.compare_update_dates,
                op_kwargs={
                    "last_success": "{{ prev_start_date_success }}",
                    "s3_keys": [
                        f"{file_name}.csv.gz" for file_name in SOURCE_FILE_NAMES
                    ],
                },
                doc_md="Has iNaturalist published new data to s3?",
            )

            create_inaturalist_schema = PostgresOperator(
                task_id="create_inaturalist_schema",
                postgres_conn_id=POSTGRES_CONN_ID,
                sql=(SCRIPT_DIR / "create_schema.sql").read_text(),
                doc_md="Create temporary schema and license table",
            )

            load_catalog_of_life_names = PythonOperator(
                task_id="load_catalog_of_life_names",
                python_callable=INaturalistDataIngester.load_catalog_of_life_names,
                doc_md="Load vernacular taxon names from Catalog of Life",
            )

            (
                check_for_file_updates
                >> create_inaturalist_schema
                >> load_catalog_of_life_names
            )

        return preingestion_tasks

    @staticmethod
    def create_postingestion_tasks():
        with TaskGroup(group_id="postingestion_tasks") as postingestion_tasks:
            drop_inaturalist_schema = PostgresOperator(
                task_id="drop_inaturalist_schema",
                postgres_conn_id=POSTGRES_CONN_ID,
                sql="DROP SCHEMA IF EXISTS inaturalist CASCADE",
                doc_md="Drop iNaturalist source tables and their schema",
                trigger_rule=TriggerRule.NONE_SKIPPED,
            )
            drop_loading_table = PythonOperator(
                task_id="drop_loading_table",
                python_callable=sql.drop_load_table,
                op_kwargs=LOADER_ARGS,
                doc_md="Drop the temporary (transformed) loading table",
                trigger_rule=TriggerRule.NONE_SKIPPED,
            )
            [drop_inaturalist_schema, drop_loading_table]
        return postingestion_tasks

    def create_ingestion_workflow():

        record_counts_by_media_type: reporting.MediaTypeRecordMetrics = {}

        with TaskGroup(group_id="ingest_data") as ingest_data:

            preingestion_tasks = INaturalistDataIngester.create_preingestion_tasks()

            with TaskGroup(group_id="pull_image_data") as pull_data:
                for source_name in SOURCE_FILE_NAMES:
                    PostgresOperator(
                        task_id=f"load_{source_name}",
                        postgres_conn_id=POSTGRES_CONN_ID,
                        sql=(SCRIPT_DIR / f"{source_name}.sql").read_text(),
                        doc_md=f"Load iNaturalist {source_name} from s3 to postgres",
                    ),

            with TaskGroup(group_id="load_image_data") as loader_tasks:

                # Using the existing set up, but the indexes on the temporary table
                # probably slow down the load quite a bit. TO DO: Consider adding
                # indices _after_ loading data into the table.
                create_loading_table = PythonOperator(
                    task_id="create_loading_table",
                    python_callable=sql.create_loading_table,
                    op_kwargs=LOADER_ARGS,
                    doc_md=(
                        "Create a temp table for ingesting data from inaturalist "
                        "source tables."
                    ),
                )

                load_intermediate_table = PythonOperator(
                    task_id="load_intermediate_table",
                    python_callable=INaturalistDataIngester.load_intermediate_table,
                    execution_timeout=timedelta(hours=24),
                    retries=0,
                    op_kwargs={
                        "intermediate_table": sql._get_load_table_name(
                            LOADER_ARGS["identifier"]
                        ),
                        "page_length": 2_000_000,
                    },
                    doc_md=(
                        "Load data from source tables to intermediate table in batches."
                    ),
                )

                clean_transformed_provider_s3_data = PythonOperator(
                    task_id="clean_transformed_provider_s3_data",
                    python_callable=sql.clean_transformed_provider_s3_data,
                    op_kwargs=LOADER_ARGS,
                    execution_timeout=timedelta(hours=6),
                    doc_md="Remove duplicates and records missing required fields.",
                )

                upsert_records_to_db_table = PythonOperator(
                    task_id="upsert_records_to_db_table",
                    python_callable=sql.upsert_records_to_db_table,
                    op_kwargs=LOADER_ARGS,
                    execution_timeout=timedelta(hours=6),
                    doc_md="Add transformed records to the target catalog image table.",
                )

                # TO DO: integrate clean-up stats via xcoms here, keeping in mind that
                # some dupes are excluded up front.
                record_counts_by_media_type[MEDIA_TYPE] = reporting.RecordMetrics(
                    upserted=XCOM_PULL_TEMPLATE.format(
                        upsert_records_to_db_table.task_id, "return_value"
                    ),
                    missing_columns=0,
                    foreign_id_dup=0,
                    url_dup=0,
                )

                (
                    create_loading_table
                    >> load_intermediate_table
                    >> clean_transformed_provider_s3_data
                    >> upsert_records_to_db_table
                )

            postingestion_tasks = INaturalistDataIngester.create_postingestion_tasks()

            (preingestion_tasks >> pull_data >> loader_tasks >> postingestion_tasks)

        # Reporting on the time it takes to load transformed data into the intermediate
        # table rather than the time it takes to load from the s3 source, because it's
        # easier to report out on a task than a task group and loading the intermediate
        # table takes a lot longer anyway.
        ingestion_metrics = {
            "duration": XCOM_PULL_TEMPLATE.format(
                load_intermediate_table.task_id, "duration"
            ),
            "record_counts_by_media_type": record_counts_by_media_type,
        }

        return ingest_data, ingestion_metrics
