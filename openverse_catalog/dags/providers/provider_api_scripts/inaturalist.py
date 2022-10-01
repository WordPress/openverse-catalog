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
from datetime import timedelta
from pathlib import Path
from typing import Dict

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from common.constants import POSTGRES_CONN_ID
from common.licenses import LicenseInfo, get_license_info
from common.loader import provider_details, sql
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logger = logging.getLogger(__name__)

AWS_CONN_ID = os.getenv("AWS_CONN_ID", "test_conn_id")
OPENVERSE_BUCKET = os.getenv("OPENVERSE_BUCKET", "openverse-storage")
PROVIDER = provider_details.INATURALIST_DEFAULT_PROVIDER
SCRIPT_DIR = Path(__file__).parents[1] / "provider_csv_load_scripts/inaturalist"
SOURCE_FILE_NAMES = ["photos", "observations", "taxa", "observers"]
LOADER_ARGS = {
    "postgres_conn_id": POSTGRES_CONN_ID,
    "identifier": "{{ ts_nodash }}",
    "media_type": "image",
}


class INaturalistDataIngester(ProviderDataIngester):

    providers = {"image": provider_details.INATURALIST_DEFAULT_PROVIDER}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pg = PostgresHook(POSTGRES_CONN_ID)

        # adjustments to buffer limits. TO DO: try to integrate this with the dev
        # environment logic in the base class, rather than just over-writing it.
        # See https://github.com/WordPress/openverse-catalog/issues/682
        self.media_stores["image"].buffer_length = 10_000
        self.batch_limit = 10_000
        self.json_sql_template = (
            SCRIPT_DIR / "export_to_json.template.sql"
        ).read_text()

    def get_next_query_params(self, prev_query_params=None, **kwargs):
        if prev_query_params is None:
            return {"offset_num": 0}
        else:
            next_offset = prev_query_params["offset_num"] + self.batch_limit
            return {"offset_num": next_offset}

    def get_response_json(self, query_params: Dict):
        # """
        # Call the SQL to pull json from Postgres, where the raw data has been loaded.
        # """
        # sql_string = self.json_sql_template.format(
        #     batch_limit=self.batch_limit, offset_num=query_params["offset_num"]
        # )
        # sql_result = self.pg.get_records(sql_string)
        # # Postgres returns a list of tuples, even if it's one tuple with one item.
        # return sql_result[0][0]
        pass

    def get_batch_data(self, response_json):
        if response_json:
            return response_json
        return None

    def get_record_data(self, data):
        if data.get("foreign_identifier") is None:
            # TO DO: maybe raise an error here or after a certain number of these?
            # more info in https://github.com/WordPress/openverse-catalog/issues/684
            return None
        license_url = data.get("license_url")
        license_info = get_license_info(license_url=license_url)
        if license_info == LicenseInfo(None, None, None, None):
            return None
        record_data = {k: data[k] for k in data.keys() if k != "license_url"}
        record_data["license_info"] = license_info
        return record_data

    def get_media_type(self, record):
        # This provider only supports Images via S3, though they have some audio files
        # on site and in the API.
        return "image"

    def endpoint(self):
        raise NotImplementedError("Normalized TSV files from AWS S3 means no endpoint.")

    @staticmethod
    def load_intermediate_table(
        intermediate_table: str,
        postgres_conn_id=POSTGRES_CONN_ID,
        sql_template_file_name="transformed_table.template.sql",
        page_length=2_000_000,
    ):
        pg = PostgresHook(postgres_conn_id)
        sql_template = (SCRIPT_DIR / sql_template_file_name).read_text()
        # get the number of "pages" in the photos table
        max_id = pg.get_records("SELECT max(photo_id) FROM inaturalist.photos")[0][0]
        logger.info(f"Loading {intermediate_table}...")
        logger.info(f"Max photo_id={max_id}. Transform {page_length} ids / iteration.")
        # then cycle through the pages
        for page_start in range(0, max_id, page_length):
            page_end = page_start + page_length - 1
            sql = sql_template.format(
                intermediate_table=intermediate_table,
                page_start=page_start,
                page_end=page_end,
            )
            total_records = pg.get_records(sql)[0][0]
            logger.info(f"{page_start=}, loaded {total_records} so far.")

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

            with TaskGroup(group_id="load_raw_source_files") as load_raw_source_files:
                for source_name in SOURCE_FILE_NAMES:
                    PostgresOperator(
                        task_id=f"load_{source_name}",
                        postgres_conn_id=POSTGRES_CONN_ID,
                        sql=(SCRIPT_DIR / f"{source_name}.sql").read_text(),
                        doc_md=f"Load iNaturalist {source_name} from s3 to postgres",
                    ),

            (
                check_for_file_updates
                >> create_inaturalist_schema
                >> load_raw_source_files
            )

        return preingestion_tasks

    @staticmethod
    def create_postingestion_tasks():
        # TO DO: add drop_loading_table step from database.loader_workflow here or in
        # selected loader tasks.
        drop_inaturalist_schema = PostgresOperator(
            task_id="drop_inaturalist_schema",
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="DROP SCHEMA IF EXISTS inaturalist CASCADE",
            doc_md="Drop iNaturalist source tables and their schema",
        )
        return drop_inaturalist_schema

    @staticmethod
    def create_ingestion_workflow():

        with TaskGroup(group_id="ingest_data") as ingest_data:

            preingestion_tasks = INaturalistDataIngester.create_preingestion_tasks()

            with TaskGroup(group_id="loader_tasks") as loader_tasks:

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
                    execution_timeout=timedelta(hours=6),
                    op_kwargs={
                        "intermediate_table": sql._get_load_table_name(
                            LOADER_ARGS["identifier"]
                        )
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

                (
                    create_loading_table
                    >> load_intermediate_table
                    >> clean_transformed_provider_s3_data
                    >> upsert_records_to_db_table
                )

            postingestion_tasks = INaturalistDataIngester.create_postingestion_tasks()

            (preingestion_tasks >> loader_tasks >> postingestion_tasks)

        return ingest_data
