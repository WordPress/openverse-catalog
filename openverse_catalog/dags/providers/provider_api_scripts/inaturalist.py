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
from pathlib import Path
from textwrap import dedent
from typing import Dict

import pendulum
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from common.constants import POSTGRES_CONN_ID
from common.licenses import LicenseInfo, get_license_info
from common.loader import provider_details as prov
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

AWS_CONN_ID = os.getenv("AWS_CONN_ID", "test_conn_id")
PROVIDER = prov.INATURALIST_DEFAULT_PROVIDER
SCRIPT_DIR = Path(__file__).parents[1] / "provider_csv_load_scripts/inaturalist"
SOURCE_FILE_NAMES = ["photos", "observations", "taxa", "observers"]


class iNaturalistDataIngester(ProviderDataIngester):

    providers = {"image": prov.INATURALIST_DEFAULT_PROVIDER}

    def __init__(self, *kwargs):
        super(iNaturalistDataIngester, self).__init__()
        self.pg = PostgresHook(POSTGRES_CONN_ID)

        # adjustments to buffer limits. TO DO: try to integrate this with the dev
        # environment logic in the base class, rather than just over-writing it.
        self.media_stores["image"].buffer_length = 10_000
        self.batch_limit = 10_000

        self.sql_template = dedent(
            (SCRIPT_DIR / "05_export_to_json_template.sql")
            .read_text()
            .replace("batch_limit", str(self.batch_limit))
        )

    def get_next_query_params(self, old_query_params=None, **kwargs):
        if old_query_params is None:
            return {"offset_num": 0}
        else:
            next_offset = old_query_params["offset_num"] + self.batch_limit
            return {"offset_num": next_offset}

    def get_response_json(self, query_params: Dict):
        """
        Call the SQL to pull json from Postgres, where the raw data has been loaded.
        """
        offset_num = str(query_params["offset_num"])
        sql_string = self.sql_template.replace("offset_num", offset_num)
        return self.pg.get_records(sql_string)

    def get_batch_data(self, response_json):
        if response_json:
            return [dict(item[0]) for item in response_json]
        return None

    def get_record_data(self, data):
        license_url = data.get("license_url", None)
        license_info = get_license_info(license_url=license_url)
        if license_info == LicenseInfo(None, None, None, None):
            return None

        foreign_id = data.get("foreign_id")
        if foreign_id is None:
            return None

        return {
            "foreign_identifier": f"{foreign_id}",
            "foreign_landing_url": data.get("foreign_landing_url"),
            "title": data.get("title"),
            "creator": data.get("creator"),
            "image_url": data.get("image_url"),
            "width": data.get("width"),
            "height": data.get("height"),
            "license_info": license_info,
            "filetype": data.get("filetype"),
            "creator_url": data.get("creator_url"),
            "raw_tags": data.get("tags").split("; "),
        }

    def get_media_type(self, record):
        # This provider only supports Images via S3, though they have some audio files
        # on site and in the API.
        return "image"

    def endpoint(self):
        raise NotImplementedError("Normalized TSV files from AWS S3 means no endpoint.")

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
            try:
                last_modified = s3_client.head_object(
                    Bucket="inaturalist-open-data", Key=key
                )["LastModified"]
            except Exception as e:
                logger.error(e)
                raise AirflowFailException(f"Can't find {key} on s3")
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
                python_callable=iNaturalistDataIngester.compare_update_dates,
                op_kwargs={
                    # With the templated values ({{ x }}) airflow will fill it in
                    "last_success": "{{ prev_start_date_success }}",
                    "s3_keys": [
                        f"{file_name}.csv.gz" for file_name in SOURCE_FILE_NAMES
                    ],
                },
            )

            # A prior draft had a separate python function to run the SQL and add some
            # logging, but the native airflow logging should be enough, right?
            create_schema = PostgresOperator(
                task_id="create_schema",
                postgres_conn_id=POSTGRES_CONN_ID,
                sql=dedent((SCRIPT_DIR / "00_create_schema.sql").read_text()),
            )

            with TaskGroup(group_id="load_source_files") as load_source_files:
                for idx, source_name in enumerate(SOURCE_FILE_NAMES):
                    PostgresOperator(
                        task_id=f"load_{source_name}",
                        postgres_conn_id=POSTGRES_CONN_ID,
                        sql=dedent(
                            (
                                SCRIPT_DIR
                                / (str(idx + 1).zfill(2) + f"_{source_name}.sql")
                            ).read_text()
                        ),
                    )

            (check_for_file_updates >> create_schema >> load_source_files)

        return preingestion_tasks
