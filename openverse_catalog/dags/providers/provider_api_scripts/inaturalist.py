"""
Provider:   Inaturalist

Output:     TSV file containing the media metadata.

Notes:      [The inaturalist API is not intended for data scraping.]
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
from pathlib import Path
from textwrap import dedent
from typing import Dict

from airflow.providers.postgres.hooks.postgres import PostgresHook
from common.constants import POSTGRES_CONN_ID
from common.licenses import LicenseInfo, get_license_info
from common.loader import provider_details as prov
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


# from typing import Dict


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

PROVIDER = prov.INATURALIST_DEFAULT_PROVIDER
SCRIPT_DIR = Path(__file__).parents[1] / "provider_csv_load_scripts/inaturalist"
PG_TO_JSON_TEMPLATE = dedent(
    (SCRIPT_DIR / "05_export_dbpage_to_json_template.sql").read_text()
)


class inaturalistDataIngester(ProviderDataIngester):

    # if we go with the db paginated approach, consider using this sql to get a
    # max number of db pages to define a get_should_continue function.
    # select relpages
    # from pg_class t join pg_namespace s on t.relnamespace=s.oid
    # where s.nspname='inaturalist' and t.relname='photos';

    providers = {"image": prov.INATURALIST_DEFAULT_PROVIDER}

    def __init__(self):
        super(inaturalistDataIngester, self).__init__()
        self.pg = PostgresHook(POSTGRES_CONN_ID)
        self.cursor = None

    def ingest_records(self):
        with self.pg.get_cursor() as self.cursor:
            # Explicitly set the number of results returned by fetchmany()
            # https://www.psycopg.org/docs/cursor.html#cursor.arraysize
            # itersize is the count fetched from postgres on the backend
            # and it defaults to 2000, we don't need to change that
            # https://www.psycopg.org/docs/cursor.html#cursor.itersize
            self.cursor.arraysize = 100
            self.cursor.connection.autocommit = True
            super(inaturalistDataIngester, self).ingest_records()

    def get_next_query_params(self, old_query_params=None, **kwargs):
        return None
        # """Page counter"""
        # if old_query_params is None:
        #     return {"page_number": 0}
        # else:
        #     next_page = old_query_params["page_number"] + 1
        #     return {"page_number": next_page}

    def get_response_json(self, query_params: Dict):
        if self.cursor.rowcount == -1:
            # Query has not yet been executed, need to execute for the first time
            logger.info("Executing big view-to-tsv query on the postgres end")
            self.cursor.execute(PG_TO_JSON_TEMPLATE)
        # Return the next 100 results
        return self.cursor.fetchmany()
        # """
        # Call the SQL to pull json from Postgres, where the raw data has been loaded.
        # """
        # db_page_number = str(query_params["page_number"])
        # sql_string = PG_TO_JSON_TEMPLATE.replace("db_page_number", db_page_number)
        # return self.pg.get_records(sql_string)

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
            "raw_tags": data.get("tags"),
        }

    def get_media_type(self, record):
        # This provider only supports Images via S3, though they have some audio files
        # on site and in the API.
        return "image"

    def endpoint(self):
        raise NotImplementedError("Normalized TSV files from AWS S3 means no endpoint.")

    def sql_loader(self, file_name):
        """
        This is really only intended for SQL scripts that don't return much in the way
        of data, e.g. the load and schema creation scripts. It takes a file name which
        must exist in SCRIPT_DIR and end with .sql.
        """
        if (SCRIPT_DIR / file_name).exists() and file_name[-4:] == ".sql":
            query = dedent((SCRIPT_DIR / file_name).read_text())
            logger.info("Begin: loading iNaturalist " + file_name)
            return self.pg.get_records(query)
        else:
            raise FileExistsError(file_name + "not found or not .sql")
