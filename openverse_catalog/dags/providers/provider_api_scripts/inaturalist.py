"""
Provider:   Inaturalist

Process:    With Inaturalist, for reasons described below, we aren't really doing
            ETL, but ELT.

Output:     TSV file containing the media metadata.

Notes:      The inaturalist API is not intended for data scraping.
            https://api.inaturalist.org/v1/docs/
            But there is a full dump intended for sharing on S3.
            https://github.com/inaturalist/inaturalist-open-data/tree/documentation/Metadata
            Because these are very large normalized tables, as opposed to more document
            oriented API responses, we found that bringing the data into postgres first
            was the most effective approach.
            More detail in slack here:
            https://wordpress.slack.com/archives/C02012JB00N/p1653145643080479?thread_ts=1653082292.714469&cid=C02012JB00N
            This uses the structure defined here, except for adding ancestry tags to the
            taxa table:
            https://github.com/inaturalist/inaturalist-open-data/blob/main/Metadata/structure.sql

TO DO:      Resolve some local dev environment issues
            Integrate with ImageStore
"""
import logging
import os
from textwrap import dedent

import psycopg2
from common.loader import provider_details as prov


# from common.storage.image import ImageStore


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

PROVIDER = prov.INATURALIST_DEFAULT_PROVIDER
SCRIPT_DIR = (
    "/usr/local/airflow/openverse_catalog/dags/providers/provider_csv_load_scripts/"
    + "inaturalist/"
)

# This is set up here as a single thread linear process, to be more consisten with the
# structure of the provider dag factory, but if I were to start from scratch with
# airflow, each of these would be a task, and the dag would be
# 00 >> [01, 02, 03, 04] >> 05.
# In dbt, the schema would be created separately, and steps 1-5 would each be a "model"
# and dbt would navigate the dependencies more or less automagically.
LOAD_PROCESS = [
    "00_create_schema.sql",
    "01_photos.sql",
    "02_observations.sql",
    "03_taxa.sql",
    "04_observers.sql",
    "05_final_tsv.sql",
]
CONNECTION_ID = os.getenv("AIRFLOW_CONN_POSTGRES_OPENLEDGER_TESTING")


def run_sql_file(file_name, file_path=SCRIPT_DIR, conn_id=CONNECTION_ID):
    """
    The process is really written in SQL so this script just enables logging
    and monitoring jobs, but this is the basic function to run the SQL files
    for each step.
    """
    logger.info(f"Running {file_name} using DB connection {conn_id}")
    result = "SQL failed. See log for details."
    try:
        assert file_name[-4:] == ".sql"
        assert os.path.exists(file_path + file_name)
        db = psycopg2.connect(conn_id)
        cursor = db.cursor()
        query = dedent(open(file_path + file_name, "r").read())
        cursor.execute(query)
        db.commit()
        if cursor.rowcount and cursor.rowcount > 0:
            result = cursor.fetchall()
        else:
            result = "No rows returned"
        logger.info("Success!")
    except Exception as e:
        logger.exception(f"SQL step failed due to {e}")
    return result


def main():
    """
    This is really just looping through the SQL steps defined above, with some
    additional logging.
    """
    logger.info("Begin: Inaturalist script")

    for f in LOAD_PROCESS:
        image_count = run_sql_file(f)
        logger.info(f"Results: {str(image_count)}")
    logger.info(f"Total images pulled: {image_count}")
    logger.info("Terminated!")


if __name__ == "__main__":
    main()
