"""
What am I trying to test here?
-   Data gets from S3 into postgres
-   Records in the final view look like what we expect them to look like [depends on above, which isn't 
    working yet]

What do I need in place to test it?
-   I need to have a way to make airflow think that it is pulling data from s3://inaturalist-open-data
    when it is actually coming from small test files I've created from the inaturalist data
-   I need to have a way to look at the results in the database

How do we accomplish this?
-   Changed the s3 volume in docker-compose.override.yml to come from a physical folder tests/s3-data, 
    that includes a subfolder (inaturalist-open-data) with the temporary files
-   I hoped that this would mean that tests.dags.common.conftest.pytest_configure would handle sending
    requests to the minio (local s3) bucket, rather than the real s3 bucket.
-   But right now, the requests (which actually come via db function here:
    https://github.com/WordPress/openverse-catalog/blob/main/docker/local_postgres/0002_aws_s3_mock.sql)
    are coming back saying they can't find the file on s3. 

TO DO:
-   There will be steps to actually update the image table, provide relevant reporting, and flow 
    through the rest of the pipeline to elastic search
-   And then I'll need to test that
"""

from unittest import mock

import pytest
from providers.provider_api_scripts import inaturalist

########################################################
# These are just record counts from the sample files 
########################################################
# ---> Reading file taxa.csv.gz
# 183 records
# ---> Reading file observations.csv.gz
# 31 records
# ---> Reading file observers.csv.gz
# 22 records
# ---> Reading file photos.csv.gz
# 36 records

@pytest.mark.parametrize(
    "file_name, expected",
    [
        ('00_create_schema.sql', [('inaturalist', )]),
        ('01_photos.sql', [(36, )] ),
        ('02_observations.sql', [(31, )] ),
        ('03_taxa.sql', [(183, )] ),
        ('04_observers.sql', [(22, )] ),
    ]
)
def test_load_from_s3(file_name, expected):
    actual = inaturalist.run_sql_file(file_name)
    assert actual == expected
