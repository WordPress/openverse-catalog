import json

import retired.update_workflows
from storage import columns as col
from util.loader import sql

from tests.dags.util.loader.test_sql import (
    POSTGRES_CONN_ID,
    TEST_ID,
    TEST_IMAGE_TABLE,
    TEST_LOAD_TABLE,
    create_query_values,
    fid_idx,
    source_idx,
)


def test_update_flickr_sub_providers(postgres_with_load_and_image_table):
    postgres_conn_id = POSTGRES_CONN_ID
    load_table = TEST_LOAD_TABLE
    image_table = TEST_IMAGE_TABLE
    identifier = TEST_ID

    FID_A = "a"
    FID_B = "b"
    IMG_URL_A = "https://images.com/a/img.jpg"
    IMG_URL_B = "https://images.com/b/img.jpg"
    CREATOR_URL_A = "https://www.flickr.com/photos/29988733@N04"
    CREATOR_URL_B = "https://www.flickr.com/photos/other_user"
    PROVIDER = "flickr"
    LICENSE = "by"
    TAGS = [
        {"name": "tagone", "provider": "test"},
        {"name": "tagtwo", "provider": "test"},
    ]

    insert_data_query = f"""INSERT INTO {load_table} VALUES
        ({create_query_values({
        col.FOREIGN_ID.db_name: FID_A,
        col.DIRECT_URL.db_name: IMG_URL_A,
        col.LICENSE.db_name: LICENSE,
        col.CREATOR_URL.db_name: CREATOR_URL_A,
        col.TAGS.db_name: json.dumps(TAGS),
        col.PROVIDER.db_name: PROVIDER,
        col.SOURCE.db_name: PROVIDER,
        })}),
        ({create_query_values({
        col.FOREIGN_ID.db_name: FID_B,
        col.DIRECT_URL.db_name: IMG_URL_B,
        col.LICENSE.db_name: LICENSE,
        col.CREATOR_URL.db_name: CREATOR_URL_B,
        col.TAGS.db_name: json.dumps(TAGS),
        col.PROVIDER.db_name: PROVIDER,
        col.SOURCE.db_name: PROVIDER,
        })});"""

    postgres_with_load_and_image_table.cursor.execute(insert_data_query)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"DELETE FROM {load_table};")
    postgres_with_load_and_image_table.connection.commit()

    retired.update_workflows.update_sql.update_flickr_sub_providers(
        postgres_conn_id, image_table
    )
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    actual_rows = postgres_with_load_and_image_table.cursor.fetchall()
    assert len(actual_rows) == 2

    for actual_row in actual_rows:
        if actual_row[fid_idx] == "a":
            assert actual_row[source_idx] == "nasa"
        else:
            assert actual_row[fid_idx] == "b" and actual_row[source_idx] == "flickr"


def test_update_europeana_sub_providers(postgres_with_load_and_image_table):
    postgres_conn_id = POSTGRES_CONN_ID
    load_table = TEST_LOAD_TABLE
    image_table = TEST_IMAGE_TABLE
    identifier = TEST_ID

    FID_A = "a"
    FID_B = "b"
    IMG_URL_A = "https://images.com/a/img.jpg"
    IMG_URL_B = "https://images.com/b/img.jpg"
    PROVIDER = "europeana"
    LICENSE = "by-nc-nd"
    META_DATA_A = {
        "country": ["Sweden"],
        "dataProvider": ["Wellcome Collection"],
        "description": "A",
        "license_url": "http://creativecommons.org/licenses/by-nc-nd/4.0/",
    }
    META_DATA_B = {
        "country": ["Sweden"],
        "dataProvider": ["Other Collection"],
        "description": "B",
        "license_url": "http://creativecommons.org/licenses/by-nc-nd/4.0/",
    }

    query_values = [
        create_query_values(
            {
                col.FOREIGN_ID.db_name: FID_A,
                col.DIRECT_URL.db_name: IMG_URL_A,
                col.LICENSE.db_name: LICENSE,
                col.META_DATA.db_name: json.dumps(META_DATA_A),
                col.PROVIDER.db_name: PROVIDER,
                col.SOURCE.db_name: PROVIDER,
            }
        ),
        create_query_values(
            {
                col.FOREIGN_ID.db_name: FID_B,
                col.DIRECT_URL.db_name: IMG_URL_B,
                col.LICENSE.db_name: LICENSE,
                col.META_DATA.db_name: json.dumps(META_DATA_B),
                col.PROVIDER.db_name: PROVIDER,
                col.SOURCE.db_name: PROVIDER,
            }
        ),
    ]
    insert_data_query = f"""INSERT INTO {load_table} VALUES
        ({query_values[0]}),
        ({query_values[1]}
        );"""

    postgres_with_load_and_image_table.cursor.execute(insert_data_query)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"DELETE FROM {load_table};")
    postgres_with_load_and_image_table.connection.commit()

    retired.update_workflows.update_sql.update_europeana_sub_providers(
        postgres_conn_id, image_table
    )
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    actual_rows = postgres_with_load_and_image_table.cursor.fetchall()
    assert len(actual_rows) == 2

    for actual_row in actual_rows:
        if actual_row[fid_idx] == "a":
            assert actual_row[source_idx] == "wellcome_collection"
        else:
            assert actual_row[fid_idx] == "b" and actual_row[source_idx] == "europeana"


def test_update_smithsonian_sub_providers(postgres_with_load_and_image_table):
    postgres_conn_id = POSTGRES_CONN_ID
    load_table = TEST_LOAD_TABLE
    image_table = TEST_IMAGE_TABLE
    identifier = TEST_ID

    FID_A = "a"
    FID_B = "b"
    IMG_URL_A = "https://images.com/a/img.jpg"
    IMG_URL_B = "https://images.com/b/img.jpg"
    PROVIDER = "smithsonian"
    LICENSE = "by-nc-nd"
    META_DATA_A = {
        "unit_code": "SIA",
        "data_source": "Smithsonian Institution Archives",
    }
    META_DATA_B = {
        "unit_code": "NMNHBIRDS",
        "data_source": "NMNH - Vertebrate Zoology - Birds Division",
    }

    query_values = [
        create_query_values(
            {
                col.FOREIGN_ID.db_name: FID_A,
                col.DIRECT_URL.db_name: IMG_URL_A,
                col.LICENSE.db_name: LICENSE,
                col.META_DATA.db_name: json.dumps(META_DATA_A),
                col.PROVIDER.db_name: PROVIDER,
                col.SOURCE.db_name: PROVIDER,
            }
        ),
        create_query_values(
            {
                col.FOREIGN_ID.db_name: FID_B,
                col.DIRECT_URL.db_name: IMG_URL_B,
                col.LICENSE.db_name: LICENSE,
                col.META_DATA.db_name: json.dumps(META_DATA_B),
                col.PROVIDER.db_name: PROVIDER,
                col.SOURCE.db_name: PROVIDER,
            }
        ),
    ]
    insert_data_query = f"""INSERT INTO {load_table} VALUES
        ({query_values[0]}),
        ({query_values[1]}
        );"""

    postgres_with_load_and_image_table.cursor.execute(insert_data_query)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"DELETE FROM {load_table};")
    postgres_with_load_and_image_table.connection.commit()

    retired.update_workflows.update_sql.update_smithsonian_sub_providers(
        postgres_conn_id, image_table
    )
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    actual_rows = postgres_with_load_and_image_table.cursor.fetchall()
    assert len(actual_rows) == 2

    for actual_row in actual_rows:
        if actual_row[fid_idx] == "a":
            assert actual_row[source_idx] == "smithsonian_institution_archives"
        else:
            assert (
                actual_row[fid_idx] == "b"
                and actual_row[source_idx]
                == "smithsonian_national_museum_of_natural_history"
            )
