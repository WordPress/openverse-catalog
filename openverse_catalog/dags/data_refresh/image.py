"""
This file configures the Apache Airflow DAG to refresh data for
the Image media type.
"""
# airflow DAG (necessary for Airflow to find this file)
from datetime import datetime, timedelta

from data_refresh.dag_factory import create_data_refresh_dag


DAG_ID = "image_data_refresh"
START_DATE = datetime(2020, 1, 1)


globals()[DAG_ID] = create_data_refresh_dag(
    DAG_ID,
    media_type="image",
    external_dag_ids=[
        "audio_data_refresh",
    ],
    start_date=START_DATE,
    execution_timeout=timedelta(hours=24),
)
