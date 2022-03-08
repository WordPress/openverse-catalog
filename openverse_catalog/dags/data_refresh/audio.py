"""
This file configures the Apache Airflow DAG to refresh data for
the Audio media type.
"""
# airflow DAG (necessary for Airflow to find this file)
from datetime import datetime, timedelta

from common.provider_dag_factory import create_data_refresh_dag


DAG_ID = "audio_data_refresh"
START_DATE = datetime(2020, 1, 1)


globals()[DAG_ID] = create_data_refresh_dag(
    DAG_ID,
    media_type="audio",
    external_dag_ids=[
        "image_data_refresh",
    ],
    start_date=START_DATE,
    execution_timeout=timedelta(hours=24),
)
