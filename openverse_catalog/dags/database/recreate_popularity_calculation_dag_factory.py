"""
This file generates Apache Airflow DAGs that, for the given media type,
completely wipe out the PostgreSQL relations and functions involved in
calculating our standardized popularity metric. It then recreates relations
and functions to make the calculation, and performs an initial calculation.
The results are available in the materialized view for that media type.

These DAGs are not on a schedule, and should only be run manually when new
SQL code is deployed for the calculation.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from common.constants import DAG_DEFAULT_ARGS, POSTGRES_CONN_ID
from common.popularity import sql
from data_refresh.data_refresh_types import DATA_REFRESH_CONFIGS, DataRefresh


def create_recreate_popularity_calculation_dag(data_refresh: DataRefresh):
    media_type = data_refresh.media_type
    default_args = {
        **DAG_DEFAULT_ARGS,
        **data_refresh.default_args,
    }

    dag = DAG(
        dag_id=f"recreate_{media_type}_popularity_calculation",
        default_args=default_args,
        max_active_tasks=1,
        max_active_runs=1,
        schedule_interval=None,
        catchup=False,
        doc_md=__doc__,
        tags=["database", "data_refresh"],
    )
    with dag:
        # Drop the existing popularity views and tables.
        drop_relations = PythonOperator(
            task_id="drop_media_popularity_relations",
            python_callable=sql.drop_media_popularity_relations,
            op_args=[POSTGRES_CONN_ID, media_type],
        )
        # Drop the popularity functions.
        drop_functions = PythonOperator(
            task_id="drop_media_popularity_functions",
            python_callable=sql.drop_media_popularity_functions,
            op_args=[POSTGRES_CONN_ID, media_type],
        )
        # Create the popularity metrics table, which maps popularity
        # metrics and percentiles to providers.
        create_metrics = PythonOperator(
            task_id="create_media_popularity_metrics_table",
            python_callable=sql.create_media_popularity_metrics,
            op_args=[POSTGRES_CONN_ID, media_type],
        )

        # Inserts values into the popularity metrics table.
        update_metrics = PythonOperator(
            task_id="update_media_popularity_metrics_table",
            python_callable=sql.update_media_popularity_metrics,
            op_args=[POSTGRES_CONN_ID, media_type],
        )

        # Create the function for calculating percentile.
        create_percentile = PythonOperator(
            task_id="create_media_popularity_percentile",
            python_callable=sql.create_media_popularity_percentile_function,
            op_args=[POSTGRES_CONN_ID, media_type],
        )

        # Create a materialized view for popularity constants per provider.
        create_constants = PythonOperator(
            task_id="create_media_popularity_constants_view",
            python_callable=sql.create_media_popularity_constants_view,
            op_args=[POSTGRES_CONN_ID, media_type],
            execution_timeout=data_refresh.create_pop_constants_view_timeout,
        )

        # Create a function to calculate standardized media popularity.
        create_popularity = PythonOperator(
            task_id="create_media_standardized_popularity",
            python_callable=sql.create_standardized_media_popularity_function,
            op_args=[POSTGRES_CONN_ID, media_type],
        )

        # Create the materialized popularity view.
        create_matview = PythonOperator(
            task_id="create_materialized_popularity_view",
            python_callable=sql.create_media_view,
            op_args=[POSTGRES_CONN_ID, media_type],
            execution_timeout=data_refresh.create_materialized_view_timeout,
        )

        (
            [drop_relations, drop_functions]
            >> create_metrics
            >> [update_metrics, create_percentile]
            >> create_constants
            >> create_popularity
            >> create_matview
        )

    return dag


# Generate a recreate_popularity_calculation DAG for each DATA_REFRESH_CONFIG.
for data_refresh in DATA_REFRESH_CONFIGS:
    recreate_popularity_calculation_dag = create_recreate_popularity_calculation_dag(
        data_refresh
    )
    globals()[
        recreate_popularity_calculation_dag.dag_id
    ] = recreate_popularity_calculation_dag
