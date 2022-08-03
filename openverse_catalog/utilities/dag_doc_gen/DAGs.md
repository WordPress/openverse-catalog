# DAGs

_Note: this document is auto-generated and should not be manually edited_

This document describes the DAGs available along with pertinent DAG information and
the DAG's documentation.

# Available DAGs
## Commoncrawl

| DAG ID | Schedule Interval |
| --- | --- |
| `sync_commoncrawl_workflow` | `0 16 15 * *` |
| `commoncrawl_etl_workflow` | `0 0 * * 1` |

## Data Refresh

| DAG ID | Schedule Interval |
| --- | --- |
| `image_data_refresh` | `@weekly` |
| `audio_data_refresh` | `@weekly` |

## Database

| DAG ID | Schedule Interval |
| --- | --- |
| `recreate_audio_popularity_calculation` | `None` |
| `recreate_image_popularity_calculation` | `None` |
| `report_pending_reported_media` | `@weekly` |
| `image_expiration_workflow` | `None` |
| `tsv_to_postgres_loader` | `None` |

## Maintenance

| DAG ID | Schedule Interval |
| --- | --- |
| `airflow_log_cleanup` | `@weekly` |
| `pr_review_reminders` | `0 0 * * 1-5` |

## Oauth

| DAG ID | Schedule Interval |
| --- | --- |
| `oauth2_token_refresh` | `0 */12 * * *` |
| `oauth2_authorization` | `None` |

## Provider

| DAG ID | Schedule Interval | Dated | Media Type(s) |
| --- | --- | --- | --- |
| `brooklyn_museum_workflow` | `@monthly` | `False` | image |
| `cleveland_museum_workflow` | `@monthly` | `False` | image |
| `europeana_workflow` | `@daily` | `True` | image |
| `finnish_museums_workflow` | `@monthly` | `False` | image |
| `flickr_workflow` | `@daily` | `True` | image |
| `freesound_workflow` | `@monthly` | `False` | audio |
| `jamendo_workflow` | `@monthly` | `False` | audio |
| `metropolitan_museum_workflow` | `@daily` | `True` | image |
| `museum_victoria_workflow` | `@monthly` | `False` | image |
| `nypl_workflow` | `@monthly` | `False` | image |
| `phylopic_workflow` | `@weekly` | `True` | image |
| `rawpixel_workflow` | `@monthly` | `False` | image |
| `science_museum_workflow` | `@monthly` | `False` | image |
| `smithsonian_workflow` | `@weekly` | `False` | image |
| `smk_workflow` | `@monthly` | `False` | image |
| `stocksnap_workflow` | `@monthly` | `False` | image |
| `walters_workflow` | `@monthly` | `False` | image |
| `wikimedia_commons_workflow` | `@daily` | `True` | image, audio |
| `wordpress_workflow` | `@monthly` | `False` | image |

## Provider Reingestion

| DAG ID | Schedule Interval |
| --- | --- |
| `europeana_ingestion_workflow` | `@daily` |
| `flickr_ingestion_workflow` | `@daily` |
| `wikimedia_ingestion_workflow` | `@daily` |
