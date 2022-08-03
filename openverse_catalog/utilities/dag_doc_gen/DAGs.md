# DAGs

_Note: this document is auto-generated and should not be manually edited_

This document describes the DAGs available along with pertinent DAG information and
the DAG's documentation.

## Available DAGs
 - `europeana_ingestion_workflow`: @daily, dated=False, tags=['provider-reingestion'], type=provider-reingestion
 - `flickr_ingestion_workflow`: @daily, dated=False, tags=['provider-reingestion'], type=provider-reingestion
 - `wikimedia_ingestion_workflow`: @daily, dated=False, tags=['provider-reingestion'], type=provider-reingestion
 - `brooklyn_museum_workflow`: @monthly, dated=False, tags=['provider', 'provider: image'], type=provider
 - `cleveland_museum_workflow`: @monthly, dated=False, tags=['provider', 'provider: image'], type=provider
 - `europeana_workflow`: @daily, dated=True, tags=['provider', 'provider: image'], type=provider
 - `finnish_museums_workflow`: @monthly, dated=False, tags=['provider', 'provider: image'], type=provider
 - `flickr_workflow`: @daily, dated=True, tags=['provider', 'provider: image'], type=provider
 - `freesound_workflow`: @monthly, dated=False, tags=['provider', 'provider: audio'], type=provider
 - `jamendo_workflow`: @monthly, dated=False, tags=['provider', 'provider: audio'], type=provider
 - `metropolitan_museum_workflow`: @daily, dated=True, tags=['provider', 'provider: image'], type=provider
 - `museum_victoria_workflow`: @monthly, dated=False, tags=['provider', 'provider: image'], type=provider
 - `nypl_workflow`: @monthly, dated=False, tags=['provider', 'provider: image'], type=provider
 - `phylopic_workflow`: @weekly, dated=True, tags=['provider', 'provider: image'], type=provider
 - `rawpixel_workflow`: @monthly, dated=False, tags=['provider', 'provider: image'], type=provider
 - `science_museum_workflow`: @monthly, dated=False, tags=['provider', 'provider: image'], type=provider
 - `smithsonian_workflow`: @weekly, dated=False, tags=['provider', 'provider: image'], type=provider
 - `smk_workflow`: @monthly, dated=False, tags=['provider', 'provider: image'], type=provider
 - `stocksnap_workflow`: @monthly, dated=False, tags=['provider', 'provider: image'], type=provider
 - `walters_workflow`: @monthly, dated=False, tags=['provider', 'provider: image'], type=provider
 - `wikimedia_commons_workflow`: @daily, dated=True, tags=['provider', 'provider: image', 'provider: audio'], type=provider
 - `wordpress_workflow`: @monthly, dated=False, tags=['provider', 'provider: image'], type=provider
 - `airflow_log_cleanup`: @weekly, dated=False, tags=['maintenance'], type=maintenance
 - `pr_review_reminders`: 0 0 * * 1-5, dated=False, tags=['maintenance'], type=maintenance
 - `image_data_refresh`: @weekly, dated=False, tags=['data_refresh'], type=data_refresh
 - `audio_data_refresh`: @weekly, dated=False, tags=['data_refresh'], type=data_refresh
 - `recreate_audio_popularity_calculation`: None, dated=False, tags=['database', 'data_refresh'], type=database
 - `recreate_image_popularity_calculation`: None, dated=False, tags=['database', 'data_refresh'], type=database
 - `report_pending_reported_media`: @weekly, dated=False, tags=['database'], type=database
 - `image_expiration_workflow`: None, dated=False, tags=['database'], type=database
 - `tsv_to_postgres_loader`: None, dated=False, tags=['database'], type=database
 - `sync_commoncrawl_workflow`: 0 16 15 * *, dated=False, tags=['commoncrawl'], type=commoncrawl
 - `commoncrawl_etl_workflow`: 0 0 * * 1, dated=False, tags=['commoncrawl'], type=commoncrawl
 - `oauth2_token_refresh`: 0 */12 * * *, dated=False, tags=['oauth'], type=oauth
 - `oauth2_authorization`: None, dated=False, tags=['oauth'], type=oauth
