"""
Content Provider:       Europeana

ETL Process:            Use the API to identify all CC licensed images.

Output:                 TSV file containing the images and the
                        respective meta-data.

Notes:                  https://www.europeana.eu/api/v2/search.json
"""
import argparse
import logging
from datetime import datetime, timedelta, timezone

import common
from airflow.models import Variable
from common.licenses import get_license_info
from common.loader import provider_details as prov
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logger = logging.getLogger(__name__)
logging.getLogger(common.urls.__name__).setLevel(logging.WARNING)


class EuropeanaRecordBuilder:
    """
    A small class to contain the record building functionality
    and simplify testing a bit.
    """

    def get_record_data(self, data: dict) -> dict:
        record = {
            "foreign_landing_url": self._get_foreign_landing_url(data),
            "image_url": data.get("edmIsShownBy")[0],
            "foreign_identifier": data.get("id"),
            "meta_data": self._get_meta_data_dict(data),
            "title": data.get("title")[0],
            "license_info": get_license_info(
                license_url=self._get_license_url(data.get("rights"))
            ),
        }

        data_providers = set(record["meta_data"]["dataProvider"])
        eligible_sub_providers = {
            s
            for s in EuropeanaDataIngester.sub_providers
            if EuropeanaDataIngester.sub_providers[s] in data_providers
        }
        if len(eligible_sub_providers) > 1:
            raise Exception(
                f"More than one sub-provider identified for the "
                f"image with foreign ID {record['foreign_identifier']}"
            )

        return record | {
            "source": (
                eligible_sub_providers.pop()
                if len(eligible_sub_providers) == 1
                else EuropeanaDataIngester.providers["image"]
            )
        }

    def _get_license_url(self, license_field) -> str | None:
        if len(license_field) > 1:
            logger.warning("More than one license field found")
        for license_ in license_field:
            if "creativecommons" in license_:
                return license_
        return None

    def _get_foreign_landing_url(self, data: dict) -> str:
        original_url = data.get("edmIsShownAt")
        if original_url is not None:
            return original_url[0]
        europeana_url = data.get("guid")
        return europeana_url

    def _get_meta_data_dict(self, data: dict) -> dict:
        meta_data = {
            "country": data.get("country"),
            "dataProvider": data.get("dataProvider"),
            "description": self._get_description(data),
        }

        return {k: v for k, v in meta_data.items() if v is not None}

    def _get_description(self, data: dict) -> str | None:
        description = None
        lang_aware_description = data.get("dcDescriptionLangAware")
        if lang_aware_description:
            description = lang_aware_description.get(
                "en"
            ) or lang_aware_description.get("def")

        if not description:  # cover None and []
            description = data.get("dcDescription")

        if description:
            return description[0].strip()

        return ""


class EuropeanaDataIngester(ProviderDataIngester):
    providers = {"image": prov.EUROPEANA_DEFAULT_PROVIDER}
    sub_providers = prov.EUROPEANA_SUB_PROVIDERS
    endpoint = "https://www.europeana.eu/api/v2/search.json?"
    delay = 30

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Each response back from Europeana returns a `nextCursor`
        # property that needs to be passed to subsequent requests
        # as `cursor`. This allows us to systematically page
        # through the API data.
        self.cursor = None

        self.base_request_body = {
            "wskey": Variable.get("API_KEY_EUROPEANA", default_var=None),
            "profile": "rich",
            "reusability": ["open", "restricted"],
            "sort": ["europeana_id+desc", "timestamp_created+desc"],
            "rows": str(self.batch_limit),
            "media": "true",
            "start": 1,
            "qf": ["TYPE:IMAGE", "provider_aggregation_edm_isShownBy:*"],
            # As a dated DAG, Europeana accepts a ``query`` prop in the
            # request params that delineates the timestamps between which
            # records will have been added. The base class sets up the
            # ``self.date`` attribute for us, so we can construct that
            # ``query`` prop for the request params ahead of time.
            "query": self._get_timestamp_query_param(self.date),
            "cursor": "*",
        }

        self.record_builder = EuropeanaRecordBuilder()

    def _get_timestamp_query_param(self, date):
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        utc_date = date_obj.replace(tzinfo=timezone.utc)
        start_timestamp = utc_date.isoformat()
        end_timestamp = (utc_date + timedelta(days=1)).isoformat()

        start_timestamp = start_timestamp.replace("+00:00", "Z")
        end_timestamp = end_timestamp.replace("+00:00", "Z")

        return f"timestamp_created:[{start_timestamp} TO {end_timestamp}]"

    def get_next_query_params(self, prev_query_params) -> dict:
        if not prev_query_params:
            return self.base_request_body

        return prev_query_params | {
            "cursor": self.cursor,
        }

    def get_should_continue(self, response_json: dict):
        self.cursor = response_json.get("nextCursor")

        return self.cursor is not None

    def get_batch_data(self, response_json: dict) -> None | list[dict]:
        if response_json.get("success") != "True":
            logger.warning('Request failed with ``success = "False"``')
            # No batch data to process if the request failed.
            return None

        return response_json.get("items")

    def get_record_data(self, data: dict) -> dict:
        return self.record_builder.get_record_data(data)


def main(date):
    logger.info(f"Begin: Europeana data ingestion for {date}")
    ingester = EuropeanaDataIngester(date)
    ingester.ingest_records()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Europeana API Job", add_help=True)
    parser.add_argument(
        "--date", help="Identify images uploaded on a date (format: YYYY-MM-DD)."
    )
    args = parser.parse_args()
    if args.date:
        date = args.date
    else:
        date_obj = datetime.now() - timedelta(days=2)
        date = datetime.strftime(date_obj, "%Y-%m-%d")

    main(date)
