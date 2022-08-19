"""
Content Provider:       Metropolitan Museum of Art

ETL Process:            Use the API to identify all CC0 artworks.

Output:                 TSV file containing the image, their respective
                        meta-data.

Notes:                  https://metmuseum.github.io/#search
                        "Please limit requests to 80 requests per second." Changing
                        delay to 3 seconds, because of blocking encountered during
                        development.

                        Some analysis to improve data quality was conducted using a
                        separate csv file here: https://github.com/metmuseum/openaccess
"""

import argparse
import logging

from common.licenses import get_license_info
from common.loader import provider_details as prov
from provider_data_ingester import ProviderDataIngester


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# get a list of object IDs for this week:
# https://collectionapi.metmuseum.org/public/collection/v1/objects?metadataDate=2022-08-10
# get a specific object:
# https://collectionapi.metmuseum.org/public/collection/v1/objects/1027
# The search functionality requires a specific query (term search) in addition to date
# and public domain. It seems like it won't connect with just date and license.
# https://collectionapi.metmuseum.org/public/collection/v1/search?isPublicDomain=true&metadataDate=2022-08-07


class MetMuseumDataIngester(ProviderDataIngester):
    providers = {"image": prov.MET_MUSEUM_DEFAULT_PROVIDER}
    endpoint = "https://collectionapi.metmuseum.org/public/collection/v1/objects"
    DEFAULT_LICENSE_INFO = get_license_info(license_="cc0", license_version="1.0")

    def __init__(self, date: str = None):
        super(MetMuseumDataIngester, self).__init__(date=date)
        self.retries = 5
        self.delayed_requester._DELAY = 3

        self.query_param = None
        if self.date:
            self.query_param = {"metadataDate": date}

        # this seems like useful information to track for context on the existing load
        # metrics, but just adding them to the log in aggregate for now rather than
        # logging each record individually or doing something fancier in airflow.
        self.object_ids_retrieved = 0  # total object IDs based on date
        self.non_cc0_objects = 0  # number checked and ignored because of licensing

    def get_next_query_params(self, prev_query_params=None):
        return self.query_param

    def get_batch_data(self, response_json):
        if response_json:
            self.object_ids_retrieved = response_json["total"]
            logger.info(f"Total objects found {self.object_ids_retrieved}")
            return response_json["objectIDs"]
        else:
            logger.warning("No content available")
            return None

    def get_record_data(self, object_id):
        object_endpoint = f"{self.endpoint}/{object_id}"
        object_json = self.delayed_requester.get_response_json(
            object_endpoint, self.retries
        )

        if object_json.get("isPublicDomain") is False:
            self.non_cc0_objects += 1
            if self.non_cc0_objects % self.batch_limit == 0:
                logger.info(f"Retrieved {self.non_cc0_objects} non-CC0 records.")
            return None

        main_image = object_json.get("primaryImage")
        other_images = object_json.get("additionalImages", [])
        image_list = [main_image] + other_images

        meta_data = self._get_meta_data(object_json)
        raw_tags = self._get_tag_list(object_json)
        title = self._get_title(object_json)
        artist = self._get_artist_name(object_json)

        # We aren't currently populating creator_url. In theory we could url encode
        # f"https://collectionapi.metmuseum.org/public/collection/v1/search?artistOrCulture={artist}"
        # per API guide here: https://metmuseum.github.io/#search
        # but it seems fairly buggy (i.e. nonresponsive), at least when tested with
        # "Chelsea Porcelain Manufactory" and "Minton(s)" and "Jean Pucelle"

        return [
            {
                "foreign_landing_url": object_json.get("objectURL"),
                "image_url": img,
                "license_info": self.DEFAULT_LICENSE_INFO,
                "foreign_identifier": self._get_foreign_id(object_id, img),
                "creator": artist,
                "title": title,
                "meta_data": meta_data,
                "raw_tags": raw_tags,
            }
            for img in image_list
        ]

    def _get_foreign_id(self, object_id: int, image_url: str):
        unique_identifier = image_url.split("/")[-1].split(".")[0]
        return f"{object_id}-{unique_identifier}"

    def _get_meta_data(self, object_json):
        meta_data = None
        if object_json.get("accessionNumber") is not None:
            meta_data = {
                "accession_number": object_json.get("accessionNumber"),
            }
        return meta_data

    def _get_tag_list(self, object_json):
        tag_list = [
            tag
            for tag in [
                object_json.get("department"),
                object_json.get("medium"),
                object_json.get("culture"),
                object_json.get("objectName"),
                self._get_artist_name(object_json),
                object_json.get("classification"),
                object_json.get("objectDate"),
                object_json.get("creditLine"),
                object_json.get("period"),
            ]
            if tag
        ]
        if object_json.get("tags"):
            tag_list += [tag["term"] for tag in object_json.get("tags")]
        return tag_list

    def _get_title(self, record):
        if record.get("title"):
            return record.get("title")
        else:
            return record.get("objectName")

    def _get_artist_name(self, record):
        artist = record.get("artistDisplayName")
        # Treating "unidentified" the same as missing, but maybe it would be useful in
        # in search? Maybe in the art world it has a more specific outsider art
        # connotation?
        if artist in ["Unidentified", "Unidentified artist"]:
            return None
        else:
            return artist

    def get_media_type(self, record):
        # This provider only supports Images.
        return "image"


def main(date: str):
    logger.info("Begin: Metropolitan Museum data ingestion")
    ingester = MetMuseumDataIngester(date)
    ingester.ingest_records()


if __name__ == "__main__":
    mode = "date :"
    parser = argparse.ArgumentParser(
        description="Metropolitan Museum of Art API", add_help=True
    )
    parser.add_argument(
        "--date", help="Fetches all the artwork uploaded after given date"
    )
    args = parser.parse_args()
    if args.date:
        date = args.date

    else:
        date = None
    logger.info("Processing images")

    main(date)
