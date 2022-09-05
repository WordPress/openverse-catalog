import logging
import re
from urllib.parse import parse_qs, urlparse

from airflow.models import Variable
from common.licenses import get_license_info
from common.loader import provider_details as prov
from common.loader.provider_details import ImageCategory
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

NYPL_API = Variable.get("API_KEY_NYPL", default_var=None)
FILETYPE_PATTERN = r" .(jpeg|gif) "


class NyplDataIngester(ProviderDataIngester):
    providers = {"image": prov.NYPL_DEFAULT_PROVIDER}
    headers = {"Authorization": f"Token token={NYPL_API}"}
    endpoint_base = "http://api.repo.nypl.org/api/v1/items"
    endpoint = f"{endpoint_base}/search/"
    metadata_endpoint = f"{endpoint_base}/item_details/"
    batch_limit = 500
    delay = 5
    image_url_dimensions = ["g", "v", "q", "w", "r"]

    def get_next_query_params(self, prev_query_params, **kwargs):
        if not prev_query_params:
            return {
                "q": "CC_0",
                "field": "use_rtxt_s",
                "page": 1,
                "per_page": self.batch_limit,
            }

        else:
            # Increment `skip` by the batch limit.
            return {
                **prev_query_params,
                "page": prev_query_params["page"] + 1,
            }

    def get_media_type(self, record):
        # This provider only supports Images.
        return "image"

    def get_batch_data(self, response_json):
        if response_json:
            return response_json.get("nyplAPI", {}).get("response", {}).get("result")
        return None

    def get_record_data(self, data):
        uuid = data.get("uuid")

        item_json = (
            self.get_response_json({}, endpoint=self.metadata_endpoint + uuid) or {}
        )
        item_details = item_json.get("nyplAPI", {}).get("response")
        if not item_details:
            return None
        mods = item_details.get("mods")

        title_info = mods.get("titleInfo")
        if isinstance(title_info, list) and len(title_info) > 0:
            title_info = title_info[0]
        title = "" if title_info is None else title_info.get("title", {}).get("$")

        name_properties = mods.get("name")
        creator = self._get_creators(name_properties) if name_properties else None

        metadata = self._get_metadata(mods)
        category = (
            ImageCategory.PHOTOGRAPH.value
            if metadata.get("genre") == "Photographs"
            else None
        )

        captures = item_details.get("sibling_captures", {}).get("capture")
        if not captures:
            return None
        if not isinstance(captures, list):
            captures = [captures]
        images = []
        for capture in captures:
            image_id = capture.get("imageID", {}).get("$")
            if image_id is None:
                continue
            image_link = capture.get("imageLinks", {}).get("imageLink", [])
            image_url, filetype = self._get_image_data(image_link)
            if not image_url:
                continue
            foreign_landing_url = capture.get("itemLink", {}).get("$")
            license_url = capture.get("rightsStatementURI", {}).get("$")
            if not foreign_landing_url or license_url is None:
                continue

            image_data = {
                "foreign_identifier": image_id,
                "foreign_landing_url": foreign_landing_url,
                "image_url": image_url,
                "license_info": get_license_info(license_url=license_url),
                "title": title,
                "creator": creator,
                "filetype": filetype,
                "category": category,
                "meta_data": metadata,
            }
            images.append(image_data)
        return images or None

    @staticmethod
    def _get_filetype(description: str):
        """
        Extracts the filetype from a description string like:
        "Cropped .jpeg (1600 pixels on the long side)"
        :param description: the description string
        :return:  jpeg | gif
        """
        if match := re.search(FILETYPE_PATTERN, description):
            return match.group(1)
        return None

    @staticmethod
    def _get_image_data(images):
        """
        Gets a list of dictionaries of the following shape:
        {
          "$": "http://images.nypl.org/index.php?id=56738467&t=q&download=1
        &suffix=29eed1f0-3d50-0134-c4c7-00505686a51c.001",
          "description": "Cropped .jpeg (1600 pixels on the long side)"
        }
        Extracts the largest image based on the `t` query parameter
        and IMAGE_URL_DIMENSIONS.
        """

        image_type = {
            parse_qs(urlparse(img["$"]).query)["t"][0]: {
                "url": img["$"],
                "description": img["description"],
            }
            for img in images
        }
        if image_type == {}:
            return None, None
        preferred_image = (
            (
                image_type[dimension]["url"].replace("&download=1", ""),
                NyplDataIngester._get_filetype(image_type[dimension]["description"]),
            )
            for dimension in NyplDataIngester.image_url_dimensions
            if dimension in image_type
        )
        image_url, filetype = next(preferred_image, None)
        image_url = image_url.replace("http://", "https://")
        return image_url, filetype

    @staticmethod
    def _get_creators(creatorinfo):
        if isinstance(creatorinfo, list):
            primary_creator = (
                info.get("namePart", {}).get("$")
                for info in creatorinfo
                if info.get("usage") == "primary"
            )
            creator = next(primary_creator, None)
        else:
            creator = creatorinfo.get("namePart", {}).get("$")

        return creator

    @staticmethod
    def _get_metadata(mods):
        metadata = {}

        type_of_resource = mods.get("typeOfResource")
        if isinstance(type_of_resource, list) and (
            type_of_resource[0].get("usage") == "primary"
        ):
            metadata["type_of_resource"] = type_of_resource[0].get("$")

        if isinstance(mods.get("genre"), dict):
            metadata["genre"] = mods.get("genre").get("$")

        origin_info = mods.get("originInfo", {})
        if date_issued := origin_info.get("dateIssued", {}).get("$"):
            metadata["date_issued"] = date_issued
        if date_created := origin_info.get("dateCreated", {}):
            if isinstance(date_created, list):
                # Approximate dates have a start and an end
                # [{'encoding': 'w3cdtf', 'keyDate': 'yes',
                # 'point': 'start', 'qualifier': 'approximate', '$': '1990'},
                # {'encoding': 'w3cdtf', 'point': 'end',
                # 'qualifier': 'approximate', '$': '1999'}]
                start_date = next(
                    d.get("$") for d in date_created if d.get("point") == "start"
                )
                end_date = next(
                    d.get("$") for d in date_created if d.get("point") == "end"
                )
                if start_date and end_date:
                    date_created = f"{start_date}-{end_date}"
                elif start_date:
                    date_created = start_date
                else:
                    date_created = None
            else:
                date_created = date_created.get("$")
            metadata["date_created"] = date_created
        if publisher := origin_info.get("publisher", {}).get("$"):
            metadata["publisher"] = publisher

        if description := mods.get("physicalDescription", {}).get("note", {}).get("$"):
            metadata["physical_description"] = description

        subject_list = mods.get("subject", [])
        if isinstance(subject_list, dict):
            subject_list = [subject_list]
        topics = [
            subject["topic"].get("$")
            for subject in subject_list
            if "topic" in subject and subject["topic"].get("$")
        ]
        if topics:
            metadata["topics"] = ", ".join(topics)

        return metadata


def main():
    logger.info("Begin: NYPL data ingestion")
    ingester = NyplDataIngester()
    ingester.ingest_records()


if __name__ == "__main__":
    main()
