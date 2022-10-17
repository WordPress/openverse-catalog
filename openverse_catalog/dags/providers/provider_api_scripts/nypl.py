import logging
import re
from urllib.parse import parse_qs, urlparse

from airflow.models import Variable
from common import constants
from common.licenses import get_license_info
from common.loader import provider_details as prov
from common.loader.provider_details import ImageCategory
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logger = logging.getLogger(__name__)


class NyplDataIngester(ProviderDataIngester):
    providers = {"image": prov.NYPL_DEFAULT_PROVIDER}
    NYPL_API = Variable.get("API_KEY_NYPL")
    headers = {"Authorization": f"Token token={NYPL_API}"}
    endpoint_base = "http://api.repo.nypl.org/api/v1/items"
    endpoint = f"{endpoint_base}/search/"
    metadata_endpoint = f"{endpoint_base}/item_details/"
    batch_limit = 500
    delay = 5
    # NYPL returns a list of image objects, with the dimension encoded
    # in the URL's query parameter.
    # This list is in order from the largest image to the smallest one.
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
        return constants.IMAGE

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
        if isinstance(title_info, list) and title_info:
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
        return images

    @staticmethod
    def _get_filetype(description: str):
        """
        Extracts the filetype from a description string like:
        "Cropped .jpeg (1600 pixels on the long side)"
        This is required because the filetype is not present/extractable from the
        url via the MediaStore class.
        :param description: the description string
        :return:  jpeg | gif
        """
        if match := re.search(r" .(jpeg|gif) ", description):
            return match.group(1)
        return None

    @staticmethod
    def _get_image_data(images) -> tuple[None, None] | tuple[str, str]:
        """
        Receives a list of dictionaries of the following shape:
        {
          "$": "http://images.nypl.org/index.php?id=56738467&t=q&download=1
        &suffix=29eed1f0-3d50-0134-c4c7-00505686a51c.001",
          "description": "Cropped .jpeg (1600 pixels on the long side)"
        }
        Selects the largest image based on the image URL's `t` query parameter
        and IMAGE_URL_DIMENSIONS.
        """
        # Create a dict with the NyplDataIngester.image_url_dimensions as keys,
        # and image data as value.
        image_types = {
            parse_qs(urlparse(img["$"]).query)["t"][0]: i
            for i, img in enumerate(images)
        }
        if not image_types:
            return None, None

        # Select the dict with the largest image
        for dimension in NyplDataIngester.image_url_dimensions:
            if (preferred_image_index := image_types.get(dimension)) is not None:
                preferred_image = images[preferred_image_index]

                image_url = preferred_image["$"].replace("&download=1", "")
                filetype = NyplDataIngester._get_filetype(
                    preferred_image["description"]
                )
                return image_url, filetype

        return None, None

    @staticmethod
    def _get_creators(creatorinfo):
        if not isinstance(creatorinfo, list):
            creatorinfo = [creatorinfo]
        primary_creator = (
            info.get("namePart", {}).get("$")
            for info in creatorinfo
            if info.get("usage") == "primary"
        )
        creator = next(primary_creator, None)

        return creator

    @staticmethod
    def get_value_from_dict_or_list(
        dict_or_list: dict | list, key: str
    ) -> dict | list | str | None:
        """
        If dict_or_list is a list, returns the value from the first
        dictionary in the list.
        If it is a dict, returns the value from the dict.
        """
        if isinstance(dict_or_list, list):
            for item in dict_or_list:
                if key in item:
                    return item[key]
        else:
            return dict_or_list.get(key)

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
        if date_issued := NyplDataIngester.get_value_from_dict_or_list(
            origin_info, "dateIssued"
        ):
            metadata["date_issued"] = date_issued
        if date_created_object := NyplDataIngester.get_value_from_dict_or_list(
            origin_info, "dateCreated"
        ):
            if isinstance(date_created_object, dict):
                if date_created := date_created_object.get("$"):
                    metadata["date_created"] = date_created
            elif isinstance(date_created_object, list):
                logger.info(f"date_created: {date_created_object}")
                # Approximate dates have a start and an end
                # [{'encoding': 'w3cdtf', 'keyDate': 'yes',
                # 'point': 'start', 'qualifier': 'approximate', '$': '1990'},
                # {'encoding': 'w3cdtf', 'point': 'end',
                # 'qualifier': 'approximate', '$': '1999'}]
                start, end = None, None
                for item in date_created_object:
                    point = item.get("point")
                    if point == "start":
                        start = item.get("$")
                    elif point == "end":
                        end = item.get("$")
                if start:
                    metadata["date_created"] = f"{start}{f'-{end}' if end else ''}"

        if publisher := NyplDataIngester.get_value_from_dict_or_list(
            origin_info, "publisher"
        ):
            metadata["publisher"] = publisher

        physical_description = NyplDataIngester.get_value_from_dict_or_list(
            mods, "physicalDescription"
        )
        if physical_description:
            note = NyplDataIngester.get_value_from_dict_or_list(
                physical_description, "note"
            )

            if note and (description := note.get("$")):
                metadata["physical_description"] = description
                logger.info(f"Added description: {description}")

        subject_list = mods.get("subject", [])
        if isinstance(subject_list, dict):
            subject_list = [subject_list]
        # Topic can be a dictionary or a list
        topics = [subject["topic"] for subject in subject_list if "topic" in subject]
        if topics:
            tags = []
            for topic in topics:
                if isinstance(topic, list):
                    tags.extend([t.get("$") for t in topic])
                else:
                    tags.append(topic.get("$"))
            if tags:
                metadata["tags"] = ", ".join(tags)

        return metadata


def main():
    logger.info("Begin: NYPL data ingestion")
    ingester = NyplDataIngester()
    ingester.ingest_records()


if __name__ == "__main__":
    main()
