from common.licenses import LicenseInfo
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


_license_info = (
    "cc0",
    "1.0",
    "https://creativecommons.org/publicdomain/zero/1.0/",
    None,
)
LICENSE_INFO = LicenseInfo(*_license_info)
AUDIO_PROVIDER = "mock_audio_provider"
IMAGE_PROVIDER = "mock_image_provider"
ENDPOINT = "http://mock-api/endpoint"
DELAY = 5
RETRIES = 3
BATCH_LIMIT = 1000
HEADERS = {"api_key": "mock_api_key"}
DEFAULT_QUERY_PARAMS = {"has_image": 1, "page": 1}


class MockProviderDataIngester(ProviderDataIngester):
    """
    A very simple concrete implementation of the ProviderDataIngester class,
    for testing purposes.
    """

    def __init__(
        self,
        providers={"audio": AUDIO_PROVIDER, "image": IMAGE_PROVIDER},
        endpoint=ENDPOINT,
        delay=DELAY,
        batch_limit=BATCH_LIMIT,
        retries=RETRIES,
        headers=HEADERS,
    ):
        super().__init__(providers, endpoint, batch_limit, delay, retries, headers)

    def get_next_query_params(self, old_query_params):
        return DEFAULT_QUERY_PARAMS

    def get_media_type(self, record):
        return record["media_type"]

    def get_record_data(self, record):
        data = {
            "foreign_identifier": record["id"],
            "foreign_landing_url": record["url"],
            "license_info": LICENSE_INFO,
        }
        if record["media_type"] == "audio":
            data["audio_url"] = record["audio_url"]
        elif record["media_type"] == "image":
            data["image_url"] = record["image_url"]
        return data


# Expected result of calling `get_batch_data` with `response_success.json`
EXPECTED_BATCH_DATA = [
    {
        "id": 100,
        "media_type": "image",
        "title": "Title 100",
        "image_url": "https://openaccess-cdn.clevelandart.org/1916.586.a/1916.586.a_web.jpg",  # noqa: E501
        "url": "https://clevelandart.org/art/1916.586.a",
    },
    {
        "id": 101,
        "media_type": "audio",
        "title": "Title 101",
        "audio_url": "https://openaccess-cdn.clevelandart.org/1335.1917/1335.1917_web.jpg",  # noqa: E501
        "url": "https://clevelandart.org/art/1335.1917",
    },
    {
        "id": 102,
        "media_type": "image",
        "title": "Title 102",
        "image_url": "https://openaccess-cdn.clevelandart.org/1915.534/1915.534_web.jpg",  # noqa: E501
        "url": "https://clevelandart.org/art/1915.534",
    },
]

# Expected result of calling `get_record_data` with `audio_record.json`
EXPECTED_RECORD_DATA_AUDIO = {
    "foreign_identifier": 101,
    "foreign_landing_url": "https://clevelandart.org/art/1335.1917",
    "license_info": LICENSE_INFO,
    "audio_url": "https://openaccess-cdn.clevelandart.org/1335.1917/1335.1917_web.jpg",
}

# Expected result of calling `get_record_data` with `image_record.json`
EXPECTED_RECORD_DATA_IMAGE = {
    "foreign_identifier": 100,
    "foreign_landing_url": "https://clevelandart.org/art/1916.586.a",
    "license_info": LICENSE_INFO,
    "image_url": "https://openaccess-cdn.clevelandart.org/1916.586.a/1916.586.a_web.jpg",  # noqa: E501
}
