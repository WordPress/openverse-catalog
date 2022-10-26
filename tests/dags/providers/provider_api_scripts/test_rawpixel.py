import json
from pathlib import Path

import pytest
from common.licenses import LicenseInfo
from common.loader.provider_details import ImageCategory
from providers.provider_api_scripts.rawpixel import RawpixelDataIngester


_license_info = (
    "cc0",
    "1.0",
    "https://creativecommons.org/publicdomain/zero/1.0/",
    None,
)
license_info = LicenseInfo(*_license_info)
rwp = RawpixelDataIngester()
rwp.api_key = "PREDICTABLE-KEY"

RESOURCES = Path(__file__).parent.resolve() / "resources/rawpixel"


def _get_resource_json(json_name):
    with (RESOURCES / json_name).open() as f:
        resource_json = json.load(f)
    return resource_json


@pytest.mark.parametrize(
    "query_params, expected",
    [
        # Empty params
        ({}, "j5VDmEme7JqzMkKAxNfjWb6EaVtIpLq4N2QnYIHZvWg"),
        # One item
        ({"foo": "bar"}, "ZenXVF0pAhfm9EzlAsvw-REsQ27nQQ5mtxmSu4upmHo"),
        # Multiple items
        (
            {"foo": "bar", "crimothy": "roberts"},
            "rSz4Ou1ZZFY57z5Ff7AHxZqwZW_PsgOsN9ksTmpbWIM",
        ),
        # Multiple items of different types
        ({"foo": "bar", "dogs": 12}, "qWEHU7OsSfSFcNsqS9OkHWMDWe_33DBxMR9ULOLrLSw"),
        (
            {"foo": "bar", "sentence": "to+be+or+not+to+be"},
            "aJccI57xaj_pH_xUcD208ZKO_lWne0c2KsjSO15qI-I",
        ),
        (
            {"foo": "bar", "sentence": "to be or not to be"},
            "jbW0P2Oi2LL-BLvRsGAydF7VGlFOvWQFMSbkJFX6LQo",
        ),
        # Multiple items with list
        (
            {"foo": "bar", "favorites": ["chocolate", "video games", "cats"]},
            "FM_kVUym-GonOgfZAeNuswEQLZas3BOOvkTXvax_mTw",
        ),
    ],
)
def test_get_signature(query_params, expected):
    actual = rwp._get_signature(query_params)
    assert actual == expected


def test_get_next_query_params_empty():
    actual = rwp.get_next_query_params({})
    actual.pop("s")
    assert actual == {
        "tags": "$publicdomain",
        "page": 1,
        "pagesize": 100,
    }


def test_get_next_query_params_increments_page_size():
    actual = rwp.get_next_query_params({"foo": "bar", "page": 5})
    actual.pop("s")
    assert actual == {"foo": "bar", "page": 6}


@pytest.mark.parametrize(
    "data, expected",
    [
        # No data
        ({}, None),
        # Missing filter passes through
        ({"style_uri": "nothing_to_format"}, "nothing_to_format"),
        # Happy path
        (
            {"style_uri": "string_to_format: {}"},
            f"string_to_format: {rwp.full_size_option}",
        ),
    ],
)
def test_get_image_url(data, expected):
    actual = rwp._get_image_url(data, rwp.full_size_option)
    assert actual == expected


@pytest.mark.parametrize(
    "data, expected",
    [
        # Empty case
        ({}, (None, None)),
        # Happy path normal fields
        ({"width": 500, "height": 600}, (500, 600)),
        # Happy path display image fields
        ({"display_image_width": 500, "display_image_height": 600}, (500, 600)),
        # One field is 0 but data is still present
        ({"width": 0, "height": 600}, (0, 600)),
        # Fields conflict so max is chosen
        ({"width": 500, "display_image_width": 700, "height": 600}, (700, 600)),
    ],
)
def test_get_image_properties(data, expected):
    actual = rwp._get_image_properties(data)
    assert actual == expected


@pytest.mark.parametrize(
    "title, expected",
    [
        (
            "Bull elk searches for food | Free Photo - rawpixel",
            "Bull elk searches for food",
        ),
        (
            "Desktop wallpaper summer beach landscape, | Free Photo - rawpixel",
            "Desktop wallpaper summer beach landscape",
        ),
        (
            "Japanese autumn tree color drawing. | Free Photo - rawpixel",
            "Japanese autumn tree color drawing",
        ),
        (
            "Open hand, palm reading. Original | Free Photo Illustration - rawpixel",
            "Open hand, palm reading",
        ),
        (
            "Claude Monet's The Magpie (1868&ndash;1869) | Free Photo Illustration - rawpixel",
            "Claude Monet's The Magpie (1868–1869)",
        ),
        ("Red poppy field. Free public | Free Photo - rawpixel", "Red poppy field"),
        ("Free public domain CC0 photo. | Free Photo - rawpixel", None),
        (
            "Floral glasses. Free public domain | Free Photo - rawpixel",
            "Floral glasses",
        ),
        (
            "Claude Monet's The Cliffs at &Eacute;tretat | Free Photo Illustration - rawpixel",
            "Claude Monet's The Cliffs at Étretat",
        ),
    ],
)
def test_get_title(title, expected):
    metadata = {"title": title}
    actual = rwp._get_title(metadata)
    assert actual == expected


@pytest.mark.parametrize(
    "data, expected",
    [
        # Empty cases
        ({}, None),
        ({"artist_names": ""}, None),
        # Name without suffix
        ({"artist_names": "Monet"}, "Monet"),
        # Name with spaces
        ({"artist_names": "   Monet    "}, "Monet"),
        # Name with suffix
        ({"artist_names": "Monet (Source)"}, "Monet"),
    ],
)
def test_get_source(data, expected):
    actual = rwp._get_creator(data)
    assert actual == expected


EXAMPLE_KEYWORDS = [
    # Digitized art
    [
        "flower",
        "vintage",
        "public domain",
        "ornament",
        "vintage illustrations",
        "floral",
        "public domain art",
        "vintage flowers",
        "illustration",
        "vintage illustration public domain",
        "victorian",
        "flowers public domain",
    ],
    # Illustration
    [
        "orange",
        "tiger",
        "tiger face",
        "bengal tiger",
        "animal illustrations",
        "public domain tiger",
        "clipart",
        "face",
        "wildlife",
        "clip art public domain",
        "animal art",
        "public domain animal",
    ],
    # Photograph
    [
        "dog",
        "public domain dog",
        "animal photos",
        "public domain",
        "public domain fashion",
        "animal",
        "photo",
        "hat dog",
        "image",
        "fashion dog",
        "beagle",
        "animals public domain",
    ],
]


@pytest.mark.parametrize(
    "keywords, expected",
    zip(
        EXAMPLE_KEYWORDS,
        (
            [
                "flower",
                "vintage",
                "ornament",
                "vintage illustrations",
                "floral",
                "vintage flowers",
                "illustration",
                "victorian",
            ],
            [
                "orange",
                "tiger",
                "tiger face",
                "bengal tiger",
                "animal illustrations",
                "clipart",
                "face",
                "wildlife",
                "animal art",
            ],
            [
                "dog",
                "animal photos",
                "animal",
                "photo",
                "hat dog",
                "image",
                "fashion dog",
                "beagle",
            ],
        ),
    ),
)
def test_get_tags(keywords, expected):
    actual = rwp._get_tags({"popular_keywords": keywords})
    assert actual == expected


@pytest.mark.parametrize(
    "keywords, expected",
    zip(
        [*EXAMPLE_KEYWORDS, []],
        [
            ImageCategory.DIGITIZED_ARTWORK.value,
            ImageCategory.ILLUSTRATION.value,
            ImageCategory.PHOTOGRAPH.value,
            None,
        ],
    ),
)
def test_get_category(keywords, expected):
    actual = rwp._get_category({"popular_keywords": keywords})
    assert actual == expected


def test_get_record_data():
    data = _get_resource_json("public_domain_response.json")
    actual = rwp.get_record_data(data["results"][0])
    assert actual == {
        "category": None,
        "creator": "National Park Service",
        "filetype": "jpg",
        "foreign_identifier": 4032668,
        "foreign_landing_url": "https://www.rawpixel.com/image/4032668/photo-image-background-nature-mountain",  # noqa
        "height": 5515,
        "image_url": "https://images.rawpixel.com/image_1300/cHJpdmF0ZS9sci9pbWFnZXMvd2Vic2l0ZS8yMDIyLTA1L2ZsNDY0NDU5OTQ2MjQtaW1hZ2Uta3UyY21zcjUuanBn.jpg",  # noqa
        "license_info": LicenseInfo(
            license="cc0",
            version="1.0",
            url="https://creativecommons.org/publicdomain/zero/1.0/",
            raw_url="https://creativecommons.org/publicdomain/zero/1.0/",
        ),
        "meta_data": {
            "description": "Bull elk searches for food beneath the snow. Frank. Original public domain image from Flickr",  # noqa
            "download_count": 0,
        },
        "raw_tags": [
            "animal",
            "deer",
            "winter",
            "snow",
            "background",
            "sunset",
            "nature background",
            "elk",
            "national park",
            "wildlife",
            "yellowstone",
        ],
        "title": "Bull elk searches for food",
        "width": 8272,
    }
