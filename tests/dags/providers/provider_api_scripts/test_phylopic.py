import json
from functools import partial
from pathlib import Path
from unittest.mock import patch

import pytest
from common.licenses import LicenseInfo
from providers.provider_api_scripts.phylopic import PhylopicDataIngester


RESOURCES = Path(__file__).parent / "resources/phylopic"
pp = PhylopicDataIngester()


@pytest.fixture
def image_data():
    yield get_json("correct_meta_data_example.json")


def get_json(filename):
    with open(RESOURCES / filename) as f:
        return json.load(f)


invalid_endpoint = partial(pytest.param, marks=pytest.mark.raises(exception=ValueError))


@pytest.mark.parametrize(
    "kwargs, expected_kwargs",
    [
        # Empty params
        ({"query_params": {}}, {"query_params": {}, "endpoint": None}),
        # Empty params & falsy endpoint
        ({"query_params": {}, "endpoint": ""}, {"query_params": {}, "endpoint": None}),
        (
            {"query_params": {}, "endpoint": None},
            {"query_params": {}, "endpoint": None},
        ),
        # Query params with no endpoint
        (
            {"query_params": {"foo": "bar"}},
            {"query_params": {"foo": "bar"}, "endpoint": None},
        ),
        # Query params with endpoint defined
        (
            {"query_params": {"foo": "bar", "endpoint": "special-endpoint"}},
            {"query_params": {"foo": "bar"}, "endpoint": "special-endpoint"},
        ),
        # Endpoint kwarg supersedes endpoint query param
        (
            {
                "query_params": {"foo": "bar", "endpoint": "gets-ignored"},
                "endpoint": "is-actually-used",
            },
            {"query_params": {"foo": "bar"}, "endpoint": "is-actually-used"},
        ),
    ],
)
def test_get_response_json_override(kwargs, expected_kwargs):
    with patch.object(
        PhylopicDataIngester.__bases__[0], "get_response_json"
    ) as mock_response_json:
        query_params_before = kwargs.get("query_params").copy()
        pp.get_response_json(**kwargs)
        actual_kwargs = mock_response_json.call_args.kwargs
        assert actual_kwargs == expected_kwargs
        # query params should not change as part of the call
        assert kwargs.get("query_params") == query_params_before


@pytest.fixture
def ingester() -> PhylopicDataIngester:
    _pp = PhylopicDataIngester()
    offset = _pp.offset
    batch_limit = _pp.batch_limit
    yield _pp
    _pp.offset = offset
    _pp.batch_limit = batch_limit


@pytest.mark.parametrize(
    "offset, limit, prev_query_params, non_dated_endpoint",
    [
        # Basic limit/offset changes
        (0, 25, False, "http://phylopic.org/api/a/image/list/0/25"),
        (0, 30, False, "http://phylopic.org/api/a/image/list/0/30"),
        (60, 30, False, "http://phylopic.org/api/a/image/list/60/30"),
        # Existence of previous query params increments offset
        (0, 25, True, "http://phylopic.org/api/a/image/list/25/25"),
        (60, 30, True, "http://phylopic.org/api/a/image/list/90/30"),
    ],
)
@pytest.mark.parametrize(
    "date, dated_endpoint",
    [
        # Defer to non-dated case
        (None, None),
        # Dated DAG produces specific endpoint irrespective of everything else
        (
            "2022-01-01",
            "http://phylopic.org/api/a/image/list/modified/2022-01-01/2022-01-08",
        ),
    ],
)
def test_get_next_query_params(
    offset,
    limit,
    prev_query_params,
    non_dated_endpoint,
    date,
    dated_endpoint,
    ingester,
):
    # Defer to dated endpoint if provided
    expected_endpoint = dated_endpoint or non_dated_endpoint
    ingester.date = date
    ingester.offset = offset
    ingester.batch_limit = limit
    actual = ingester.get_next_query_params(prev_query_params)
    assert actual["endpoint"] == expected_endpoint


@pytest.mark.parametrize("dated", [True, False])
def test_get_should_continue(dated, ingester):
    ingester.date = dated
    actual = ingester.get_should_continue({})
    # Should continue only if we're running as non-dated
    assert actual == (not dated)


@pytest.mark.parametrize(
    "response_json, expected",
    [
        # Empty cases
        (None, None),
        ({}, None),
        ({"other": "yes"}, None),
        # Failure
        ({"success": False}, None),
        # Success, but no results
        ({"success": True}, None),
        # Success with results
        ({"success": True, "result": 123}, 123),
    ],
)
def test_get_response_data(response_json, expected):
    actual = pp._get_response_data(response_json)
    assert actual == expected


def test_get_batch_data():
    r = get_json("image_ids_example.json")
    actual_img_ids = [data.get("uid") for data in pp.get_batch_data(r)]
    expect_img_ids = [
        "863694ac-9f36-40f5-9452-1b435337d9cc",
        "329ff574-4bec-4f94-9dd6-9acfec2a6275",
        "9c98ff56-8044-483e-b9f1-bf368e4f3322",
    ]
    assert actual_img_ids == expect_img_ids


def test_get_meta_data_with_no_img_url():
    r = get_json("no_image_url_example.json")
    meta_data = pp._get_meta_data(r["result"])
    assert meta_data is None


def test_get_creator_details(image_data):
    result = image_data["result"]
    actual_creator_details = pp._get_creator_details(result)
    expect_creator_details = (
        "Jonathan Wells",
        "Jonathan Wells",
        "2020-02-26 11:59:53",
    )
    assert actual_creator_details == expect_creator_details


def test_get_taxa_details(image_data):
    result = image_data["result"]
    actual_taxa = pp._get_taxa_details(result)
    expect_taxa = (
        ["Chondrus crispus NODC Taxonomic Code, database (version 8.0) 1996"],
        "Chondrus crispus NODC Taxonomic Code, database (version 8.0) 1996",
    )
    assert actual_taxa == expect_taxa


def test_get_record_data(image_data):
    image_uuid = "e9df48fe-68ea-419e-b9df-441e0b208335"
    expected = {
        "foreign_identifier": image_uuid,
        "foreign_landing_url": f"http://phylopic.org/image/{image_uuid}",
        "width": 847,
        "height": 1024,
        "creator": "Jonathan Wells",
        "title": "Chondrus crispus NODC Taxonomic Code, database (version 8.0) 1996",
        "image_url": "http://phylopic.org/assets/images/submissions/e9df48fe-68ea-419e-b9df-441e0b208335.1024.png",
        "license_info": LicenseInfo(
            license="cc0",
            version="1.0",
            url="https://creativecommons.org/publicdomain/zero/1.0/",
            raw_url="http://creativecommons.org/publicdomain/zero/1.0/",
        ),
        "meta_data": {
            "taxa": [
                "Chondrus crispus NODC Taxonomic Code, database (version 8.0) 1996"
            ],
            "credit_line": "Jonathan Wells",
            "pub_date": "2020-02-26 11:59:53",
        },
    }
    with patch.object(pp, "get_response_json", return_value=image_data):
        actual = pp.get_record_data({"uid": image_uuid})
    assert actual == expected


def test_get_record_data_no_data():
    actual = pp.get_record_data({})
    assert actual is None


def test_get_image_info(image_data):
    result = image_data["result"]
    actual_img_info = pp._get_image_info(result, "e9df48fe-68ea-419e-b9df-441e0b208335")
    expect_img_info = (
        (
            "http://phylopic.org/assets/images/submissions/e9df48fe-68ea-"
            "419e-b9df-441e0b208335.1024.png"
        ),
        847,
        1024,
    )
    assert actual_img_info == expect_img_info


def test_get_image_info_with_no_img_url():
    r = get_json("no_image_url_example.json")
    result = r["result"]
    actual_img_info = list(
        pp._get_image_info(result, "7f7431c6-8f78-498b-92e2-ebf8882a8923")
    )
    expect_img_info = [None, None, None]
    assert actual_img_info == expect_img_info


@pytest.mark.parametrize(
    "date_start, days, expected",
    [
        ("2022-01-10", 10, "2022-01-20"),
        ("2022-02-28", 7, "2022-03-07"),
        ("2022-02-28", -7, "2022-02-21"),
    ],
)
def test_compute_date_range(date_start, days, expected):
    actual = pp._compute_date_range(date_start, days)
    assert actual == expected
