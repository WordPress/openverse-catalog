import json
import logging
import os
from datetime import datetime, timedelta, timezone
from itertools import repeat
from unittest.mock import MagicMock, patch

import pytest
from common.licenses import LicenseInfo
from common.loader import provider_details as prov
from common.storage.image import ImageStore
from providers.provider_api_scripts.finnish_museums import FinnishMuseumsDataIngester


RESOURCES = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), "resources/finnishmuseums"
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.DEBUG
)

FROZEN_DATE = "2020-04-01"
FROZEN_UTC_DATE = datetime.strptime(FROZEN_DATE, "%Y-%m-%d").replace(
    tzinfo=timezone.utc
)
fm = FinnishMuseumsDataIngester(date=FROZEN_DATE)
image_store = ImageStore(provider=prov.FINNISH_DEFAULT_PROVIDER)
fm.media_stores = {"image": image_store}


def _get_resource_json(json_name):
    with open(os.path.join(RESOURCES, json_name)) as f:
        resource_json = json.load(f)
    return resource_json


# TODO Tests:
# 1. Test that get_timestamp_pairs returns empty list if no records
# 2. Test get_record_count itself
# 3. Test get_timestamp_pairs returns one pair for whole day if <10k
# 4. Test get_timestamp_pairs with several hours that have no records, and at
#    least one hour with more than 100k hours and one with less than 100k
# 5. Test that get_batch_data quits early if we consume more than the total
# 6. Better integration test, call ingest_records and test that it quits early
#    if we exceed the total_records. That's a lot to mock
# 7. Test that ingest_records resets counts between buildings


def test_get_record_count():
    pass


def test_get_timestamp_pairs_returns_empty_list_when_no_records_found():
    # mock _get_record_count to return 0 records
    with patch.object(fm, "_get_record_count", return_value=0):
        actual_pairs_list = fm._get_timestamp_pairs("test_building")

        assert len(actual_pairs_list) == 0


@pytest.mark.parametrize(
    "num_divisions, expected_pairs",
    [
        # One division
        (1, [(datetime(2020, 4, 1, 1, 0), datetime(2020, 4, 1, 2, 0))]),
        # Two divisions divides into 30 min increments
        (
            2,
            [
                (datetime(2020, 4, 1, 1, 0), datetime(2020, 4, 1, 1, 30)),
                (datetime(2020, 4, 1, 1, 30), datetime(2020, 4, 1, 2, 0)),
            ],
        ),
        # 8 divisions divides into 7.5 min increments
        (
            8,
            [
                (datetime(2020, 4, 1, 1, 0, 0), datetime(2020, 4, 1, 1, 7, 30)),
                (datetime(2020, 4, 1, 1, 7, 30), datetime(2020, 4, 1, 1, 15, 0)),
                (datetime(2020, 4, 1, 1, 15, 0), datetime(2020, 4, 1, 1, 22, 30)),
                (datetime(2020, 4, 1, 1, 22, 30), datetime(2020, 4, 1, 1, 30, 0)),
                (datetime(2020, 4, 1, 1, 30, 0), datetime(2020, 4, 1, 1, 37, 30)),
                (datetime(2020, 4, 1, 1, 37, 30), datetime(2020, 4, 1, 1, 45, 0)),
                (datetime(2020, 4, 1, 1, 45, 0), datetime(2020, 4, 1, 1, 52, 30)),
                (datetime(2020, 4, 1, 1, 52, 30), datetime(2020, 4, 1, 2, 0, 0)),
            ],
        ),
        # 7 does not divide evenly into 60, so error is raised
        pytest.param(7, None, marks=pytest.mark.raises(exception=ValueError)),
    ],
)
def test_get_timestamp_query_params_list(num_divisions, expected_pairs):
    # Hour long time slice
    start_date = datetime(2020, 4, 1, 1, 0)
    end_date = datetime(2020, 4, 1, 2, 0)

    actual_pairs = fm._get_timestamp_query_params_list(
        start_date, end_date, num_divisions
    )
    assert actual_pairs == expected_pairs


def test_get_timestamp_pairs_returns_full_day_when_few_records_found():
    # When < 10_000 records are found for the day, we should get back a single
    # timestamp pair representing the whole day
    expected_pairs_list = [
        (
            datetime(2020, 4, 1, 0, 0, tzinfo=timezone.utc),
            datetime(2020, 4, 2, 0, 0, tzinfo=timezone.utc),
        )
    ]

    with patch.object(fm, "_get_record_count", return_value=9999):
        actual_pairs_list = fm._get_timestamp_pairs("test_building")
        assert actual_pairs_list == expected_pairs_list


def test_get_timestamp_pairs_with_large_record_counts():
    with patch.object(fm, "_get_record_count") as mock_count:
        # Mock the calls to _get_record_count in order
        mock_count.side_effect = [
            150_000,  # Getting total count for the entire day
            0,  # Count for first hour, should be ommitted
            10,  # Count for second hour, should have one slice
            101_000,  # Count for third hour, should be split into 3min intervals
            49_090,  # Count for fourth hour, should be split into 5min intervals
        ] + list(
            repeat(0, 20)
        )  # fill list with 0 for the remaining hours

        # We only get timestamp pairs for the hours that had records. For the
        # hour with > 100k records, we get 20 3-min pairs. For the hour with
        # < 100k records, we get 12 5-min pairs.
        expected_pairs_list = [
            # Single intervals for second hour
            ("2020-04-01T01:00:00Z", "2020-04-01T02:00:00Z"),
            # 20 3-min intervals across the third hour
            ("2020-04-01T02:00:00Z", "2020-04-01T02:03:00Z"),
            ("2020-04-01T02:03:00Z", "2020-04-01T02:06:00Z"),
            ("2020-04-01T02:06:00Z", "2020-04-01T02:09:00Z"),
            ("2020-04-01T02:09:00Z", "2020-04-01T02:12:00Z"),
            ("2020-04-01T02:12:00Z", "2020-04-01T02:15:00Z"),
            ("2020-04-01T02:15:00Z", "2020-04-01T02:18:00Z"),
            ("2020-04-01T02:18:00Z", "2020-04-01T02:21:00Z"),
            ("2020-04-01T02:21:00Z", "2020-04-01T02:24:00Z"),
            ("2020-04-01T02:24:00Z", "2020-04-01T02:27:00Z"),
            ("2020-04-01T02:27:00Z", "2020-04-01T02:30:00Z"),
            ("2020-04-01T02:30:00Z", "2020-04-01T02:33:00Z"),
            ("2020-04-01T02:33:00Z", "2020-04-01T02:36:00Z"),
            ("2020-04-01T02:36:00Z", "2020-04-01T02:39:00Z"),
            ("2020-04-01T02:39:00Z", "2020-04-01T02:42:00Z"),
            ("2020-04-01T02:42:00Z", "2020-04-01T02:45:00Z"),
            ("2020-04-01T02:45:00Z", "2020-04-01T02:48:00Z"),
            ("2020-04-01T02:48:00Z", "2020-04-01T02:51:00Z"),
            ("2020-04-01T02:51:00Z", "2020-04-01T02:54:00Z"),
            ("2020-04-01T02:54:00Z", "2020-04-01T02:57:00Z"),
            ("2020-04-01T02:57:00Z", "2020-04-01T03:00:00Z"),
            # 12 5-min intervals across the fourth hour
            ("2020-04-01T03:00:00Z", "2020-04-01T03:05:00Z"),
            ("2020-04-01T03:05:00Z", "2020-04-01T03:10:00Z"),
            ("2020-04-01T03:10:00Z", "2020-04-01T03:15:00Z"),
            ("2020-04-01T03:15:00Z", "2020-04-01T03:20:00Z"),
            ("2020-04-01T03:20:00Z", "2020-04-01T03:25:00Z"),
            ("2020-04-01T03:25:00Z", "2020-04-01T03:30:00Z"),
            ("2020-04-01T03:30:00Z", "2020-04-01T03:35:00Z"),
            ("2020-04-01T03:35:00Z", "2020-04-01T03:40:00Z"),
            ("2020-04-01T03:40:00Z", "2020-04-01T03:45:00Z"),
            ("2020-04-01T03:45:00Z", "2020-04-01T03:50:00Z"),
            ("2020-04-01T03:50:00Z", "2020-04-01T03:55:00Z"),
            ("2020-04-01T03:55:00Z", "2020-04-01T04:00:00Z"),
        ]

        actual_pairs_list = fm._get_timestamp_pairs("test_building")
        # Formatting the timestamps so the test will be more readable
        formatted_actual_pairs_list = [
            (fm.format_ts(x), fm.format_ts(y)) for x, y in actual_pairs_list
        ]
        assert formatted_actual_pairs_list == expected_pairs_list


def test_ingest_records_halts_early_if_the_total_count_has_been_exceeded():
    # TODO: Clean this test up, better doc strings
    # Top level doc string which describes what is expected to happen over the course
    # of the test.
    fm = FinnishMuseumsDataIngester(date=FROZEN_DATE)
    fm.buildings = ["test_building"]

    # Mock _get_timestamp_pairs to return two timestamps. Later we'll
    # mock the first interval to simulate the bug, and the second will
    # work as normal.
    with patch.object(fm, "_get_timestamp_pairs") as ts_mock:
        ts_mock.return_value = [
            (
                datetime(2020, 4, 1, 11, 0, tzinfo=timezone.utc),
                datetime(2020, 4, 1, 12, 0, tzinfo=timezone.utc),
            ),
            (
                datetime(2020, 4, 1, 12, 0, tzinfo=timezone.utc),
                datetime(2020, 4, 1, 13, 0, tzinfo=timezone.utc),
            ),
        ]

        mock_response = {
            "status": "ok",
            "records": [MagicMock(), MagicMock()],  # Two mock records,
            "resultCount": 2,  # Claim there are only two records total
        }

        with (
            patch.object(fm, "get_response_json") as get_mock,
            patch.object(fm, "process_batch", return_value=2) as process_mock,
            patch("common.slack.send_alert") as slack_alert_mock,
        ):
            # mock get_response_json calls in order
            get_mock.side_effect = [
                # First call, get the first page which says there are 2 records
                mock_response,
                # Second call, have it get a second page even though we've already ingested 2 records
                mock_response,
                # Third call, this should be for the SECOND timeslice (should've returned early last time)
                mock_response,
                # Fourth call halts ingestion entirely
                None,
            ]

            # Now try to ingest records
            fm.ingest_records()

            # get_mock should have been called 4 times
            assert get_mock.call_count == 4
            # process_mock should only have been called twice. The second 'batch' for the first time slice
            # should have returned early
            assert process_mock.call_count == 2
            # Slack should have been alerted to the issue
            assert slack_alert_mock.called is True


def test_build_query_param_default():
    actual_param_made = fm.get_next_query_params(
        None,
        building="0/Museovirasto/",
        start_ts=FROZEN_UTC_DATE,
        end_ts=FROZEN_UTC_DATE + timedelta(days=1),
    )
    expected_param = {
        "filter[]": [
            'format:"0/Image/"',
            'building:"0/Museovirasto/"',
            'last_indexed:"[2020-04-01T00:00:00Z TO 2020-04-02T00:00:00Z]"',
        ],
        "limit": 100,
        "page": 1,
    }
    assert actual_param_made == expected_param


def test_build_query_param_given():
    prev_query_params = {
        "filter[]": ['format:"0/Image/"', 'building:"0/Museovirasto/"'],
        "limit": 100,
        "page": 3,
    }
    actual_param_made = fm.get_next_query_params(prev_query_params)
    # Page is incremented
    expected_param = {
        "filter[]": ['format:"0/Image/"', 'building:"0/Museovirasto/"'],
        "limit": 100,
        "page": 4,
    }
    assert actual_param_made == expected_param


def test_get_object_list_from_json_returns_expected_output():
    json_resp = _get_resource_json("finna_full_response_example.json")
    actual_items_list = fm.get_batch_data(json_resp)
    expect_items_list = _get_resource_json("object_list_example.json")
    assert actual_items_list == expect_items_list


def test_get_object_list_return_none_if_empty():
    test_dict = {"records": []}
    assert fm.get_batch_data(test_dict) is None


def test_get_object_list_return_none_if_missing():
    test_dict = {}
    assert fm.get_batch_data(test_dict) is None


def test_get_object_list_return_none_if_none_json():
    assert fm.get_batch_data(None) is None


def test_process_object_with_real_example():
    object_data = _get_resource_json("object_complete_example.json")
    data = fm.get_record_data(object_data)

    assert len(data) == 1
    assert data[0] == {
        "license_info": LicenseInfo(
            "by",
            "4.0",
            "https://creativecommons.org/licenses/by/4.0/",
            "http://creativecommons.org/licenses/by/4.0/",
        ),
        "foreign_identifier": "museovirasto.CC0641BB5337F541CBD19169838BAC1F",
        "foreign_landing_url": (
            "https://www.finna.fi/Record/museovirasto.CC0641BB5337F541CBD19169838BAC1F"
        ),
        "image_url": (
            "https://api.finna.fi/Cover/Show?id=museovirasto.CC0641BB5337F541CBD19169838BAC1F&index=0&size=large"
        ),
        "title": "linnunpönttö koivussa",
        "source": "finnish_heritage_agency",
        "raw_tags": [
            "koivu",
            "koivussa",
            "linnunpöntöt",
            "Revonristi",
            "valmistusaika: 11.06.1923",
        ],
    }


def test_get_image_url():
    response_json = _get_resource_json("full_image_object.json")
    image_url = fm._get_image_url(response_json)
    expected_image_url = "https://api.finna.fi/Cover/Show?id=museovirasto.CC0641BB5337F541CBD19169838BAC1F&index=0&size=large"
    assert image_url == expected_image_url


@pytest.mark.parametrize(
    "image_rights_obj, expected_license_url",
    [
        ({}, None),
        (
            {
                "imageRights": {
                    "link": "http://creativecommons.org/licenses/by/4.0/deed.fi"
                }
            },
            "http://creativecommons.org/licenses/by/4.0/",
        ),
        (
            {"imageRights": {"link": "http://creativecommons.org/licenses/by/4.0/"}},
            "http://creativecommons.org/licenses/by/4.0/",
        ),
    ],
)
def test_get_license_url(image_rights_obj, expected_license_url):
    assert fm.get_license_url(image_rights_obj) == expected_license_url
