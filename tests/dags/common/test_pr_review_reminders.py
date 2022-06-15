from datetime import datetime, timedelta

import pytest

from openverse_catalog.dags.common.pr_review_reminders import days_without_weekends


MONDAY = datetime(2022, 6, 13)
TUESDAY = MONDAY + timedelta(days=1)
WEDNESDAY = MONDAY + timedelta(days=2)
THURSDAY = MONDAY + timedelta(days=3)
FRIDAY = MONDAY + timedelta(days=4)
SATURDAY = MONDAY + timedelta(days=5)
SUNDAY = MONDAY + timedelta(days=6)

NEXT_MONDAY = MONDAY + timedelta(days=7)
NEXT_TUESDAY = MONDAY + timedelta(days=8)
NEXT_WEDNESDAY = MONDAY + timedelta(days=9)

LAST_SUNDAY = MONDAY - timedelta(days=1)
LAST_SATURDAY = MONDAY - timedelta(days=2)
LAST_FRIDAY = MONDAY - timedelta(days=3)
LAST_THURSDAY = MONDAY - timedelta(days=4)
LAST_WEDNESDAY = MONDAY - timedelta(days=5)
LAST_TUESDAY = MONDAY - timedelta(days=6)
LAST_MONDAY = MONDAY - timedelta(days=7)


@pytest.mark.parametrize(
    "today, against, expected_days",
    (
        (MONDAY, LAST_SUNDAY, 0),
        (MONDAY, LAST_SATURDAY, 0),
        (MONDAY, LAST_FRIDAY, 1),
        (MONDAY, LAST_THURSDAY, 2),
        (MONDAY, LAST_WEDNESDAY, 3),
        (MONDAY, LAST_TUESDAY, 4),
        (MONDAY, LAST_MONDAY, 5),
        (NEXT_MONDAY, LAST_MONDAY, 10),
        (NEXT_TUESDAY, LAST_MONDAY, 11),
        (NEXT_WEDNESDAY, LAST_MONDAY, 12),
    ),
)
def test_days_without_weekends_no_weekend_days_monday(today, against, expected_days):
    delta = today - against
    assert days_without_weekends(today, delta) == expected_days
