import datetime
import json
from pathlib import Path

from openverse_catalog.dags.maintenance.pr_review_reminders.pr_review_reminders import (
    COMMENT_MARKER,
    Urgency,
)


def _read_fixture(fixture: str) -> dict:
    with open(Path(__file__).parent / f"{fixture}.json") as file:
        return json.loads(file.read())


def _make_label(priority: Urgency) -> dict:
    return {"name": f"priority: {priority.label}"}


def walk_backwards_in_time_until_weekday_count(today: datetime.datetime, count: int):
    test_date = today
    weekday_count = 0
    while weekday_count < count:
        test_date = test_date - datetime.timedelta(days=1)
        if test_date.weekday() < 5:
            weekday_count += 1

    return test_date


_pr_count = 1


def make_pull(priority: Urgency, past_due: bool) -> dict:
    global _pr_count
    pull = _read_fixture("pull")
    pull["number"] = pull["id"] = _pr_count
    _pr_count += 1

    for label in pull["labels"]:
        if "priority" in label["name"]:
            label.update(**_make_label(priority))
            break

    if past_due:
        updated_at = walk_backwards_in_time_until_weekday_count(
            datetime.datetime.now(), priority.days
        )
    else:
        updated_at = datetime.datetime.now()

    pull["updated_at"] = f"{updated_at.isoformat()}Z"

    return pull


def make_requested_reviewer(login: str) -> dict:
    requested_reviewer = _read_fixture("requested_reviewer")

    requested_reviewer["login"] = login

    return requested_reviewer


def make_pr_comment(is_reminder: bool) -> dict:
    comment = _read_fixture("comment")

    if is_reminder:
        comment["user"]["login"] = "openverse-bot"

    comment["body"] = (
        ("This is a comment\n" f"{COMMENT_MARKER}\n\n" "Please review me :)")
        if is_reminder
        else (
            "This looks great! Amazing work :tada: "
            "You're lovely and valued as a contributor "
            "and as a whole person."
        )
    )

    return comment
