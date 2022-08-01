from collections import defaultdict
from datetime import datetime, timedelta

import pytest

from openverse_catalog.dags.maintenance.pr_review_reminders.pr_review_reminders import (
    Urgency,
    days_without_weekends,
    post_reminders,
)
from tests.factories.github import (
    make_current_pr_comment,
    make_outdated_pr_comment,
    make_pr_comment,
    make_pull,
    make_requested_reviewer,
    make_review,
)


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
        (MONDAY, MONDAY, 0),
        (TUESDAY, MONDAY, 1),
        (WEDNESDAY, MONDAY, 2),
        (THURSDAY, MONDAY, 3),
        (FRIDAY, MONDAY, 4),
        (FRIDAY, THURSDAY, 1),
        (THURSDAY, WEDNESDAY, 1),
        (WEDNESDAY, TUESDAY, 1),
        (SUNDAY, SATURDAY, 0),
        (NEXT_MONDAY, LAST_MONDAY, 10),
        (NEXT_TUESDAY, LAST_MONDAY, 11),
        (NEXT_WEDNESDAY, LAST_MONDAY, 12),
    ),
)
def test_days_without_weekends_no_weekend_days_monday(today, against, expected_days):
    assert days_without_weekends(today, against) == expected_days


@pytest.fixture
def github(monkeypatch):
    pulls = []
    pull_comments = defaultdict(list)
    posted_comments = defaultdict(list)
    deleted_comments = []
    pull_reviews = defaultdict(list)

    def get_prs(*args, **kwargs):
        return pulls

    def get_comments(*args, **kwargs):
        pr_number = args[2]
        return pull_comments[pr_number]

    def post_comment(*args, **kwargs):
        pr_number = args[2]
        body = args[3]
        posted_comments[pr_number].append(body)

    def delete_comment(*args, **kwargs):
        comment_number = args[2]
        deleted_comments.append(comment_number)

    def get_reviews(*args, **kwargs):
        pr_number = args[2]
        return pull_reviews[pr_number]

    def patch_gh_fn(fn, impl):
        monkeypatch.setattr(
            f"openverse_catalog.dags.maintenance.pr_review_reminders.pr_review_reminders.GitHubAPI.{fn}",
            impl,
        )

    patch_gh_fn("get_open_prs", get_prs)
    patch_gh_fn("get_issue_comments", get_comments)
    patch_gh_fn("post_issue_comment", post_comment)
    patch_gh_fn("delete_issue_comment", delete_comment)
    patch_gh_fn("get_pull_reviews", get_reviews)

    yield {
        "pulls": pulls,
        "pull_comments": pull_comments,
        "posted_comments": posted_comments,
        "deleted_comments": deleted_comments,
        "pull_reviews": pull_reviews,
    }


@pytest.fixture(autouse=True)
def freeze_friday(freeze_time):
    freeze_time.freeze(FRIDAY)


parametrize_urgency = pytest.mark.parametrize(
    "urgency",
    (
        Urgency.CRITICAL,
        Urgency.HIGH,
        Urgency.MEDIUM,
        Urgency.LOW,
    ),
)


@parametrize_urgency
def test_pings_past_due(github, urgency):
    past_due_pull = make_pull(urgency, past_due=True)
    past_due_pull["requested_reviewers"] = [
        make_requested_reviewer(f"reviewer-due-{i}") for i in range(2)
    ]
    not_due_pull = make_pull(urgency, past_due=False)
    not_due_pull["requested_reviewers"] = [
        make_requested_reviewer(f"reviewer-not-due-{i}") for i in range(2)
    ]
    github["pulls"] += [past_due_pull, not_due_pull]
    github["pull_comments"][past_due_pull["number"]].append(
        make_pr_comment(is_reminder=False)
    )

    post_reminders("not_set", dry_run=False)

    assert past_due_pull["number"] in github["posted_comments"]
    assert not_due_pull["number"] not in github["posted_comments"]

    comments = github["posted_comments"][past_due_pull["number"]]
    for reviewer in past_due_pull["requested_reviewers"]:
        for comment in comments:
            assert f"@{reviewer['login']}" in comment


@parametrize_urgency
def test_does_not_reping_past_due_if_reminder_is_current(github, urgency):
    past_due_pull = make_pull(urgency, past_due=True)
    past_due_pull["requested_reviewers"] = [
        make_requested_reviewer(f"reviewer-due-{i}") for i in range(2)
    ]
    not_due_pull = make_pull(urgency, past_due=False)
    not_due_pull["requested_reviewers"] = [
        make_requested_reviewer(f"reviewer-not-due-{i}") for i in range(2)
    ]
    github["pulls"] += [past_due_pull, not_due_pull]
    github["pull_comments"][past_due_pull["number"]].append(
        make_current_pr_comment(past_due_pull)
    )

    post_reminders("not_set", dry_run=False)

    assert past_due_pull["number"] not in github["posted_comments"]
    assert not_due_pull["number"] not in github["posted_comments"]


@parametrize_urgency
def test_does_reping_past_due_if_reminder_is_outdated(github, urgency):
    past_due_pull = make_pull(urgency, past_due=True)
    past_due_pull["requested_reviewers"] = [
        make_requested_reviewer(f"reviewer-due-{i}") for i in range(2)
    ]
    not_due_pull = make_pull(urgency, past_due=False)
    not_due_pull["requested_reviewers"] = [
        make_requested_reviewer(f"reviewer-not-due-{i}") for i in range(2)
    ]
    github["pulls"] += [past_due_pull, not_due_pull]
    reminder_comment = make_outdated_pr_comment(past_due_pull)
    github["pull_comments"][past_due_pull["number"]].append(reminder_comment)

    post_reminders("not_set", dry_run=False)

    assert past_due_pull["number"] in github["posted_comments"]
    assert not_due_pull["number"] not in github["posted_comments"]
    assert reminder_comment["id"] in github["deleted_comments"]


UNAPPROVED_REVIEW_STATES = ("CHANGES_REQUESTED", "COMMENTED")


@parametrize_urgency
@pytest.mark.parametrize(
    "first_review_state",
    # None represents not having a review so we can test the
    # one review state.
    (None,) + UNAPPROVED_REVIEW_STATES,
)
@pytest.mark.parametrize(
    "second_review_state",
    # No none here otherwise we'd duplicate the single review states
    # and we don't need to test the "no review" state because that's
    # tested elsewhere.
    ("APPROVED",) + UNAPPROVED_REVIEW_STATES,
)
def test_does_ping_if_pr_has_less_than_two_approvals(
    github, urgency, first_review_state, second_review_state
):
    reviews = [
        make_review(state)
        for state in (first_review_state, second_review_state)
        if state is not None
    ]
    past_due_pull = make_pull(urgency, past_due=True)
    past_due_pull["requested_reviewers"] = [
        make_requested_reviewer(f"reviewer-due-{i}") for i in range(2)
    ]
    github["pulls"] += [past_due_pull]
    github["pull_reviews"][past_due_pull["id"]] = reviews

    post_reminders("not_set", dry_run=False)

    assert past_due_pull["number"] in github["posted_comments"]


@parametrize_urgency
def test_does_not_ping_if_pr_has_two_approvals(github, urgency):
    past_due_pull = make_pull(urgency, past_due=True)
    past_due_pull["requested_reviewers"] = [
        make_requested_reviewer(f"reviewer-due-{i}") for i in range(2)
    ]
    github["pulls"] += [past_due_pull]
    github["pull_reviews"][past_due_pull["id"]] = [
        make_review("APPROVED"),
        make_review("APPROVED"),
    ]

    post_reminders("not_set", dry_run=False)

    assert past_due_pull["number"] not in github["posted_comments"]


@parametrize_urgency
def test_does_not_ping_if_no_reviewers(github, urgency):
    past_due_pull = make_pull(urgency, past_due=True)
    past_due_pull["requested_reviewers"] = []
    not_due_pull = make_pull(urgency, past_due=False)
    not_due_pull["requested_reviewers"] = []
    github["pulls"] += [past_due_pull, not_due_pull]
    github["pull_comments"][past_due_pull["number"]].append(
        make_pr_comment(is_reminder=False)
    )

    post_reminders("not_set", dry_run=False)

    assert past_due_pull["number"] not in github["posted_comments"]
    assert not_due_pull["number"] not in github["posted_comments"]
