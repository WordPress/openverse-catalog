import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from common.github import GithubAPI


logger = logging.getLogger(__name__)


@dataclass
class Urgency:
    label: str
    days: int


@dataclass
class ReviewDelta:
    urgency: Urgency
    days: int


def pr_urgency(pr) -> Urgency:
    priority_label = [
        label["name"] for label in pr["labels"] if "priority" in label["name"].lower()
    ][0]
    if "critical" in priority_label:
        return Urgency("critical", 1)
    elif "high" in priority_label:
        return Urgency("high", 2)
    elif "medium" in priority_label:
        return Urgency("medium", 4)
    elif "low" in priority_label:
        return Urgency("low", 5)


def days_without_weekends(today: datetime, delta: timedelta) -> int:
    days_in_previous_week = abs(today.weekday() - delta.days)
    if days_in_previous_week > 0:
        # we know there's at least one weekend in this case,
        # so anything floored by 7 will indicate additional weekends
        additional_weekends = days_in_previous_week // 7
        num_weekends = additional_weekends + 1
        return delta.days - max(num_weekends * 2, 2)

    return delta.days


def get_urgency_if_urgent(pr) -> Optional[ReviewDelta]:
    updated_at = datetime.fromisoformat(pr["updated_at"])
    today = datetime.now()
    urgency = pr_urgency(pr)
    days = days_without_weekends(today - updated_at)

    return ReviewDelta(urgency, days) if days > urgency.days else None


def has_already_reviewed(request, reviews):
    return request["user"]["login"] in [review["user"]["login"] for review in reviews]


COMMENT_MARKER = (
    "This reminder is being automatically generated due to the urgency configuration."
)


COMMENT_TEMPLATE = (
    """
Based on the {urgency_label} urgency of this PR, the following reviewers are being
gently reminded to review this PR:

{user_logins}

"""
    f"{COMMENT_MARKER}"
    """
Ignoring weekend days, this PR was updated {days_since_update} day(s) ago. PRs
labelled with {urgency_label} urgency are expected to be reviewed within {urgency_days}.
"""
)


def build_comment(review_delta: ReviewDelta, stale_requests, dry_run: bool):
    user_handles = [f"@{req['user']['login']}" for req in stale_requests]
    return user_handles, COMMENT_TEMPLATE.format(
        urgency_label=review_delta.urgency.label,
        urgency_days=review_delta.urgency.days,
        user_logins="\n".join(user_handles),
        days_since_update=review_delta.days,
    )


def post_reminders(access_key: str, dry_run: bool):
    if datetime.now().weekday() >= 5:
        return  # it's the weekend!

    gh = GithubAPI(access_key, "pr_review_reminders")

    repositories = [
        "openverse",
        "openverse-catalog",
        "openverse-api",
        "openverse-frontend",
        "openverse-infrastructure",
    ]

    open_prs = []
    for repo in repositories:
        open_prs += [pr for pr in gh.get_open_prs(repo) if not pr["draft"]]

    urgent_prs = []
    for pr in open_prs:
        review_delta = get_urgency_if_urgent(pr)
        if review_delta:
            urgent_prs.append((pr, review_delta))

    to_ping = []
    for pr, review_delta in urgent_prs:
        comments = gh.get_issue_comments(pr["repo"]["name"], pr["number"])

        reminder_comments = [
            comment
            for comment in comments
            if (
                comment["user"]["login"] == "openverse-bot"
                and COMMENT_MARKER in comment["body"]
            )
        ]
        if reminder_comments:
            # maybe in the future we re-ping in some cases?
            continue

        review_requests = gh.get_pr_review_requests(pr["repo"]["name"], pr["number"])
        reviews = gh.get_pr_reviews(pr["repo"]["name"], pr["number"])

        stale_requests = [
            request
            for request in review_requests
            if not has_already_reviewed(request, reviews)
        ]
        if stale_requests:
            to_ping.append((pr, review_delta, stale_requests))

    for pr, stale_requests in to_ping:
        user_handles, comment_body = build_comment(review_delta, stale_requests)

        logger.info(f"Pinging {', '.join(user_handles)} to review {pr['title']}")
        if not dry_run:
            gh.post_issue_comment(pr["repo"]["name"], pr["number"], comment_body)

    if dry_run:
        logger.info(
            "This was a dry run. None of the pings listed above were actually sent."
        )
