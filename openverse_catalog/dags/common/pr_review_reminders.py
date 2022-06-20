import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from common.github import GitHubAPI


logger = logging.getLogger(__name__)


REPOSITORIES = [
    "openverse",
    "openverse-catalog",
    "openverse-api",
    "openverse-frontend",
    "openverse-infrastructure",
]


@dataclass
class Urgency:
    label: str
    days: int


@dataclass
class ReviewDelta:
    urgency: Urgency
    days: int


def pr_urgency(pr: dict) -> Urgency:
    priority_labels = [
        label["name"] for label in pr["labels"] if "priority" in label["name"].lower()
    ]
    if not priority_labels:
        logger.error(f"Found unabled PR ({pr['html_url']}). Skipping!")
        return None

    priority_label = priority_labels[0]

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
        if days_in_previous_week < 2:
            return 0
        return abs(delta.days - max((days_in_previous_week // 7) * 2, 2))

    return delta.days


def get_urgency_if_urgent(pr: dict) -> Optional[ReviewDelta]:
    updated_at = datetime.fromisoformat(pr["updated_at"].rstrip("Z"))
    today = datetime.now()
    urgency = pr_urgency(pr)
    if urgency is None:
        return None

    days = days_without_weekends(today, today - updated_at)

    return ReviewDelta(urgency, days) if days > urgency.days else None


def has_already_reviewed(request: dict, reviews: list[dict]):
    return request["login"] in [review["user"]["login"] for review in reviews]


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

@{pr_author}, if this PR is not ready for a review, please draft it to prevent reviewers
from getting further unnecessary pings.
"""
)


def build_comment(review_delta: ReviewDelta, stale_requests: list[dict], pr: dict):
    user_handles = [f"@{req['login']}" for req in stale_requests]
    return user_handles, COMMENT_TEMPLATE.format(
        urgency_label=review_delta.urgency.label,
        urgency_days=review_delta.urgency.days,
        user_logins="\n".join(user_handles),
        days_since_update=review_delta.days,
        pr_author=pr["user"]["login"],
    )


def base_repo_name(pr: dict):
    return pr["base"]["repo"]["name"]


def post_reminders(github_pat: str, dry_run: bool):
    gh = GitHubAPI(github_pat)

    open_prs = []
    for repo in REPOSITORIES:
        open_prs += [pr for pr in gh.get_open_prs(repo) if not pr["draft"]]

    urgent_prs = []
    for pr in open_prs:
        review_delta = get_urgency_if_urgent(pr)
        if review_delta:
            urgent_prs.append((pr, review_delta))

    to_ping = []
    for pr, review_delta in urgent_prs:
        repo = base_repo_name(pr)
        comments = gh.get_issue_comments(repo, pr["number"])

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

        review_requests = gh.get_pr_review_requests(repo, pr["number"])
        reviews = gh.get_pr_reviews(repo, pr["number"])

        stale_requests = [
            request
            for request in review_requests["users"]
            if not has_already_reviewed(request, reviews)
        ]
        if stale_requests:
            to_ping.append((pr, review_delta, stale_requests))

    for pr, review_delta, stale_requests in to_ping:
        user_handles, comment_body = build_comment(review_delta, stale_requests, pr)

        logger.info(f"Pinging {', '.join(user_handles)} to review {pr['title']}")
        if not dry_run:
            gh.post_issue_comment(base_repo_name(pr), pr["number"], comment_body)

    if dry_run:
        logger.info(
            "This was a dry run. None of the pings listed above were actually sent."
        )
