import datetime
import logging
from dataclasses import dataclass
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


class Urgency:
    @dataclass
    class Urgency:
        label: str
        days: int

    CRITICAL = Urgency("critical", 1)
    HIGH = Urgency("high", 2)
    MEDIUM = Urgency("medium", 4)
    LOW = Urgency("low", 5)


@dataclass
class ReviewDelta:
    urgency: Urgency.Urgency
    days: int


def pr_urgency(pr: dict) -> Urgency.Urgency:
    priority_labels = [
        label["name"] for label in pr["labels"] if "priority" in label["name"].lower()
    ]
    if not priority_labels:
        logger.error(f"Found unabled PR ({pr['html_url']}). Skipping!")
        return None

    priority_label = priority_labels[0]

    if "critical" in priority_label:
        return Urgency.CRITICAL
    elif "high" in priority_label:
        return Urgency.HIGH
    elif "medium" in priority_label:
        return Urgency.MEDIUM
    elif "low" in priority_label:
        return Urgency.LOW


def days_without_weekends(today: datetime, updated_at: datetime) -> int:
    """
    Adapted from:
    https://stackoverflow.com/a/3615984 CC BY-SA 2.5
    """
    if today.weekday() == 0 and (today - updated_at).days < 3:
        # shortcut mondays to 0 if last updated on the weekend
        return 0

    daygenerator = (
        updated_at + datetime.timedelta(x + 1) for x in range((today - updated_at).days)
    )
    return sum(1 for day in daygenerator if day.weekday() < 5)


def parse_gh_date(d) -> datetime.datetime:
    return datetime.datetime.fromisoformat(d.rstrip("Z"))


def get_urgency_if_urgent(pr: dict) -> Optional[ReviewDelta]:
    updated_at = parse_gh_date(pr["updated_at"])
    today = datetime.datetime.now()
    urgency = pr_urgency(pr)
    if urgency is None:
        return None

    days = days_without_weekends(today, updated_at)

    return ReviewDelta(urgency, days) if days >= urgency.days else None


COMMENT_MARKER = (
    "This reminder is being automatically generated due to the urgency configuration."
)

# The formatting of this string is kind of annoying, but GitHub interprets comment
# bodies sent to it via the API strictly and single returns get inserted as hard
# breaks even if they don't have trailing spaces. Therefore, the string below
# concatenates into a single "flat" string and the explicit newline characters are
# there to make it easier to see the intended visual presentation of it... while
# still keeping flake8 at bay about string line lengths.
COMMENT_TEMPLATE = (
    "Based on the {urgency_label} urgency of this PR, the following "
    "reviewers are being gently reminded to review this PR:"
    "\n"
    "\n"
    "{user_logins}"
    "\n"
    "\n"
    f"{COMMENT_MARKER}"
    "\n"
    "\n"
    "Ignoring weekend[^1] days, this PR was updated {days_since_update} day(s) ago. "
    "PRs labelled with {urgency_label} urgency are expected to be reviewed within "
    "{urgency_days} weekday(s)[^2]."
    "\n"
    "\n"
    "@{pr_author}, if this PR is not ready for a review, please draft it to "
    "prevent reviewers from getting further unnecessary pings."
    "\n"
    "[^1]: Specifically, Saturday and Sunday.\n"
    "[^2]: For the purpose of these reminders we treat Monday - Friday as weekdays. "
    "Please note that the that generates these reminders runs at midnight "
    "UTC on Monday - Friday. This means that depending on your timezone, "
    "you may be pinged outside of the expected range."
)


def build_comment(review_delta: ReviewDelta, pr: dict):
    user_handles = [f"@{req['login']}" for req in pr["requested_reviewers"]]
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
        previous_comments = gh.get_issue_comments(repo, pr["number"])

        reminder_comments = [
            comment
            for comment in previous_comments
            if (
                comment["user"]["login"] == "openverse-bot"
                and COMMENT_MARKER in comment["body"]
            )
        ]

        # While this version of the DAG will only ever allow a single
        # cyrrent reminder comment to exist, we still need to split this out
        # so that comments left by previous iterations will still get picked
        # up, read as current or not, and also recorded for deletion by the
        # script so that the final step can delete all but the most recently
        # left reminder comment.
        current_reminder = next(
            iter(
                [
                    r
                    for r in reminder_comments
                    if parse_gh_date(r["created_at"]) > parse_gh_date(pr["updated_at"])
                ]
            ),
            None,
        )

        if current_reminder:
            continue

        if pr["requested_reviewers"] == []:
            # no requested reviewers to ping, maybe in the future we ping
            # the PR author or the whole openverse maintainers team?
            continue

        existing_reviews = gh.get_pull_reviews(base_repo_name(pr), pr["number"])

        approved_reviews = [r for r in existing_reviews if r["state"] == "APPROVED"]
        if len(approved_reviews) == 2:
            # if PR already has sufficient reviews to be merged, do not ping
            # the requested reviewers.
            continue

        to_ping.append(
            (
                pr,
                review_delta,
                reminder_comments,
            )
        )

    for pr, review_delta, previous_comments in to_ping:
        user_handles, comment_body = build_comment(review_delta, pr)

        logger.info(f"Pinging {', '.join(user_handles)} to review {pr['title']}")
        if not dry_run:
            gh.post_issue_comment(base_repo_name(pr), pr["number"], comment_body)
            for comment in previous_comments:
                gh.delete_issue_comment(base_repo_name(pr), comment["id"])

    if dry_run:
        logger.info(
            "This was a dry run. None of the pings listed above were actually sent."
        )
