from airflow.providers.http.hooks.http import HttpHook
from requests import Response


class GithubAPI:
    def __init__(self, github_pat: str, http_conn_id: str):
        self.github_pat = github_pat
        self.http_conn_id = http_conn_id

    def _get_endpoint(self, resource):
        return f"https://api.github.com/{resource}"

    def _get_hook(self, method) -> HttpHook:
        return HttpHook(method, self.http_conn_id)

    @property
    def _headers(self):
        return {"Authorization": f"token {self.github_pat}"}

    def _run_hook(self, method, resource, data=None, params=None) -> Response:
        hook = self._get_hook(method)
        return hook.run(
            self._get_endpoint(resource),
            data=data,
            params=params,
            headers=self._headers,
        )

    def get_open_prs(self, repo, owner="WordPress"):
        return self._run_hook(
            "GET",
            f"repos/{owner}/{repo}/pulls",
            params={
                "state": "open",
                "base": "main",
                "sort": "updated",
                # this is the default when ``sort`` is ``updated`` but
                # it's helpful to specify for readers
                "direction": "asc",
                # we don't bother paginating because if we ever
                # have more than 100 open PRs in a single repo
                # then something is seriously wrong
                "per_page": 100,
            },
        ).json()

    def get_pr_review_requests(
        self, repo: str, pr_number: int, owner: str = "WordPress"
    ):
        return self._run_hook(
            "GET",
            f"repos/{owner}/{repo}/pulls/{pr_number}/requested_reviewers",
        )

    def get_pr_reviews(self, repo: str, pr_number: int, owner: str = "WordPress"):
        return self._run_hook(
            "GET",
            f"repos/{owner}/{repo}/pulls/{pr_number}/reviews",
        )

    def post_issue_comment(
        self, repo: str, issue_number: int, comment_body: str, owner: str = "WordPress"
    ):
        return self._run_hook(
            "POST",
            f"repos/{owner}/{repo}/issues/{issue_number}/comments",
            data={"body": comment_body},
        )

    def get_issue_comments(
        self, repo: str, issue_number: int, owner: str = "WordPress"
    ):
        return self._run_hook(
            "GET",
            f"repos/{owner}/{repo}/issues/{issue_number}/comments",
        )
