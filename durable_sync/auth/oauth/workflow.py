"""OAuthTokenWorkflow — the entity workflow that owns a rotating OAuth refresh
token (provider-agnostic).

Why a workflow and not a cron job + a file:
- It's the SINGLE owner of the rotating refresh token, so refreshes are
  serialized by construction — the concurrent-refresh `invalid_grant` race that
  rotating-refresh-token providers warn about can't happen.
- Its state (the refresh token) is durable across worker restarts.
- It hands out fresh access tokens via @workflow.query, which is NOT recorded in
  history — so activities fetch a token without it touching the event log. (Pair
  with the encryption codec to protect the token in workflow state.)

Start one per provider/account, with the id the destination expects (e.g.
config.NOTION_AUTH_WORKFLOW_ID). A bootstrap captures the initial refresh token;
from then on this runs unattended, refreshing ~5 min before expiry.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from durable_sync.auth.oauth.refresh import RefreshInput, RefreshOutput, refresh_oauth_token

# Refresh this long before the access token's stated expiry.
_REFRESH_SKEW = timedelta(minutes=5)
# Continue-as-new after this many refreshes to keep event history small.
_REFRESHES_BEFORE_CONTINUE = 24


@dataclass
class AuthParams:
    client_id: str
    token_endpoint: str
    refresh_token: str
    # Carried across continue-as-new so the count survives history truncation.
    refreshes_so_far: int = 0


@workflow.defn
class OAuthTokenWorkflow:
    def __init__(self) -> None:
        self._access_token: str = ""

    @workflow.run
    async def run(self, params: AuthParams) -> None:
        refresh_token = params.refresh_token
        refreshes = params.refreshes_so_far

        while True:
            out: RefreshOutput = await workflow.execute_activity(
                refresh_oauth_token,
                RefreshInput(
                    client_id=params.client_id,
                    token_endpoint=params.token_endpoint,
                    refresh_token=refresh_token,
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=5),
            )
            self._access_token = out.access_token
            refresh_token = out.refresh_token  # rotated — keep the newest
            refreshes += 1

            if refreshes >= _REFRESHES_BEFORE_CONTINUE:
                workflow.continue_as_new(
                    AuthParams(
                        client_id=params.client_id,
                        token_endpoint=params.token_endpoint,
                        refresh_token=refresh_token,
                        refreshes_so_far=0,
                    )
                )

            sleep_for = timedelta(seconds=out.expires_in) - _REFRESH_SKEW
            if sleep_for <= timedelta(0):
                sleep_for = timedelta(seconds=max(out.expires_in // 2, 30))
            await workflow.sleep(sleep_for)

    @workflow.query
    def get_access_token(self) -> str:
        """Current valid access token. Queries aren't written to history, so
        callers get the secret without it ever touching the event log."""
        return self._access_token
