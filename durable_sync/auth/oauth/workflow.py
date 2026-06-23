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
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from durable_sync.auth.oauth.refresh import RefreshInput, RefreshOutput, refresh_oauth_token

# Refresh this long before the access token's stated expiry.
_REFRESH_SKEW = timedelta(minutes=5)
# Continue-as-new after this many refreshes to keep event history small.
_REFRESHES_BEFORE_CONTINUE = 24
# Back-off between retries when a refresh fails for a TRANSIENT reason (the token
# endpoint is down) — so the workflow self-heals instead of giving up.
_TRANSIENT_BACKOFF = timedelta(seconds=60)


def _is_auth_failure(err: BaseException | None) -> bool:
    """The refresh activity raises a non-retryable ApplicationError(type=AuthError)
    when the refresh token is dead (mirrors sync_records). Type-only check, so it's
    pure/deterministic and safe in the workflow."""
    while err is not None:
        if isinstance(err, ApplicationError) and err.type == "AuthError":
            return True
        err = err.__cause__
    return False


@dataclass
class AuthParams:
    client_id: str
    token_endpoint: str
    refresh_token: str
    # Carried across continue-as-new so the count survives history truncation.
    refreshes_so_far: int = 0
    # Carried across continue-as-new so the query stays warm at the boundary
    # (otherwise the new run starts with an empty token until its first refresh,
    # and any sync querying right then gets an empty token). The codec encrypts it
    # in history — the reason the codec exists.
    access_token: str = ""


@workflow.defn
class OAuthTokenWorkflow:
    @workflow.init
    def __init__(self, params: AuthParams) -> None:
        self._access_token = params.access_token
        self._refresh_token = params.refresh_token
        # client_id / token_endpoint are mutable state (not read from `params`) so a
        # re-bootstrap that mints a NEW OAuth client can be healed via `reauthorize`
        # too — not just a fresh refresh token under the same client.
        self._client_id = params.client_id
        self._token_endpoint = params.token_endpoint
        self._refreshes = params.refreshes_so_far
        # Pause/recover state (mirrors SourceSyncWorkflow) — a dead refresh token
        # parks the workflow instead of crashing it, so it stays queryable and is
        # resumable via the `reauthorize` signal without re-creating it.
        self._paused = False
        self._last_error: str | None = None
        self._last_refresh: str | None = None
        # Supplied by reauthorize after a re-bootstrap; applied on resume.
        self._new_refresh_token = ""
        self._new_client_id = ""
        self._new_token_endpoint = ""

    @workflow.run
    async def run(self, params: AuthParams) -> None:
        while True:
            try:
                out: RefreshOutput = await workflow.execute_activity(
                    refresh_oauth_token,
                    RefreshInput(
                        client_id=self._client_id,
                        token_endpoint=self._token_endpoint,
                        refresh_token=self._refresh_token,
                    ),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(maximum_attempts=5),
                )
            except Exception as e:  # noqa: BLE001 - classify, never let the workflow die
                self._last_error = str(e)
                if _is_auth_failure(e):
                    # Refresh token revoked/expired/spent — only a human re-auth fixes
                    # it. Park until `reauthorize` supplies a fresh refresh token.
                    self._paused = True
                    workflow.logger.error(
                        "OAuth refresh permanently rejected for %s — pausing until "
                        "`reauthorize` with a fresh refresh token.", self._client_id,
                    )
                    await workflow.wait_condition(lambda: not self._paused)
                    # Apply anything reauthorize handed us (token and/or a new client).
                    if self._new_refresh_token:
                        self._refresh_token = self._new_refresh_token
                        self._new_refresh_token = ""
                    if self._new_client_id:
                        self._client_id = self._new_client_id
                        self._new_client_id = ""
                    if self._new_token_endpoint:
                        self._token_endpoint = self._new_token_endpoint
                        self._new_token_endpoint = ""
                else:
                    # Transient (endpoint down, network) — back off and retry rather
                    # than terminating the only source of access tokens.
                    workflow.logger.warning(
                        "OAuth refresh transient failure for %s; retrying after backoff.",
                        self._client_id,
                    )
                    await workflow.sleep(_TRANSIENT_BACKOFF)
                continue

            self._access_token = out.access_token
            self._refresh_token = out.refresh_token  # rotated — keep the newest
            self._refreshes += 1
            self._last_refresh = workflow.now().isoformat()
            self._last_error = None

            sleep_for = timedelta(seconds=out.expires_in) - _REFRESH_SKEW
            if sleep_for <= timedelta(0):
                sleep_for = timedelta(seconds=max(out.expires_in // 2, 30))
            await workflow.sleep(sleep_for)

            # Roll history only AFTER sleeping until the token is near expiry, so
            # the fresh run's immediate refresh is the one that's actually due —
            # not a wasted back-to-back rotation. Carries the latest refresh AND
            # access token so the new run picks up exactly where this one left off
            # (no empty-token query gap at the boundary).
            if self._refreshes >= _REFRESHES_BEFORE_CONTINUE:
                await workflow.wait_condition(workflow.all_handlers_finished)
                workflow.continue_as_new(
                    AuthParams(
                        client_id=self._client_id,
                        token_endpoint=self._token_endpoint,
                        refresh_token=self._refresh_token,
                        refreshes_so_far=0,
                        access_token=self._access_token,
                    )
                )

    # --- Signals (flip flags only; non-async; tolerate stray payloads) -------

    @workflow.signal
    def reauthorize(self, refresh_token: str = "", client_id: str = "",
                    token_endpoint: str = "", *_: object) -> None:
        """Resume after a pause. Pass a fresh refresh token (from re-running the
        provider's bootstrap) when the old one was revoked/expired; a bare signal
        just retries with the current token (e.g. to recover from a long outage).
        Also pass client_id / token_endpoint if the re-bootstrap minted a NEW OAuth
        client — so an app change heals via signal too, no re-seed needed."""
        if refresh_token:
            self._new_refresh_token = refresh_token
        if client_id:
            self._new_client_id = client_id
        if token_endpoint:
            self._new_token_endpoint = token_endpoint
        self._paused = False

    # --- Queries (read-only) -------------------------------------------------

    @workflow.query
    def get_access_token(self) -> str:
        """Current valid access token. Queries aren't written to history, so
        callers get the secret without it ever touching the event log."""
        return self._access_token

    @workflow.query
    def status(self) -> dict:
        """Operational state (no secret) — is it healthy, and if not, why."""
        return {
            "paused": self._paused,
            "refreshes": self._refreshes,
            "last_refresh": self._last_refresh,
            "last_error": self._last_error,
            "has_token": bool(self._access_token),
        }
