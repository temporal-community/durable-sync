"""The refresh activity — the only IO in the OAuth auth hot path.

Wraps oauth.refresh_access_token so OAuthTokenWorkflow stays deterministic (no
network in the workflow). Returns the new access token AND the rotated refresh
token; the workflow persists both in its state. Provider-agnostic — the token
endpoint + client id come in via the input. Kept in its own module so the
workflow imports it pass-through without dragging `requests` into the sandbox.
"""
from __future__ import annotations

from dataclasses import dataclass

from temporalio import activity
from temporalio.exceptions import ApplicationError

from durable_sync.auth.oauth import flow as oauth
from durable_sync.core import auth_error_in_chain


@dataclass
class RefreshInput:
    client_id: str
    token_endpoint: str
    refresh_token: str


@dataclass
class RefreshOutput:
    access_token: str
    refresh_token: str  # rotated — the workflow MUST store this
    expires_in: int


@activity.defn
def refresh_oauth_token(inp: RefreshInput) -> RefreshOutput:
    try:
        tokens = oauth.refresh_access_token(inp.token_endpoint, inp.client_id, inp.refresh_token)
    except Exception as e:
        # A revoked/expired/spent refresh token can't be fixed by retrying — only a
        # human re-bootstrap mints a new one. Mark it non-retryable + typed so the
        # OAuthTokenWorkflow PAUSES (stays queryable + resumable) instead of burning
        # retries and then terminating. Transient failures stay retryable (re-raise).
        if auth_error_in_chain(e):
            raise ApplicationError(
                "OAuth refresh token is no longer valid (expired, revoked, or spent). "
                "Re-bootstrap to mint a fresh token, then send the `reauthorize` signal.",
                type="AuthError", non_retryable=True,
            ) from e
        raise
    return RefreshOutput(
        access_token=tokens["access_token"],
        # Not every provider rotates the refresh token on each refresh — many omit
        # `refresh_token` from the response when it's unchanged. Falling back to the
        # one we sent keeps the chain alive instead of KeyError-ing the activity
        # (which, after retries, would kill the token workflow and break auth).
        refresh_token=tokens.get("refresh_token") or inp.refresh_token,
        expires_in=int(tokens.get("expires_in", 3600)),
    )
