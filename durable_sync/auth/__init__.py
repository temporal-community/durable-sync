"""Generic OAuth-as-a-workflow toolkit (provider-agnostic).

For destinations whose API offers no admin-free static token: authorize as an
individual via OAuth 2.1 + PKCE + dynamic client registration, then let a Temporal
workflow OWN the rotating refresh token — refreshing on a timer and serving fresh
access tokens via query (so the secret never enters event history).

Standards-based (RFC 8414 discovery, RFC 7591 dynamic registration, PKCE), so it
works for any conformant provider given its base URL. Notion is the first
consumer (see destinations/notion); a future Slack/Linear/Jira/Google destination
reuses this wholesale.
"""
from __future__ import annotations

from durable_sync.auth.refresh import RefreshInput, RefreshOutput, refresh_oauth_token
from durable_sync.auth.token import current_access_token
from durable_sync.auth.workflow import AuthParams, OAuthTokenWorkflow

__all__ = [
    "OAuthTokenWorkflow",
    "AuthParams",
    "refresh_oauth_token",
    "RefreshInput",
    "RefreshOutput",
    "current_access_token",
]
