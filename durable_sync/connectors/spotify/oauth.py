"""Spotify binding of the generic OAuth client (durable_sync.auth.oauth.flow).

Unlike Notion/Contentful, Spotify offers NO Dynamic Client Registration and NO
discovery document: you register an app once in the Spotify developer dashboard
(https://developer.spotify.com/dashboard) to get a fixed `client_id`, and the
endpoints are well-known constants. So this binding PINS the endpoints + the
read scope and re-exports only the PKCE pieces the bootstrap needs — there is no
`discover()`/`register_client()` here on purpose.

Spotify supports Authorization Code + PKCE for public clients (no client secret),
and ROTATES the refresh token on every refresh — both already handled by the
generic flow + OAuthTokenWorkflow.
"""
from __future__ import annotations

from durable_sync.auth.oauth.flow import (  # noqa: F401  (re-exported for the bootstrap)
    build_authorize_url,
    exchange_code,
    gen_pkce,
    new_state,
    refresh_access_token,
)

# Well-known Spotify OAuth endpoints (no discovery document is published).
AUTHORIZE_ENDPOINT = "https://accounts.spotify.com/authorize"
TOKEN_ENDPOINT = "https://accounts.spotify.com/api/token"

# Read-only access to the user's "Liked Songs" library — all this source needs.
DEFAULT_SCOPE = "user-library-read"

# Env var holding the dashboard-registered app's client id (no secret: PKCE).
CLIENT_ID_ENV = "SPOTIFY_CLIENT_ID"
