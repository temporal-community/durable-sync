"""OAuth 2.1 (PKCE + dynamic client registration) — provider-agnostic HTTP
helpers. No Temporal, no browser, no file IO, no hardcoded provider: every
endpoint is passed in (discover() takes the server base URL). Reusable from an
interactive bootstrap AND from the refresh activity.

Public clients (token_endpoint_auth_method="none"): no client secret, PKCE
mandatory. Endpoints are discovered, not hardcoded, so this keeps working if a
provider moves them.

Deliberately NOT the MCP SDK's OAuthClientProvider: we own the token lifecycle
(the auth workflow does) and pass a plain Bearer header to the transport, which
sidesteps that SDK's cross-version auth API churn.
"""
from __future__ import annotations

import base64
import hashlib
import os
import secrets
from typing import Any

import requests

_TIMEOUT = 30
DEFAULT_CLIENT_NAME = "durable-sync"


def discover(base_url: str) -> dict[str, str]:
    """Two-step OAuth discovery (RFC 9728 protected-resource -> RFC 8414 AS
    metadata) against `base_url`. Returns authorization/token/registration
    endpoints."""
    pr = requests.get(f"{base_url}/.well-known/oauth-protected-resource", timeout=_TIMEOUT)
    pr.raise_for_status()
    auth_server = pr.json()["authorization_servers"][0]

    md = requests.get(f"{auth_server}/.well-known/oauth-authorization-server", timeout=_TIMEOUT)
    md.raise_for_status()
    data = md.json()
    return {
        "authorization_endpoint": data["authorization_endpoint"],
        "token_endpoint": data["token_endpoint"],
        "registration_endpoint": data["registration_endpoint"],
    }


def register_client(
    registration_endpoint: str, redirect_uri: str, *, client_name: str = DEFAULT_CLIENT_NAME
) -> dict[str, Any]:
    """Dynamic Client Registration (RFC 7591) — no admin, no pre-approval."""
    resp = requests.post(
        registration_endpoint,
        json={
            "client_name": client_name,
            "redirect_uris": [redirect_uri],
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "none",
        },
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def gen_pkce() -> tuple[str, str]:
    """Return (verifier, challenge) for PKCE S256."""
    verifier = base64.urlsafe_b64encode(os.urandom(32)).rstrip(b"=").decode()
    digest = hashlib.sha256(verifier.encode()).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return verifier, challenge


def new_state() -> str:
    return secrets.token_urlsafe(16)


def build_authorize_url(
    authorization_endpoint: str, client_id: str, redirect_uri: str,
    code_challenge: str, state: str,
) -> str:
    from urllib.parse import urlencode
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "state": state,
    }
    return f"{authorization_endpoint}?{urlencode(params)}"


def exchange_code(
    token_endpoint: str, client_id: str, code: str, redirect_uri: str, code_verifier: str
) -> dict[str, Any]:
    """Authorization code -> tokens (access_token, refresh_token, expires_in)."""
    resp = requests.post(
        token_endpoint,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "code_verifier": code_verifier,
        },
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def refresh_access_token(token_endpoint: str, client_id: str, refresh_token: str) -> dict[str, Any]:
    """Refresh token -> a fresh access_token (and possibly a ROTATED refresh_token).

    Providers like Notion rotate the refresh token on every use, so the caller
    MUST persist the returned refresh_token; an `invalid_grant` means the stored
    token was already spent -> re-bootstrap.
    """
    resp = requests.post(
        token_endpoint,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
        },
        timeout=_TIMEOUT,
    )
    if resp.status_code >= 400:
        # OAuth errors are JSON: {"error": "...", "error_description": "..."}. Surface
        # the body, and turn the common "your token is dead" cases into a plain-English
        # hint instead of a bare HTTPError. Keep `invalid_grant`/401 in the message so
        # is_auth_error still classifies it.
        body = resp.text[:600]
        try:
            err = (resp.json() or {}).get("error", "")
        except ValueError:
            err = ""
        if err in ("invalid_grant", "invalid_client") or resp.status_code in (400, 401):
            raise RuntimeError(
                f"OAuth token refresh rejected ({resp.status_code} {err or 'error'}). The stored "
                f"refresh token is no longer valid — expired, revoked, or already spent (providers "
                f"that rotate the refresh token on every use, e.g. Notion, invalidate the old one each "
                f"refresh). Re-authorize to mint a fresh token by re-running your provider's bootstrap. "
                f"Server said: {body}"
            )
        raise RuntimeError(f"OAuth token refresh failed ({resp.status_code}): {body}")
    return resp.json()
