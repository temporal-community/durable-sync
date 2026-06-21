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
from urllib.parse import urlsplit

import requests

_TIMEOUT = 30
DEFAULT_CLIENT_NAME = "durable-sync"


def _registrable_domain(host: str) -> str:
    """Last two labels of a host (heuristic, no PSL): notion.com, contentful.com.
    Good enough to pin discovered OAuth endpoints to the provider's own domain;
    the hard guarantee is the https check in _validate_endpoint."""
    labels = host.split(".")
    return ".".join(labels[-2:]) if len(labels) >= 2 else host


def _validate_endpoint(url: str, base_url: str, *, same_site: bool) -> str:
    """Reject a discovered OAuth endpoint that could exfiltrate the refresh token.

    We POST the refresh token to whatever the discovery documents name, on every
    refresh, unattended — so a tampered/compromised discovery response must not be
    able to point us at an attacker host. Enforce https always; when `same_site`
    (the default), also require the same registrable domain as the pinned base URL.
    Providers whose authorization server is on a different domain pass same_site=False."""
    parts = urlsplit(url)
    if parts.scheme != "https":
        raise ValueError(f"Refusing non-https OAuth endpoint from discovery: {url!r}")
    if same_site:
        base_host = urlsplit(base_url).hostname or ""
        host = parts.hostname or ""
        if _registrable_domain(host) != _registrable_domain(base_host):
            raise ValueError(
                f"Discovered OAuth endpoint {host!r} is off-domain from {base_host!r}; "
                f"refusing (pass same_site=False if this provider's auth server is "
                f"intentionally on another domain)."
            )
    return url


def discover(base_url: str, *, same_site: bool = True) -> dict[str, str]:
    """Two-step OAuth discovery (RFC 9728 protected-resource -> RFC 8414 AS
    metadata) against `base_url`. Returns authorization/token/registration
    endpoints. Every discovered endpoint is validated (https + same-domain) before
    return, because the token endpoint later receives the refresh token unattended
    (see _validate_endpoint)."""
    pr = requests.get(f"{base_url}/.well-known/oauth-protected-resource", timeout=_TIMEOUT)
    pr.raise_for_status()
    auth_server = _validate_endpoint(
        pr.json()["authorization_servers"][0], base_url, same_site=same_site
    )

    md = requests.get(f"{auth_server}/.well-known/oauth-authorization-server", timeout=_TIMEOUT)
    md.raise_for_status()
    data = md.json()
    return {
        "authorization_endpoint": _validate_endpoint(data["authorization_endpoint"], base_url, same_site=same_site),
        "token_endpoint": _validate_endpoint(data["token_endpoint"], base_url, same_site=same_site),
        "registration_endpoint": _validate_endpoint(data["registration_endpoint"], base_url, same_site=same_site),
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
    code_challenge: str, state: str, *, scope: str | None = None,
) -> str:
    """Build the authorization-code+PKCE redirect URL. `scope` is a space-delimited
    scope string, included only when supplied: the DCR providers (Notion/Contentful)
    grant scopes at client registration so they omit it, while a manually-registered
    app (e.g. Spotify, which has no DCR) must request scopes here."""
    from urllib.parse import urlencode
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "state": state,
    }
    if scope:
        params["scope"] = scope
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
