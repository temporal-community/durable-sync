"""OAuth 2.1 (PKCE + dynamic client registration) against Notion's hosted MCP
server. Pure HTTP helpers — no Temporal, no browser, no file IO — so they're
reusable from both the interactive bootstrap AND the refresh activity.

Notion's MCP OAuth is a public client (token_endpoint_auth_method="none"): no
client secret, PKCE is mandatory. Endpoints are discovered, not hardcoded, so
this keeps working if Notion moves them.

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

MCP_BASE = "https://mcp.notion.com"
MCP_ENDPOINT = f"{MCP_BASE}/mcp"  # Streamable HTTP transport
_TIMEOUT = 30
_CLIENT_NAME = "durable-sync"


def discover() -> dict[str, str]:
    """Two-step OAuth discovery (RFC 9728 protected-resource -> RFC 8414 AS
    metadata). Returns the endpoints we need: authorization/token/registration.
    """
    pr = requests.get(
        f"{MCP_BASE}/.well-known/oauth-protected-resource", timeout=_TIMEOUT
    )
    pr.raise_for_status()
    auth_server = pr.json()["authorization_servers"][0]

    md = requests.get(
        f"{auth_server}/.well-known/oauth-authorization-server", timeout=_TIMEOUT
    )
    md.raise_for_status()
    data = md.json()
    return {
        "authorization_endpoint": data["authorization_endpoint"],
        "token_endpoint": data["token_endpoint"],
        "registration_endpoint": data["registration_endpoint"],
    }


def register_client(registration_endpoint: str, redirect_uri: str) -> dict[str, Any]:
    """Dynamic Client Registration (RFC 7591). No admin, no pre-approval — this
    is what lets a team self-serve without a workspace-admin integration.
    """
    resp = requests.post(
        registration_endpoint,
        json={
            "client_name": _CLIENT_NAME,
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
    authorization_endpoint: str,
    client_id: str,
    redirect_uri: str,
    code_challenge: str,
    state: str,
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
    token_endpoint: str,
    client_id: str,
    code: str,
    redirect_uri: str,
    code_verifier: str,
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


def refresh_access_token(
    token_endpoint: str, client_id: str, refresh_token: str
) -> dict[str, Any]:
    """Refresh token -> a fresh access_token (and a ROTATED refresh_token).

    Reused by the Temporal refresh activity. Notion rotates the refresh token on
    every use, so the caller MUST persist the returned refresh_token; an
    `invalid_grant` means the stored token was already spent -> re-bootstrap.
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
    resp.raise_for_status()
    return resp.json()
