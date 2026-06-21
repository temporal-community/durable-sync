"""Unit tests for build_authorize_url's scope handling (no network).

DCR providers (Notion/Contentful) grant scopes at client registration and omit
`scope` from the authorize URL; a manually-registered app (Spotify) must request
scopes here. Guards that scope is included only when supplied, and is encoded."""
from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

from durable_sync.auth.oauth import flow


def _query(url: str) -> dict[str, list[str]]:
    return parse_qs(urlsplit(url).query)


def test_scope_omitted_by_default():
    url = flow.build_authorize_url("https://a/auth", "cid", "http://localhost/cb", "chal", "st")
    assert "scope" not in _query(url)


def test_scope_included_and_encoded_when_supplied():
    url = flow.build_authorize_url(
        "https://a/auth", "cid", "http://localhost/cb", "chal", "st",
        scope="user-library-read playlist-read-private",
    )
    q = _query(url)
    # urlencode round-trips the space-delimited scope as one value.
    assert q["scope"] == ["user-library-read playlist-read-private"]
    # PKCE + state still present.
    assert q["code_challenge_method"] == ["S256"]
    assert q["state"] == ["st"]


def test_empty_scope_treated_as_absent():
    url = flow.build_authorize_url(
        "https://a/auth", "cid", "http://localhost/cb", "chal", "st", scope="",
    )
    assert "scope" not in _query(url)
