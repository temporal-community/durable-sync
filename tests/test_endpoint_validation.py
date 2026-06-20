"""SSRF guard on OAuth discovery (auth/oauth/flow._validate_endpoint).

The token endpoint receives the refresh token unattended on every refresh, so a
tampered discovery response must not be able to redirect it to an attacker host.
"""
from __future__ import annotations

import pytest

from durable_sync.auth.oauth.flow import _validate_endpoint

BASE = "https://mcp.notion.com"


def test_accepts_same_registrable_domain():
    # AS on a sibling host of the pinned base is fine (mcp.notion.com / notion.com).
    assert _validate_endpoint("https://api.notion.com/token", BASE, same_site=True)
    assert _validate_endpoint("https://notion.com/token", BASE, same_site=True)


def test_rejects_non_https():
    with pytest.raises(ValueError):
        _validate_endpoint("http://mcp.notion.com/token", BASE, same_site=True)


def test_rejects_off_domain_host():
    with pytest.raises(ValueError):
        _validate_endpoint("https://evil.example.com/token", BASE, same_site=True)


def test_same_site_false_allows_off_domain_but_still_requires_https():
    assert _validate_endpoint("https://auth.thirdparty.io/token", BASE, same_site=False)
    with pytest.raises(ValueError):
        _validate_endpoint("http://auth.thirdparty.io/token", BASE, same_site=False)
