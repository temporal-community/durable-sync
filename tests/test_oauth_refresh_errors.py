"""Unit tests for refresh_access_token's error surfacing (no network).

Guards that a dead refresh token produces a plain-English, actionable error
(re-bootstrap) instead of a bare HTTPError — and keeps invalid_grant/401 in the
text so is_auth_error still classifies it."""
from __future__ import annotations

import pytest

from durable_sync.auth.oauth import flow
from durable_sync.core import auth_error_in_chain


class _Resp:
    def __init__(self, status_code, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text or (str(payload) if payload else "")

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


def _patch(monkeypatch, resp):
    monkeypatch.setattr(flow.requests, "post", lambda *a, **k: resp)


def test_invalid_grant_gives_rebootstrap_hint(monkeypatch):
    _patch(monkeypatch, _Resp(400, {"error": "invalid_grant", "error_description": "expired"}))
    with pytest.raises(RuntimeError) as ei:
        flow.refresh_access_token("https://t", "cid", "stale-token")
    msg = str(ei.value)
    assert "re-running your provider's bootstrap" in msg
    assert "invalid_grant" in msg
    # still classifiable as an auth error by the shared matcher
    assert auth_error_in_chain(ei.value)


def test_500_gives_generic_failure(monkeypatch):
    _patch(monkeypatch, _Resp(500, text="upstream boom"))
    with pytest.raises(RuntimeError, match="refresh failed .500.: upstream boom"):
        flow.refresh_access_token("https://t", "cid", "tok")


def test_success_returns_tokens(monkeypatch):
    _patch(monkeypatch, _Resp(200, {"access_token": "a", "refresh_token": "r2", "expires_in": 3600}))
    out = flow.refresh_access_token("https://t", "cid", "tok")
    assert out["access_token"] == "a" and out["refresh_token"] == "r2"


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-q"]))
