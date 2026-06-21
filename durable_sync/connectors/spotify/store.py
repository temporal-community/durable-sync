"""Spotify binding of the generic creds store (durable_sync.auth.oauth.store).

Pins Spotify's auth file path; bootstrap/start call load()/save()/path(). The file
holds the handoff refresh token only until `start` gives it to OAuthTokenWorkflow;
it is gitignored, never commit it.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from durable_sync.auth.oauth import store as _store

_FILE = os.getenv("DURABLE_SYNC_SPOTIFY_AUTH_FILE", ".spotify_auth.json")


def load() -> dict[str, Any] | None:
    return _store.load(_FILE)


def save(data: dict[str, Any]) -> None:
    _store.save(_FILE, data)


def path() -> Path:
    return _store.resolve(_FILE)
