"""Contentful binding of the generic creds store (durable_sync.auth.oauth.store).

Pins Contentful's auth file path; bootstrap/prove call load()/save()/path().
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from durable_sync.auth.oauth import store as _store

_FILE = os.getenv("DURABLE_SYNC_CONTENTFUL_AUTH_FILE", ".contentful_auth.json")


def load() -> dict[str, Any] | None:
    return _store.load(_FILE)


def save(data: dict[str, Any]) -> None:
    _store.save(_FILE, data)


def path() -> Path:
    return _store.resolve(_FILE)
