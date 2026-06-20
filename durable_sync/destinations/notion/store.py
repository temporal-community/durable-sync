"""Notion binding of the generic creds store (durable_sync.auth.store).

Pins Notion's auth file path; bootstrap/prove/start call load()/save()/path().
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from durable_sync.auth import store as _store

_FILE = os.getenv("DURABLE_SYNC_NOTION_AUTH_FILE", ".notion_auth.json")


def load() -> dict[str, Any] | None:
    return _store.load(_FILE)


def save(data: dict[str, Any]) -> None:
    _store.save(_FILE, data)


def path() -> Path:
    return _store.resolve(_FILE)
