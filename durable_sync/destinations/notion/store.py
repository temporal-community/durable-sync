"""Local credential store for the Notion OAuth bootstrap handoff.

This is the BOOTSTRAP/PROOF persistence only. In the running system the refresh
token lives in NotionAuthWorkflow's state (durable, single-owner) — but bootstrap
needs somewhere to hand off the initial token, and prove.py reads it back. File
is gitignored; never commit it.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

_PATH = Path(os.getenv("DURABLE_SYNC_NOTION_AUTH_FILE", ".notion_auth.json"))


def load() -> dict[str, Any] | None:
    if not _PATH.exists():
        return None
    return json.loads(_PATH.read_text())


def save(data: dict[str, Any]) -> None:
    """Write atomically so a crash mid-write can't corrupt the rotating token."""
    tmp = _PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    try:
        tmp.chmod(0o600)
    except OSError:
        pass
    os.replace(tmp, _PATH)


def path() -> Path:
    return _PATH
