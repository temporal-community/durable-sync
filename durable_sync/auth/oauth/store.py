"""Local credential store for the OAuth bootstrap handoff (provider-agnostic).

BOOTSTRAP/PROOF persistence only. In the running system the refresh token lives
in OAuthTokenWorkflow's state (durable, single-owner) — but bootstrap needs
somewhere to hand off the initial token, and prove reads it back. The file is
gitignored; never commit it. Each provider passes its own `file` path.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def load(file: str) -> dict[str, Any] | None:
    p = Path(file)
    if not p.exists():
        return None
    return json.loads(p.read_text())


def save(file: str, data: dict[str, Any]) -> None:
    """Write atomically so a crash mid-write can't corrupt the rotating token."""
    p = Path(file)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    try:
        tmp.chmod(0o600)
    except OSError:
        pass
    os.replace(tmp, p)


def resolve(file: str) -> Path:
    return Path(file)
