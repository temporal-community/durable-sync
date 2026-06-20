"""Load a local `.env` into os.environ — dev convenience for `python -m …` tools
and the live smokes, so each script doesn't roll its own (which led to scripts
that silently ignored a populated `.env`).

Idempotent, never overrides an already-set var, no-op if there's no `.env`. Uses
python-dotenv when present (the `dev` extra); falls back to a tiny built-in parser
so it still works in a minimal install. Run scripts from the repo root.

NOT imported by config.py / the workflow sandbox — this does file IO, so it stays
out of the deterministic path and is called explicitly by scripts only.
"""
from __future__ import annotations

import os
import stat
import sys
from pathlib import Path


def load_env(path: str | os.PathLike | None = None) -> None:
    _warn_if_world_readable(Path(path) if path else Path(".env"))
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError:
        _load_fallback(path)
        return
    load_dotenv(path) if path else load_dotenv()


def _warn_if_world_readable(p: Path) -> None:
    """The `.env` is the documented home for DURABLE_SYNC_ENC_KEY (the AES master
    key) and connector PATs. The token JSON stores are chmod'd 0o600; the `.env`
    must be too, or a local user reads the key that decrypts every token. Warn
    (don't fail — a CI runner with injected env vars may have no `.env`)."""
    try:
        mode = p.stat().st_mode
    except OSError:
        return  # no file / not readable — nothing loaded from it
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        print(
            f"WARNING: {p} is group/other-accessible (mode {oct(mode & 0o777)}); "
            f"it may hold secrets (DURABLE_SYNC_ENC_KEY, PATs). Run: chmod 600 {p}",
            file=sys.stderr,
        )


def _load_fallback(path: str | os.PathLike | None) -> None:
    p = Path(path) if path else Path(".env")
    if not p.exists():
        return
    for line in p.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
