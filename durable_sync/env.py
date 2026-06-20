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
from pathlib import Path


def load_env(path: str | os.PathLike | None = None) -> None:
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError:
        _load_fallback(path)
        return
    load_dotenv(path) if path else load_dotenv()


def _load_fallback(path: str | os.PathLike | None) -> None:
    p = Path(path) if path else Path(".env")
    if not p.exists():
        return
    for line in p.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
