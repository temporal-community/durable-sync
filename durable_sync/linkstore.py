"""Idempotency link store: source primary_key <-> destination object id.

Most destinations key idempotency on the object itself (Notion's key column,
Asana's `external.gid`). But some systems can't store a foreign key on their
objects (Luma events; Contentful unless you bend the content model) — so the
correspondence has to live OUTSIDE, in a LinkStore.

The library owns the SEAM (this protocol) + reference impls, because remembering a
link the sync itself minted is idempotency — mechanism, not policy (see the
boundary doctrine in CONTRIBUTING). The DURABLE store in production is usually the
app's: back it with your real datastore, especially multi-node.

Note we deliberately do NOT back this with a Temporal workflow: a workflow would
hold an unbounded, ever-growing map in its state/history — an anti-pattern. A
KV/DB is the right backing. `SqliteLinkStore` is a dependency-free single-node
reference; swap your own for scale.
"""
from __future__ import annotations

import asyncio
import sqlite3
from typing import Protocol, runtime_checkable


@runtime_checkable
class LinkStore(Protocol):
    """Durable map of source primary_key -> destination object id. Async so an
    implementation can be DB-, KV-, or service-backed."""

    async def get_all(self) -> dict[str, str]: ...
    async def put(self, primary_key: str, dest_id: str) -> None: ...


class InMemoryLinkStore:
    """Dev/test only — NOT durable. Loses its map on restart, which for an FK-less
    destination means DUPLICATE writes after a restart. Never use in production."""

    def __init__(self) -> None:
        self._m: dict[str, str] = {}

    async def get_all(self) -> dict[str, str]:
        return dict(self._m)

    async def put(self, primary_key: str, dest_id: str) -> None:
        self._m[primary_key] = dest_id


class SqliteLinkStore:
    """LOCAL-DEV reference impl (stdlib sqlite3) — durable across restarts on ONE
    machine. Do NOT use on Temporal Cloud or any multi-worker deployment: workers
    are distributed and ephemeral with no shared local disk, so a links.db on one
    worker is invisible to the others. Production wants an external SHARED
    datastore (Postgres/Dynamo/Redis/…) behind the LinkStore seam — that's the
    app's to own (see the boundary doctrine). Namespaced by `route` so several
    routes can share one file; SQLite calls run in a thread, serialized by a lock."""

    def __init__(self, path: str, *, route: str = "default") -> None:
        self._path = path
        self._route = route
        self._lock = asyncio.Lock()
        with sqlite3.connect(self._path) as c:
            c.execute(
                "CREATE TABLE IF NOT EXISTS links "
                "(route TEXT, primary_key TEXT, dest_id TEXT, "
                "PRIMARY KEY (route, primary_key))"
            )

    async def get_all(self) -> dict[str, str]:
        async with self._lock:
            return await asyncio.to_thread(self._get_all)

    def _get_all(self) -> dict[str, str]:
        with sqlite3.connect(self._path) as c:
            rows = c.execute(
                "SELECT primary_key, dest_id FROM links WHERE route = ?", (self._route,)
            ).fetchall()
        return {pk: did for pk, did in rows}

    async def put(self, primary_key: str, dest_id: str) -> None:
        async with self._lock:
            await asyncio.to_thread(self._put, primary_key, dest_id)

    def _put(self, primary_key: str, dest_id: str) -> None:
        with sqlite3.connect(self._path) as c:
            c.execute(
                "INSERT OR REPLACE INTO links(route, primary_key, dest_id) VALUES (?, ?, ?)",
                (self._route, primary_key, dest_id),
            )
