"""Asana destination — direct REST, self-serve PAT.

Why this exists: it's the abstraction's stress test. Notion let property names BE
column names; Asana tasks have a FIXED schema (name, notes, due_on, completed) plus
custom fields addressed by gid — so a neutral Record needs an explicit
destination-owned `field_map`. That mapping living here (not in the source) is
exactly the seam working as intended.

Idempotency: the source's primary_key is stored in the task's `external.gid`
(Asana's purpose-built external-system handle). `query_existing_ids` lists the
project's tasks with `opt_fields=external` and maps external.gid -> task gid.

Auth: a Personal Access Token (Bearer). Self-serve, no admin, no auth workflow —
so this destination defines no aux_workflows/aux_activities.

Requires the `asana` extra:  pip install "durable-sync[asana]"
"""
from __future__ import annotations

import asyncio
import datetime as dt
import os
from contextlib import asynccontextmanager
from typing import Any, Awaitable, Callable, AsyncIterator

import httpx

from durable_sync.core import DestinationHTTPError, Record, auth_error_in_chain
from durable_sync.http import request_with_retry

ASANA_API = "https://app.asana.com/api/1.0"
_MAX_NOTES = 65000
_MAX_RETRIES = 6
_BACKOFF_BASE_SECONDS = 1.0

# Native task fields a field_map value may target directly (everything else must
# be a custom field). Kept small + explicit on purpose.
_NATIVE_FIELDS = {"name", "notes", "html_notes", "due_on", "due_at",
                  "start_on", "completed", "assignee", "resource_subtype"}

TokenProvider = Callable[[], Awaitable[str]]
# A field_map value: a native field name ("due_on") OR {"custom_field": "<gid>"}.
FieldTarget = Any


class AsanaDestination:
    name = "asana"

    def __init__(
        self,
        project_gid: str,
        *,
        title_property: str = "Name",
        body_field: str = "notes",            # native field record.body maps to
        field_map: dict[str, FieldTarget] | None = None,
        create_only_properties: set[str] | None = None,
        token_provider: TokenProvider | None = None,
        token_env: str = "ASANA_PAT",
        synced_custom_field_gid: str | None = None,   # optional date CF to stamp
        pacing_seconds: float = 0.0,
    ):
        self.project_gid = project_gid
        self.title_property = title_property
        self.body_field = body_field
        # record-property -> Asana target. Unmapped properties are DROPPED
        # (Asana can't hold arbitrary columns); title/body are handled separately.
        self.field_map = field_map or {}
        self.create_only_properties = create_only_properties or set()
        self.token_env = token_env
        self._token_provider = token_provider or self._env_token
        self.synced_custom_field_gid = synced_custom_field_gid
        self.pacing_seconds = pacing_seconds

    async def _env_token(self) -> str:
        return os.environ.get(self.token_env, "")

    @property
    def configured(self) -> bool:
        return bool(self.project_gid)

    @property
    def config_hint(self) -> str:
        return f"ASANA project gid / {self.token_env} unset"

    @asynccontextmanager
    async def connect(self) -> AsyncIterator["_AsanaSession"]:
        token = await self._token_provider()
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        async with httpx.AsyncClient(base_url=ASANA_API, headers=headers, timeout=30) as client:
            yield _AsanaSession(client, self)

    @staticmethod
    def is_auth_error(err: BaseException) -> bool:
        """A rejected PAT (401/403). Delegates to the shared matcher so we get the
        word-boundary code check for free — Asana errors carry gids/request-ids,
        and a bare `"401" in msg` would false-positive on one. "not authorized" is
        Asana's own phrasing for a permission failure."""
        return auth_error_in_chain(err, extra_needles=("not authorized",))


class _AsanaSession:
    def __init__(self, client: httpx.AsyncClient, dest: AsanaDestination):
        self._client = client
        self._d = dest

    async def _request(self, method: str, path: str, *, params=None, json=None) -> dict:
        # Shared backoff (honors Retry-After); we keep the raise here so the error
        # text carries the status for is_auth_error to classify.
        r = await request_with_retry(
            self._client, method, path, params=params, json=json,
            max_attempts=_MAX_RETRIES, base_delay=_BACKOFF_BASE_SECONDS,
        )
        if r.status_code >= 400:
            raise DestinationHTTPError(
                r.status_code, f"Asana {method} {path} -> {r.status_code}: {r.text[:600]}"
            )
        return r.json() if r.content else {}

    async def query_existing_ids(self) -> dict[str, str]:
        """{ external.gid (== our primary_key) -> task gid } for the project."""
        mapping: dict[str, str] = {}
        params: dict[str, Any] = {
            "project": self._d.project_gid, "opt_fields": "external", "limit": 100,
        }
        while True:
            resp = await self._request("GET", "/tasks", params=params)
            for t in resp.get("data", []):
                ext = t.get("external") or {}
                gid = ext.get("gid")
                if gid and t.get("gid"):
                    mapping[gid] = t["gid"]
            nxt = resp.get("next_page")
            if not nxt or not nxt.get("offset"):
                break
            params["offset"] = nxt["offset"]
        return mapping

    async def create(self, record: Record, synced_at: dt.datetime) -> bool:
        data = _encode_task(self._d, record, synced_at, creating=True)
        await self._request("POST", "/tasks", json={"data": data})
        await self._pace()
        return True

    async def update(self, existing_id: str, record: Record, synced_at: dt.datetime) -> bool:
        data = _encode_task(self._d, record, synced_at, creating=False)
        await self._request("PUT", f"/tasks/{existing_id}", json={"data": data})
        await self._pace()
        return True

    async def _pace(self) -> None:
        if self._d.pacing_seconds > 0:
            await asyncio.sleep(self._d.pacing_seconds)


def _encode_task(
    dest: AsanaDestination, record: Record, synced_at: dt.datetime, *, creating: bool
) -> dict[str, Any]:
    """Neutral Record -> Asana task `data`. Pure (no IO) so it's unit-testable.

    title_property -> name; record.body -> body_field; mapped props -> native
    fields or custom fields; UNMAPPED props are dropped (Asana has no arbitrary
    columns). On create we also set projects + external (idempotency key)."""
    props = record.properties
    data: dict[str, Any] = {}
    custom: dict[str, Any] = {}

    name = props.get(dest.title_property)
    if name is not None:
        data["name"] = str(name)
    if record.body:
        data[dest.body_field] = record.body[:_MAX_NOTES]

    for key, val in props.items():
        if key == dest.title_property or val is None:
            continue
        if not creating and key in dest.create_only_properties:
            continue
        target = dest.field_map.get(key)
        if target is None:
            continue  # unmapped -> dropped (logged at debug by caller if desired)
        if isinstance(target, dict) and "custom_field" in target:
            custom[target["custom_field"]] = _coerce(val)
        elif target in _NATIVE_FIELDS:
            data[target] = _coerce_native(target, val)

    if dest.synced_custom_field_gid:
        custom[dest.synced_custom_field_gid] = synced_at.date().isoformat()
    if custom:
        data["custom_fields"] = custom
    if creating:
        data["projects"] = [dest.project_gid]
        data["external"] = {"gid": record.primary_key}  # idempotency handle
    return data


def _coerce(val: Any) -> Any:
    """Custom-field value: numbers pass through; lists join (enum option gids are
    app-specific, out of scope); everything else stringifies."""
    if isinstance(val, bool):
        return str(val)
    if isinstance(val, (int, float)):
        return val
    if isinstance(val, (list, tuple)):
        return ", ".join(str(v) for v in val)
    return str(val)


def _coerce_native(field: str, val: Any) -> Any:
    if field == "completed":
        return bool(val)
    if field in ("due_on", "start_on"):
        return str(val)[:10]        # YYYY-MM-DD
    return str(val)
