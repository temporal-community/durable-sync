"""Jira destination — create/update issues from neutral Records (direct REST,
self-serve API token).

Mirrors the Asana destination's shape: Jira issues have a FIXED schema (summary,
description, assignee, priority, labels, …) plus custom fields addressed by id
(`customfield_NNNNN`), so a neutral Record needs an explicit destination-owned
`field_map`. That mapping living here (not in the source) is the seam as intended.

Idempotency: the source's primary_key is stamped into a native issue **entity
property** (default key "durable-sync"), invisible to users — the clean analog of
Asana's `external.gid`. `query_existing_ids` runs one JQL search that requests
that property inline (no N+1) and maps `property.pk -> issue id`.

Auth: HTTP Basic with email + API token. Self-serve, no admin, no auth workflow —
so this destination defines no aux_workflows/aux_activities.

Requires the `jira` extra:  pip install "durable-sync[jira]"
"""
from __future__ import annotations

import asyncio
import datetime as dt
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import httpx

from durable_sync.core import Record, auth_error_in_chain
from durable_sync.connectors.jira import api

# Native issue fields a field_map value may target directly (everything else must
# be a custom field id). Kept small + explicit on purpose.
_NATIVE_FIELDS = {"labels", "priority", "assignee", "duedate", "components", "versions"}


class JiraDestination:
    name = "jira"

    def __init__(
        self,
        project_key: str,
        *,
        issue_type: str = "Task",
        title_property: str = "Summary",
        body_field: str = "description",        # native field record.body maps to
        field_map: dict[str, Any] | None = None,
        create_only_properties: set[str] | None = None,
        property_key: str = "durable-sync",     # entity-property name holding the FK
        base_url_env: str = "JIRA_BASE_URL",
        email_env: str = "JIRA_EMAIL",
        token_env: str = "JIRA_API_TOKEN",
        pacing_seconds: float = 0.0,
    ):
        self.project_key = project_key
        self.issue_type = issue_type
        self.title_property = title_property
        self.body_field = body_field
        # record-property -> native field name OR {"custom_field": "customfield_NNNNN"}.
        # Unmapped properties are DROPPED (Jira has no arbitrary columns).
        self.field_map = field_map or {}
        self.create_only_properties = create_only_properties or set()
        self.property_key = property_key
        self.base_url_env = base_url_env
        self.email_env = email_env
        self.token_env = token_env
        self.pacing_seconds = pacing_seconds

    @property
    def configured(self) -> bool:
        return bool(
            self.project_key
            and os.environ.get(self.base_url_env)
            and os.environ.get(self.email_env)
            and os.environ.get(self.token_env)
        )

    @property
    def config_hint(self) -> str:
        return (f"Jira project_key / {self.base_url_env} / {self.email_env} / "
                f"{self.token_env} unset")

    @asynccontextmanager
    async def connect(self) -> AsyncIterator["_JiraSession"]:
        base = os.environ.get(self.base_url_env, "").rstrip("/")
        headers = api.build_headers(os.environ.get(self.email_env), os.environ.get(self.token_env))
        async with httpx.AsyncClient(base_url=base, headers=headers, timeout=30) as client:
            yield _JiraSession(client, self, base, headers)

    @staticmethod
    def is_auth_error(err: BaseException) -> bool:
        """A rejected token / insufficient permission (401/403). Shared word-boundary
        matcher — Jira errors carry ids, so a bare `"401" in msg` would false-positive."""
        return auth_error_in_chain(err)


class _JiraSession:
    def __init__(self, client: httpx.AsyncClient, dest: JiraDestination, base: str, headers: dict):
        self._client = client
        self._d = dest
        self._base = base
        self._headers = headers

    async def query_existing_ids(self) -> dict[str, str]:
        """{ primary_key (from the entity property) -> issue id } for the project.
        One paginated JQL pass over the project, reading the FK property INLINE off
        each issue (the search `properties` request returns it) — no N+1.

        NB: we scope by project and read the property inline; we do NOT *filter* on
        it in JQL. Entity properties set via the REST API are not JQL-indexed unless
        a Forge/Connect app registers them, so `issue.property[...] IS NOT EMPTY`
        silently matches nothing — but the inline `properties` value is always
        returned. Issues without our property are skipped client-side (like Asana
        reading `external.gid` off every task in the project)."""
        d = self._d
        jql = f'project = "{d.project_key}" ORDER BY created ASC'
        mapping: dict[str, str] = {}
        token: str | None = None
        while True:
            issues, token = await api.search_page(
                self._client, self._base, self._headers, jql,
                next_token=token, fields=["id"], properties=[d.property_key])
            for issue in issues:
                prop = (issue.get("properties") or {}).get(d.property_key) or {}
                pk = prop.get("pk")
                if pk and issue.get("id"):
                    mapping[str(pk)] = str(issue["id"])
            if not token:
                return mapping

    async def create(self, record: Record, synced_at: dt.datetime) -> bool:
        d = self._d
        fields = _encode_issue(d, record, creating=True)
        issue_id = await api.create_issue(self._client, self._base, self._headers, fields)
        if not issue_id:
            raise RuntimeError(
                f"Jira create for primary_key {record.primary_key!r} returned no issue "
                "id; cannot stamp the idempotency property (would duplicate on retry)"
            )
        # Stamp the FK so the NEXT sync recognizes this issue as already-synced. If
        # this fails after the create, fail loudly rather than silently risk a dup.
        await api.set_issue_property(
            self._client, self._base, self._headers, issue_id, d.property_key,
            {"pk": record.primary_key})
        await self._pace()
        return True

    async def update(self, existing_id: str, record: Record, synced_at: dt.datetime) -> bool:
        fields = _encode_issue(self._d, record, creating=False)
        await api.update_issue(self._client, self._base, self._headers, existing_id, fields)
        await self._pace()
        return True

    async def _pace(self) -> None:
        if self._d.pacing_seconds > 0:
            await asyncio.sleep(self._d.pacing_seconds)


def _encode_issue(dest: JiraDestination, record: Record, *, creating: bool) -> dict[str, Any]:
    """Neutral Record -> Jira issue `fields`. Pure (no IO) so it's unit-testable.

    title_property -> summary; record.body -> body_field as ADF; mapped props ->
    native fields or custom fields; UNMAPPED props are dropped (Jira has no
    arbitrary columns). On create we also set project + issuetype. On update,
    create-only properties are skipped so human edits in Jira survive."""
    props = record.properties
    fields: dict[str, Any] = {}

    summary = props.get(dest.title_property)
    if summary is not None:
        fields["summary"] = str(summary)
    if record.body:
        fields[dest.body_field] = api.text_to_adf(record.body)

    for key, val in props.items():
        if key == dest.title_property or val is None:
            continue
        if not creating and key in dest.create_only_properties:
            continue
        target = dest.field_map.get(key)
        if target is None:
            continue  # unmapped -> dropped
        if isinstance(target, dict) and "custom_field" in target:
            fields[target["custom_field"]] = _coerce(val)
        elif target in _NATIVE_FIELDS:
            fields[target] = _coerce_native(target, val)

    if creating:
        fields["project"] = {"key": dest.project_key}
        fields["issuetype"] = {"name": dest.issue_type}
    return fields


def _coerce(val: Any) -> Any:
    """Custom-field value: numbers pass through; lists stay lists (Jira labels /
    multi-value fields take arrays); everything else stringifies."""
    if isinstance(val, bool):
        return str(val)
    if isinstance(val, (int, float)):
        return val
    if isinstance(val, (list, tuple)):
        return list(val)
    return str(val)


def _coerce_native(field: str, val: Any) -> Any:
    if field == "labels":
        # Jira labels are an array of strings and may not contain spaces.
        items = val if isinstance(val, (list, tuple)) else [val]
        return [str(v).replace(" ", "_") for v in items]
    if field in ("priority", "assignee"):
        # Reference fields take an object; map a plain string to the common key.
        return {"name": str(val)} if field == "priority" else {"accountId": str(val)}
    if field == "duedate":
        return str(val)[:10]        # YYYY-MM-DD
    return str(val)
