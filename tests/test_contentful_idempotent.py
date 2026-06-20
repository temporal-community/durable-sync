"""ContentfulDestination idempotent create: deterministic id + PUT-with-id.

Verifies the request SEQUENCE against a fake httpx client (no network): a create
on a missing entry does GET(404) -> PUT(create, content-type header); a retry (or
update) on an existing entry does GET(200) -> PUT(update, version header) — so a
retried create can't duplicate. The deterministic id is a pure sha256 of the key.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import hashlib

import httpx

from durable_sync.core import Record
from durable_sync.linkstore import InMemoryLinkStore
from durable_sync.connectors.contentful import ContentfulDestination
from durable_sync.connectors.contentful.destination import _ContentfulSession
from durable_sync.connectors.contentful.encode import deterministic_entry_id

NOW = dt.datetime(2026, 6, 20, tzinfo=dt.timezone.utc)


class FakeClient:
    """Minimal stand-in for httpx.AsyncClient: scripted responses by (method) and a
    recorded call log. `exists` toggles GET 404 vs 200."""

    def __init__(self, *, exists: bool):
        self.exists = exists
        self.calls: list[tuple[str, str, dict]] = []

    async def request(self, method, url, *, headers=None, params=None, json=None):
        self.calls.append((method, url, headers or {}))
        req = httpx.Request(method, url)
        if method == "GET":
            if self.exists:
                return httpx.Response(200, json={"sys": {"version": 7}}, request=req)
            return httpx.Response(404, json={"message": "no"}, request=req)
        if method == "PUT":
            return httpx.Response(200, json={"sys": {"id": url.rsplit("/", 1)[-1], "version": 8}}, request=req)
        return httpx.Response(204, request=req)


def _dest():
    return ContentfulDestination(
        space_id="space1", content_type="blogPost",
        field_map={"Name": "title"}, link_store=InMemoryLinkStore(),
    )


def _session(client, dest):
    return _ContentfulSession(client, dest)


def test_deterministic_id_is_pure_sha256():
    assert deterministic_entry_id("repo-42") == hashlib.sha256(b"repo-42").hexdigest()
    assert len(deterministic_entry_id("anything")) == 64  # fits Contentful's 64-char limit


def test_create_on_missing_entry_puts_with_content_type():
    dest = _dest()
    client = FakeClient(exists=False)
    rec = Record(primary_key="repo-42", properties={"Name": "Hi"})
    eid = deterministic_entry_id("repo-42")

    asyncio.run(_session(client, dest).create(rec, NOW))

    methods = [c[0] for c in client.calls]
    assert methods == ["GET", "PUT"]                       # checked existence, then created
    put_headers = client.calls[1][2]
    assert client.calls[1][1].endswith(f"/entries/{eid}")  # PUT at the deterministic id
    assert "X-Contentful-Content-Type" in put_headers      # create path
    assert "X-Contentful-Version" not in put_headers
    assert asyncio.run(dest.link_store.get_all()) == {"repo-42": eid}


def test_retried_create_on_existing_entry_updates_not_duplicates():
    dest = _dest()
    client = FakeClient(exists=True)  # first attempt already created it
    rec = Record(primary_key="repo-42", properties={"Name": "Hi"})
    eid = deterministic_entry_id("repo-42")

    asyncio.run(_session(client, dest).create(rec, NOW))

    methods = [c[0] for c in client.calls]
    assert methods == ["GET", "PUT"]                       # same id -> update, NOT a 2nd create
    put_headers = client.calls[1][2]
    assert put_headers.get("X-Contentful-Version") == "7"  # optimistic-lock update
    assert "X-Contentful-Content-Type" not in put_headers
    assert client.calls[1][1].endswith(f"/entries/{eid}")
