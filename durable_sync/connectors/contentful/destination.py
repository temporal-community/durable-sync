"""ContentfulDestination — create/update Contentful entries from neutral Records.

The write half of the Contentful connector (e.g. cross-posting from Notion).
Writes go through the **CMA** (the Delivery API is read-only), which means:
  * a CMA token (write-capable),
  * locale-wrapped field values ({field: {locale: value}}),
  * versioned updates (fetch sys.version, send it as the optimistic-lock header),
  * an explicit publish step (else entries stay drafts).

Idempotency: the entry id is a DETERMINISTIC function of the source primary_key
(`deterministic_entry_id`), and creates go through `PUT /entries/{id}` (Contentful
lets you choose the id). So a create that's retried after a crash re-derives the
same id and UPDATES that entry rather than duplicating it — at-least-once safe
without trusting the LinkStore to have been written. The injected `LinkStore` is
still used for query_existing_ids (to route known rows straight to update), but it
is now just an optimization: even an empty/lost store can't cause duplicates.

NOTE: the CMA shape here follows the docs but has not been run against a live
space — verify field-locale wrapping, the PUT-with-id create, versioning, and
publish before relying on it. The pure encoding + id derivation are unit-tested;
the HTTP request sequence is unit-tested against a fake client but not live.

Requires the `contentful` extra.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import httpx

from durable_sync.core import Record, auth_error_in_chain
from durable_sync.linkstore import LinkStore
from durable_sync.connectors.contentful import api
from durable_sync.connectors.contentful.api import ContentfulSpace
from durable_sync.connectors.contentful.encode import deterministic_entry_id, encode_fields

_CMA_CONTENT_TYPE = "application/vnd.contentful.management.v1+json"


class ContentfulDestination:
    name = "contentful"

    def __init__(
        self,
        *,
        space_id: str,
        content_type: str,                       # the content-type id to create entries as
        field_map: dict[str, str],               # neutral property name -> CMA field id
        link_store: LinkStore,                   # REQUIRED — correspondence lives outside Contentful
        cma_token_env: str = "CONTENTFUL_CMA_TOKEN",
        environment: str = "master",
        default_locale: str = "en-US",
        create_only_properties: set[str] | None = None,
        publish: bool = False,                   # publish on write, or leave as draft
        pacing_seconds: float = 0.0,
    ):
        self.space_id = space_id
        self.content_type = content_type
        self.field_map = field_map
        self.link_store = link_store
        self.cma_token_env = cma_token_env
        self.environment = environment
        self.default_locale = default_locale
        self.create_only_properties = create_only_properties or set()
        self.publish = publish
        self.pacing_seconds = pacing_seconds

    @property
    def configured(self) -> bool:
        return bool(self.space_id and os.environ.get(self.cma_token_env))

    @property
    def config_hint(self) -> str:
        return f"Contentful space id / {self.cma_token_env} unset"

    def _space(self) -> ContentfulSpace:
        return ContentfulSpace(
            space_id=self.space_id, environment=self.environment,
            default_locale=self.default_locale,
            cma_token=os.environ.get(self.cma_token_env, ""),
        )

    @asynccontextmanager
    async def connect(self) -> AsyncIterator["_ContentfulSession"]:
        headers = {
            "Authorization": f"Bearer {os.environ.get(self.cma_token_env, '')}",
            "Content-Type": _CMA_CONTENT_TYPE,
        }
        async with httpx.AsyncClient(headers=headers, timeout=30) as client:
            yield _ContentfulSession(client, self)

    @staticmethod
    def is_auth_error(err: BaseException) -> bool:
        return auth_error_in_chain(err)


class _ContentfulSession:
    def __init__(self, client: httpx.AsyncClient, dest: ContentfulDestination):
        self._client = client
        self._d = dest
        self._space = dest._space()

    async def query_existing_ids(self) -> dict[str, str]:
        return await self._d.link_store.get_all()

    async def create(self, record: Record, synced_at: dt.datetime) -> bool:
        # Idempotent: the entry id is a pure function of primary_key, so a retried
        # create (after a crash) re-derives the SAME id and updates the entry the
        # first attempt made instead of duplicating it. We still record the link so
        # query_existing_ids routes future syncs straight to update().
        entry_id = deterministic_entry_id(record.primary_key)
        await self._upsert(entry_id, record, creating=True)
        await self._d.link_store.put(record.primary_key, entry_id)
        await self._pace()
        return True

    async def update(self, existing_id: str, record: Record, synced_at: dt.datetime) -> bool:
        await self._upsert(existing_id, record, creating=False)
        await self._pace()
        return True

    async def _upsert(self, entry_id: str, record: Record, *, creating: bool) -> None:
        fields = _encode_fields(self._d, record, creating=creating)
        version = await api.entry_version_or_none(self._client, self._space, entry_id)
        new_version = await api.upsert_entry(
            self._client, self._space, entry_id, self._d.content_type, fields, version=version
        )
        if self._d.publish:
            await api.publish_entry(self._client, self._space, entry_id, version=new_version)

    async def _pace(self) -> None:
        if self._d.pacing_seconds > 0:
            await asyncio.sleep(self._d.pacing_seconds)


def _encode_fields(dest: ContentfulDestination, record: Record, *, creating: bool = True) -> dict[str, Any]:
    """Neutral Record -> CMA `fields`. Thin wrapper over the shared encoder."""
    return encode_fields(
        record, field_map=dest.field_map, default_locale=dest.default_locale,
        create_only_properties=dest.create_only_properties, creating=creating,
    )
