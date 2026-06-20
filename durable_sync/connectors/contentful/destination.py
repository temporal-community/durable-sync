"""ContentfulDestination — create/update Contentful entries from neutral Records.

The write half of the Contentful connector (e.g. cross-posting from Notion).
Writes go through the **CMA** (the Delivery API is read-only), which means:
  * a CMA token (write-capable),
  * locale-wrapped field values ({field: {locale: value}}),
  * versioned updates (fetch sys.version, send it as the optimistic-lock header),
  * an explicit publish step (else entries stay drafts).

Like Luma, idempotency uses an injected `LinkStore` (primary_key -> entry id):
the source's neutral property names don't match Contentful's content-model field
ids and we don't want to assume a field exists to stash a key in, so the
correspondence lives in the app-owned store (see the CONTRIBUTING doctrine).

NOTE: the CMA shape here follows the docs but has not been run against a live
space — verify field-locale wrapping, versioning, and publish before relying on
it. The pure encoding (`_encode_fields`) is unit-tested; the HTTP is not.

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
from durable_sync.connectors.contentful.encode import encode_fields

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
        fields = _encode_fields(self._d, record)
        entry_id, version = await api.create_entry(self._client, self._space, self._d.content_type, fields)
        if entry_id:
            await self._d.link_store.put(record.primary_key, entry_id)
            if self._d.publish:
                await api.publish_entry(self._client, self._space, entry_id, version=version)
        await self._pace()
        return True

    async def update(self, existing_id: str, record: Record, synced_at: dt.datetime) -> bool:
        fields = _encode_fields(self._d, record, creating=False)
        version = await api.entry_version(self._client, self._space, existing_id)
        new_version = await api.update_entry(self._client, self._space, existing_id, fields, version=version)
        if self._d.publish:
            await api.publish_entry(self._client, self._space, existing_id, version=new_version)
        await self._pace()
        return True

    async def _pace(self) -> None:
        if self._d.pacing_seconds > 0:
            await asyncio.sleep(self._d.pacing_seconds)


def _encode_fields(dest: ContentfulDestination, record: Record, *, creating: bool = True) -> dict[str, Any]:
    """Neutral Record -> CMA `fields`. Thin wrapper over the shared encoder."""
    return encode_fields(
        record, field_map=dest.field_map, default_locale=dest.default_locale,
        create_only_properties=dest.create_only_properties, creating=creating,
    )
