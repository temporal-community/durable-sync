"""ContentfulMcpDestination — write entries to Contentful over its MCP server.

The no-admin / SSO-blocked path: where a static CMA token can't reach the space,
OAuth-as-an-individual through the MCP server can. Create + idempotent update are
live-verified; `publish` is optional and TOLERANT — the Contentful MCP app
installation has its own per-tool permission layer (separate from OAuth scopes),
so `publish_entry` may be disallowed by a space admin. We create/update the entry
regardless and only skip publishing with a warning when it's gated.

Idempotency uses a LinkStore (primary_key -> entry id), like the REST destination
and Luma — Contentful field ids don't match neutral property names. Reuses the
shared `encode_fields` for the wire shape. `token_provider` yields the OAuth access
token (e.g. a query to the workflow-owned token; the smoke passes one directly).

Requires the `contentful` extra.
"""
from __future__ import annotations

import datetime as dt
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from durable_sync.core import Record, auth_error_in_chain
from durable_sync.linkstore import LinkStore
from durable_sync.transport.mcp import TokenProvider
from durable_sync.connectors.contentful.encode import encode_fields
from durable_sync.connectors.contentful.mcp import ContentfulMcp, open_contentful
from durable_sync.connectors.contentful.token import current_access_token

log = logging.getLogger("durable_sync.connectors.contentful.mcp_destination")


class ContentfulMcpDestination:
    name = "contentful"

    def __init__(
        self,
        *,
        space_id: str,
        content_type: str,
        field_map: dict[str, str],
        link_store: LinkStore,
        token_provider: TokenProvider | None = None,
        environment: str = "master",
        default_locale: str = "en-US",
        create_only_properties: set[str] | None = None,
        publish: bool = False,
    ):
        self.space_id = space_id
        self.content_type = content_type
        self.field_map = field_map
        self.link_store = link_store
        self.environment = environment
        self.default_locale = default_locale
        self.create_only_properties = create_only_properties or set()
        self.publish = publish
        # Default: query the workflow that owns the Contentful OAuth token (started
        # via connectors.contentful.start), so a worker runs unattended.
        self._token_provider = token_provider or current_access_token

    @property
    def configured(self) -> bool:
        return bool(self.space_id and self.content_type)

    @property
    def config_hint(self) -> str:
        return "Contentful space id / content type unset"

    @asynccontextmanager
    async def connect(self) -> AsyncIterator["_McpSession"]:
        async with open_contentful(self.space_id, self.environment, self._token_provider) as cf:
            yield _McpSession(cf, self)

    @staticmethod
    def is_auth_error(err: BaseException) -> bool:
        return auth_error_in_chain(err)

    # The worker registers these so the Contentful token-owner workflow runs
    # alongside the sync (same OAuth-as-a-workflow toolkit as Notion).
    def aux_workflows(self) -> list:
        from durable_sync.auth.oauth.workflow import OAuthTokenWorkflow
        return [OAuthTokenWorkflow]

    def aux_activities(self) -> list:
        from durable_sync.auth.oauth.refresh import refresh_oauth_token
        return [refresh_oauth_token]


class _McpSession:
    def __init__(self, cf: ContentfulMcp, dest: ContentfulMcpDestination):
        self._cf = cf
        self._d = dest

    async def query_existing_ids(self) -> dict[str, str]:
        return await self._d.link_store.get_all()

    def _fields(self, record: Record, *, creating: bool):
        return encode_fields(
            record, field_map=self._d.field_map, default_locale=self._d.default_locale,
            create_only_properties=self._d.create_only_properties, creating=creating,
        )

    async def create(self, record: Record, synced_at: dt.datetime) -> bool:
        entry_id = await self._cf.create_entry(self.content_type, self._fields(record, creating=True))
        if not entry_id:
            raise RuntimeError("Contentful MCP create_entry: could not determine the new entry id")
        await self._d.link_store.put(record.primary_key, entry_id)
        await self._maybe_publish(entry_id)
        return True

    async def update(self, existing_id: str, record: Record, synced_at: dt.datetime) -> bool:
        version = await self._cf.entry_version(existing_id)
        if version is None:
            raise RuntimeError(f"Contentful MCP: could not read version for entry {existing_id}")
        await self._cf.update_entry(existing_id, self._fields(record, creating=False), version)
        await self._maybe_publish(existing_id)
        return True

    @property
    def content_type(self) -> str:
        return self._d.content_type

    async def _maybe_publish(self, entry_id: str) -> None:
        """Publish if asked — but tolerate the MCP app's per-tool permission gate:
        the entry is already created/updated, so a forbidden publish_entry leaves a
        draft + a warning rather than failing the whole sync."""
        if not self._d.publish:
            return
        try:
            await self._cf.publish_entry(entry_id)
        except RuntimeError as e:
            if "publish_entry" in str(e) or "permission" in str(e).lower():
                log.warning("Contentful publish_entry not permitted for %s (MCP app config) — left as draft: %s",
                            entry_id, e)
                return
            raise
