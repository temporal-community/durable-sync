"""Notion binding of the generic token accessor (durable_sync.auth.token).

The default token_provider for NotionDestination: query the OAuthTokenWorkflow
running under config.NOTION_AUTH_WORKFLOW_ID for a fresh access token.
"""
from __future__ import annotations

from durable_sync import config
from durable_sync.auth.token import current_access_token as _current


async def current_access_token() -> str:
    return await _current(config.NOTION_AUTH_WORKFLOW_ID)
