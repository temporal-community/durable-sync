"""Access-token accessor for use INSIDE activities (the Notion destination).

Activities call current_access_token() to get a valid token by querying
NotionAuthWorkflow. The query result is used locally and never returned from the
activity, so the token stays out of Temporal event history.

Not workflow code — safe to do IO and cache a client.
"""
from __future__ import annotations

from temporalio.client import Client

from durable_sync import config
from durable_sync.destinations.notion.auth_workflow import NotionAuthWorkflow
from durable_sync.temporal_client import connect

_client: Client | None = None


async def _get_client() -> Client:
    global _client
    if _client is None:
        _client = await connect()
    return _client


async def current_access_token() -> str:
    """Query NotionAuthWorkflow for a fresh access token. The default
    `token_provider` for NotionDestination."""
    client = await _get_client()
    handle = client.get_workflow_handle(config.NOTION_AUTH_WORKFLOW_ID)
    token = await handle.query(NotionAuthWorkflow.get_access_token)
    if not token:
        raise RuntimeError(
            "NotionAuthWorkflow returned an empty access token — is it running? "
            "Bootstrap + start it (see durable_sync.destinations.notion.bootstrap)."
        )
    return token
