"""Access-token accessor for use INSIDE activities (a destination's session).

current_access_token(workflow_id) queries the OAuthTokenWorkflow with that id for
a valid token. The query result is used locally and never returned from the
activity, so the token stays out of Temporal event history.

Not workflow code — safe to do IO and cache a client.
"""
from __future__ import annotations

from temporalio.client import Client

from durable_sync.auth.workflow import OAuthTokenWorkflow
from durable_sync.temporal_client import connect

_client: Client | None = None


async def _get_client() -> Client:
    global _client
    if _client is None:
        _client = await connect()
    return _client


async def current_access_token(workflow_id: str) -> str:
    """Query the OAuthTokenWorkflow `workflow_id` for a fresh access token."""
    client = await _get_client()
    handle = client.get_workflow_handle(workflow_id)
    token = await handle.query(OAuthTokenWorkflow.get_access_token)
    if not token:
        raise RuntimeError(
            f"OAuthTokenWorkflow '{workflow_id}' returned an empty access token — "
            f"is it running? Bootstrap + start it (see the destination's docs)."
        )
    return token
