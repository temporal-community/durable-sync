"""Launch OAuthTokenWorkflow from the credentials bootstrap saved.

    PYTHONPATH=. python -m durable_sync.destinations.notion.start

Reads the bootstrap creds, starts the single long-running auth workflow, and
hands ownership of the refresh token to it. After this, the worker keeps access
tokens fresh unattended; the local file is no longer the source of truth.
"""
from __future__ import annotations

import asyncio

from durable_sync import config
from durable_sync.auth.oauth.workflow import AuthParams, OAuthTokenWorkflow
from durable_sync.destinations.notion import store
from durable_sync.temporal_client import connect


async def main() -> None:
    creds = store.load()
    if not creds:
        raise SystemExit(
            f"No credentials at {store.path()}. Run the bootstrap first:\n"
            f"  PYTHONPATH=. python -m durable_sync.destinations.notion.bootstrap"
        )

    client = await connect()
    handle = await client.start_workflow(
        OAuthTokenWorkflow.run,
        AuthParams(
            client_id=creds["client_id"],
            token_endpoint=creds["token_endpoint"],
            refresh_token=creds["refresh_token"],
        ),
        id=config.NOTION_AUTH_WORKFLOW_ID,
        task_queue=config.TASK_QUEUE,
    )
    print(
        f"Started OAuthTokenWorkflow (id={handle.id}). It now owns the refresh "
        f"token and keeps access tokens fresh.\n"
        f"Verify:  temporal workflow query --workflow-id {handle.id} --type get_access_token"
    )


if __name__ == "__main__":
    asyncio.run(main())
