"""Launch OAuthTokenWorkflow from the saved Contentful bootstrap credentials.

    PYTHONPATH=. python -m durable_sync.connectors.contentful.start

Reads the bootstrap creds, starts the long-running auth workflow that owns the
rotating refresh token, and serves fresh access tokens via query. After this, a
worker hosting ContentfulMcpDestination keeps tokens fresh unattended.
"""
from __future__ import annotations

import asyncio

from durable_sync import config
from durable_sync.auth.oauth.workflow import AuthParams, OAuthTokenWorkflow
from durable_sync.connectors.contentful import store
from durable_sync.temporal_client import connect


async def main() -> None:
    creds = store.load()
    if not creds:
        raise SystemExit(
            f"No credentials at {store.path()}. Run the bootstrap first:\n"
            f"  PYTHONPATH=. python -m durable_sync.connectors.contentful.bootstrap"
        )

    client = await connect()
    handle = await client.start_workflow(
        OAuthTokenWorkflow.run,
        AuthParams(
            client_id=creds["client_id"],
            token_endpoint=creds["token_endpoint"],
            refresh_token=creds["refresh_token"],
        ),
        id=config.CONTENTFUL_AUTH_WORKFLOW_ID,
        task_queue=config.TASK_QUEUE,
    )
    print(
        f"Started OAuthTokenWorkflow (id={handle.id}). It now owns the Contentful "
        f"refresh token and keeps access tokens fresh.\n"
        f"Verify:  temporal workflow query --workflow-id {handle.id} --type get_access_token"
    )


if __name__ == "__main__":
    asyncio.run(main())
