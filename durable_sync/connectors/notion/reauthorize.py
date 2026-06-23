"""Heal the OAuthTokenWorkflow in place after a re-bootstrap — signal it the fresh
creds so it resumes without a terminate/re-seed, even if the re-bootstrap minted a
NEW OAuth client (the signal carries client_id + token_endpoint, not just the token).

    PYTHONPATH=. python -m durable_sync.connectors.notion.bootstrap     # browser re-consent
    PYTHONPATH=. python -m durable_sync.connectors.notion.reauthorize   # heal in place

Use this instead of terminate + start when the grant was revoked/expired/spent — the
workflow stays the same instance (keeps its history/counters) and just picks up the
fresh creds on its next refresh.
"""
from __future__ import annotations

import asyncio

from durable_sync import config
from durable_sync.auth.oauth.workflow import OAuthTokenWorkflow
from durable_sync.connectors.notion import store
from durable_sync.temporal_client import connect


async def main() -> None:
    creds = store.load()
    if not creds:
        raise SystemExit(
            f"No credentials at {store.path()}. Re-run the bootstrap first:\n"
            f"  PYTHONPATH=. python -m durable_sync.connectors.notion.bootstrap"
        )
    client = await connect()
    handle = client.get_workflow_handle(config.NOTION_AUTH_WORKFLOW_ID)
    await handle.signal(
        OAuthTokenWorkflow.reauthorize,
        args=[creds["refresh_token"], creds["client_id"], creds["token_endpoint"]],
    )
    print(
        f"Sent reauthorize to {config.NOTION_AUTH_WORKFLOW_ID} (fresh refresh token + "
        f"client_id + token_endpoint). It resumes on the next refresh.\n"
        f"Verify:  temporal workflow query --workflow-id {config.NOTION_AUTH_WORKFLOW_ID} --type status"
    )


if __name__ == "__main__":
    asyncio.run(main())
