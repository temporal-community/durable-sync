"""The refresh activity — the only IO in the Notion auth hot path.

Wraps oauth.refresh_access_token so NotionAuthWorkflow stays deterministic (no
network in the workflow). Returns the new access token AND the rotated refresh
token; the workflow persists both in its state. Kept in its own module so the
workflow can import it via pass-through without dragging `requests` into the
sandbox at workflow-import time.
"""
from __future__ import annotations

from dataclasses import dataclass

from temporalio import activity

from durable_sync.destinations.notion import oauth


@dataclass
class RefreshInput:
    client_id: str
    token_endpoint: str
    refresh_token: str


@dataclass
class RefreshOutput:
    access_token: str
    refresh_token: str  # rotated — the workflow MUST store this
    expires_in: int


@activity.defn
def refresh_notion_token(inp: RefreshInput) -> RefreshOutput:
    tokens = oauth.refresh_access_token(
        inp.token_endpoint, inp.client_id, inp.refresh_token
    )
    return RefreshOutput(
        access_token=tokens["access_token"],
        refresh_token=tokens["refresh_token"],
        expires_in=int(tokens.get("expires_in", 3600)),
    )
