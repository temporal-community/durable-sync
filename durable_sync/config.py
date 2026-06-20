"""Runtime + connection config (generic). Side-effect-free: imported indirectly
into the workflow sandbox, so no IO / no import-time failures.

Integration-specific config (which orgs, which Notion DB, Asana project) lives in
the Source/Destination you wire up — not here.
"""
from __future__ import annotations

import os

TASK_QUEUE = os.environ.get("DURABLE_SYNC_TASK_QUEUE", "durable-sync")

# Temporal connection (defaults to a local dev server; set these for Cloud).
TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
TEMPORAL_NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "default")
TEMPORAL_API_KEY = os.environ.get("TEMPORAL_API_KEY")  # set for Temporal Cloud

# Id of the workflow that owns the Notion OAuth token
# (auth.workflow.OAuthTokenWorkflow, started via connectors.notion.start).
NOTION_AUTH_WORKFLOW_ID = os.environ.get(
    "DURABLE_SYNC_NOTION_AUTH_WORKFLOW_ID", "durable-sync:notion-auth"
)
