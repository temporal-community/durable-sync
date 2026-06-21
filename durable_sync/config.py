"""Runtime + connection config (generic). Side-effect-free: imported indirectly
into the workflow sandbox, so no IO / no import-time failures.

Integration-specific config (which orgs, which Notion DB, Asana project) lives in
the Source/Destination you wire up — not here.
"""
from __future__ import annotations

import os

TASK_QUEUE = os.environ.get("DURABLE_SYNC_TASK_QUEUE", "durable-sync")

# Worker Versioning — OPT-IN, off by default so local/simple runs need zero setup.
# Set DURABLE_SYNC_BUILD_ID (e.g. a git SHA) in production: a redeploy whose
# workflow code changed then only affects NEW/continued executions, while in-flight
# histories drain on the old build — the safe way to evolve the long-lived entity
# workflows (SourceSyncWorkflow / OAuthTokenWorkflow) without non-determinism
# errors. When unset, all workflows run unversioned exactly as before. The
# alternative for in-place changes is workflow.patched() (see CONTRIBUTING).
BUILD_ID = os.environ.get("DURABLE_SYNC_BUILD_ID", "")
DEPLOYMENT_NAME = os.environ.get("DURABLE_SYNC_DEPLOYMENT_NAME", "durable-sync")

# Temporal connection (defaults to a local dev server; set these for Cloud).
TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
TEMPORAL_NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "default")
TEMPORAL_API_KEY = os.environ.get("TEMPORAL_API_KEY")  # set for Temporal Cloud

# Ids of the workflows that own each provider's OAuth token
# (auth.workflow.OAuthTokenWorkflow, started via connectors.<provider>.start).
NOTION_AUTH_WORKFLOW_ID = os.environ.get(
    "DURABLE_SYNC_NOTION_AUTH_WORKFLOW_ID", "durable-sync:notion-auth"
)
CONTENTFUL_AUTH_WORKFLOW_ID = os.environ.get(
    "DURABLE_SYNC_CONTENTFUL_AUTH_WORKFLOW_ID", "durable-sync:contentful-auth"
)
SPOTIFY_AUTH_WORKFLOW_ID = os.environ.get(
    "DURABLE_SYNC_SPOTIFY_AUTH_WORKFLOW_ID", "durable-sync:spotify-auth"
)
