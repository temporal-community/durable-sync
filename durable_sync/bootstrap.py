"""Start one entity workflow per source unit. Idempotent: re-running won't
disturb a workflow that's already up (USE_EXISTING), so it doubles as a reconcile.

    from durable_sync.bootstrap import start_sources
    await start_sources(SOURCE)

No Schedule is needed — each workflow's own timer loop is the periodicity. Drive
or inspect them by id, e.g.:

    temporal workflow signal --workflow-id "durable-sync:org:temporal-community" \
        --name sync_now --input '[]'
    temporal workflow query  --workflow-id "durable-sync:org:temporal-community" \
        --type status
"""
from __future__ import annotations

from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy

from durable_sync import config
from durable_sync.core import Source
from durable_sync.temporal_client import connect
from durable_sync.workflows.sync import SourceState, SourceSyncWorkflow


async def start_sources(
    source: Source,
    *,
    client: Client | None = None,
    task_queue: str | None = None,
    id_prefix: str = "durable-sync",
) -> None:
    client = client or await connect()
    tq = task_queue or config.TASK_QUEUE
    for spec in source.specs():
        wf_id = f"{id_prefix}:{spec.key}"
        await client.start_workflow(
            SourceSyncWorkflow.run,
            SourceState(spec=spec),
            id=wf_id,
            task_queue=tq,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
        print(f"ensured entity workflow: {wf_id}")
