"""Worker assembly. The app wires its Source + Destination; this builds a Worker
that hosts the generic SourceSyncWorkflow + activities, PLUS any auxiliary
workflows/activities the destination needs (e.g. the Notion auth workflow).

    from durable_sync.worker import run_worker
    asyncio.run(run_worker(SOURCE, DESTINATION))

A destination MAY expose `aux_workflows()` / `aux_activities()` (returning extra
Temporal workflows/activities to register). Destinations without them (e.g. a
REST/PAT one) just don't define them.
"""
from __future__ import annotations

from temporalio.client import Client
from temporalio.worker import Worker

from durable_sync import config
from durable_sync.activities import make_activities
from durable_sync.core import Destination, Source
from durable_sync.temporal_client import connect
from durable_sync.workflows.sync import SourceSyncWorkflow


def make_worker(
    client: Client,
    source: Source,
    destination: Destination,
    *,
    task_queue: str | None = None,
    transform=None,
) -> Worker:
    activities = make_activities(source, destination, transform=transform)
    workflows: list = [SourceSyncWorkflow]

    aux_acts = getattr(destination, "aux_activities", None)
    if callable(aux_acts):
        activities = activities + list(aux_acts())
    aux_wfs = getattr(destination, "aux_workflows", None)
    if callable(aux_wfs):
        workflows = workflows + list(aux_wfs())

    return Worker(
        client,
        task_queue=task_queue or config.TASK_QUEUE,
        workflows=workflows,
        activities=activities,
        # Activities are async, so no activity_executor is required.
    )


async def run_worker(
    source: Source,
    destination: Destination,
    *,
    task_queue: str | None = None,
    transform=None,
) -> None:
    client = await connect()
    worker = make_worker(client, source, destination, task_queue=task_queue, transform=transform)
    tq = task_queue or config.TASK_QUEUE
    print(f"Worker polling task queue '{tq}' on {config.TEMPORAL_ADDRESS}")
    await worker.run()
