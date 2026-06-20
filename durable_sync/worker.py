"""Worker assembly. The app wires its Source + Destination; this builds a Worker
that hosts the generic SourceSyncWorkflow + activities, PLUS any auxiliary
workflows/activities the destination needs (e.g. the Notion auth workflow).

    from durable_sync.worker import run_worker
    asyncio.run(run_worker(SOURCE, DESTINATION))

A destination MAY expose `aux_workflows()` / `aux_activities()` (returning extra
Temporal workflows/activities to register). Destinations without them (e.g. a
REST/PAT one) just don't define them.

Some activities are SYNC (e.g. the Notion OAuth refresh uses `requests`), so the
worker is given a thread-pool `activity_executor`. Async activities still run on
the event loop; sync ones run in the pool.
"""
from __future__ import annotations

import concurrent.futures

from temporalio.client import Client
from temporalio.worker import Worker

from durable_sync import config
from durable_sync.activities import make_activities
from durable_sync.core import Destination, Source
from durable_sync.temporal_client import connect
from durable_sync.workflows.sync import SourceSyncWorkflow

_ACTIVITY_WORKERS = 50


def make_worker(
    client: Client,
    source: Source,
    destination: Destination,
    *,
    task_queue: str | None = None,
    transform=None,
    activity_executor: concurrent.futures.Executor | None = None,
    max_concurrent_activities: int | None = None,
) -> Worker:
    activities = make_activities(source, destination, transform=transform)
    workflows: list = [SourceSyncWorkflow]

    aux_acts = getattr(destination, "aux_activities", None)
    if callable(aux_acts):
        activities = activities + list(aux_acts())
    aux_wfs = getattr(destination, "aux_workflows", None)
    if callable(aux_wfs):
        workflows = workflows + list(aux_wfs())

    extra: dict = {}
    # Required if ANY activity is sync (e.g. a destination's OAuth refresh). Harmless
    # for all-async workers. max_concurrent_activities is capped to the pool size.
    if activity_executor is not None:
        extra["activity_executor"] = activity_executor
        if max_concurrent_activities is not None:
            extra["max_concurrent_activities"] = max_concurrent_activities

    return Worker(
        client,
        task_queue=task_queue or config.TASK_QUEUE,
        workflows=workflows,
        activities=activities,
        **extra,
    )


async def run_worker(
    source: Source,
    destination: Destination,
    *,
    task_queue: str | None = None,
    transform=None,
) -> None:
    client = await connect()
    tq = task_queue or config.TASK_QUEUE
    # A thread pool so sync activities (e.g. Notion's OAuth refresh) can run.
    with concurrent.futures.ThreadPoolExecutor(max_workers=_ACTIVITY_WORKERS) as executor:
        worker = make_worker(
            client, source, destination, task_queue=task_queue, transform=transform,
            activity_executor=executor, max_concurrent_activities=_ACTIVITY_WORKERS,
        )
        print(f"Worker polling task queue '{tq}' on {config.TEMPORAL_ADDRESS}")
        await worker.run()
