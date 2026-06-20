"""End-to-end smoke test of the generic spine against a real Temporal server.

Boots a worker (SourceSyncWorkflow + make_activities) with an in-memory source +
destination, drives a workflow via the sync_now signal, and asserts:
  * the workflow passes sandbox validation and runs,
  * fetch_source -> sync_records creates rows,
  * a second run is idempotent (updates, no duplicates),
  * create-only properties survive the update.

Run (needs a Temporal dev server on localhost:7233):
    PYTHONPATH=. python tests/smoke_spine.py
"""
from __future__ import annotations

import asyncio

from temporalio.common import WorkflowIDConflictPolicy

from durable_sync import config
from durable_sync.core import Record, SourceSpec
from durable_sync.temporal_client import connect
from durable_sync.worker import make_worker
from durable_sync.workflows.sync import SourceState, SourceSyncWorkflow
from tests.memory_destination import MemoryDestination


class ListSource:
    name = "list"

    def __init__(self, records: list[Record]):
        self._records = records

    def specs(self) -> list[SourceSpec]:
        return [SourceSpec(key="unit", interval_minutes=60)]

    async def fetch(self, spec, only_items=None) -> list[Record]:
        if only_items:
            return [r for r in self._records if r.primary_key in only_items]
        return self._records


async def _wait_runs(handle, n: int, timeout_s: float = 30.0):
    for _ in range(int(timeout_s * 2)):
        st = await handle.query(SourceSyncWorkflow.status)
        if st.runs_completed >= n:
            return st
        await asyncio.sleep(0.5)
    raise AssertionError(f"workflow did not reach {n} runs in {timeout_s}s")


def _transform(rec: Record):
    """Generic transform seam: drop record '3' (filter) and derive a field on the
    rest. Exercises both transform behaviours through the real workflow."""
    if rec.primary_key == "3":
        return None
    rec.properties["Origin"] = "durable-sync"
    return rec


async def main() -> None:
    records = [
        Record(primary_key="1", properties={"Name": "Alpha", "Stars": 5, "Seed": "orig"}),
        Record(primary_key="2", properties={"Name": "Beta", "Stars": 9, "Seed": "orig"}),
        Record(primary_key="3", properties={"Name": "Gamma (should be filtered)"}),
    ]
    source = ListSource(records)
    dest = MemoryDestination(create_only_properties={"Seed"})

    client = await connect()
    worker = make_worker(client, source, dest, transform=_transform)

    async with worker:  # runs the worker for the duration of this block
        handle = await client.start_workflow(
            SourceSyncWorkflow.run,
            SourceState(spec=SourceSpec(key="unit", interval_minutes=60)),
            id="smoke:unit",
            task_queue=config.TASK_QUEUE,
            id_conflict_policy=WorkflowIDConflictPolicy.TERMINATE_EXISTING,
        )

        # --- first sync ---
        await handle.signal(SourceSyncWorkflow.sync_now, [])
        st = await _wait_runs(handle, 1)
        assert len(dest.store) == 2, f"expected 2 rows (record 3 filtered), got {dest.store}"
        assert "3" not in dest.store, "transform filter did not drop record 3"
        assert st.last_stats == {"total": 2, "created": 2, "updated": 0, "skipped": 0}, st.last_stats
        assert dest.store["1"]["properties"].get("Origin") == "durable-sync", "transform did not derive Origin"
        print("run 1 stats:", st.last_stats, "| rows:", sorted(dest.store), "| transform: filtered '3', derived Origin")

        # mutate a create-only seed locally; it must NOT be overwritten on update
        records[0].properties["Seed"] = "CHANGED"
        records[0].properties["Stars"] = 42  # objective field -> SHOULD refresh

        # --- second sync (idempotent) ---
        await handle.signal(SourceSyncWorkflow.sync_now, [])
        st = await _wait_runs(handle, 2)
        assert len(dest.store) == 2, f"duplicates! {dest.store}"
        assert st.last_stats == {"total": 2, "created": 0, "updated": 2, "skipped": 0}, st.last_stats
        row1 = dest.store["1"]["properties"]
        assert row1["Seed"] == "orig", f"create-only seed was overwritten: {row1}"
        assert row1["Stars"] == 42, f"objective field not refreshed: {row1}"
        assert dest.store["1"]["writes"] == 2, dest.store["1"]
        print("run 2 stats:", st.last_stats, "| row 1:", row1, "| writes:", dest.store["1"]["writes"])

        await handle.terminate()

    print("\nSMOKE PASS ✅ — spine runs end-to-end: idempotent upsert + create-only honored.")


if __name__ == "__main__":
    asyncio.run(main())
