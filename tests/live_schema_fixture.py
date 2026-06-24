"""Throwaway live fixture for the schema-generation end-to-end test (real Notion).

Minimal on purpose: 2 hardcoded Records + a NotionDestination using the DEFAULT
workflow-owned token provider (queries OAuthTokenWorkflow). So this needs the full
auth stack up — there's no stale-token problem because the workflow owns rotation:

  temporal server start-dev                                  # 1) Temporal up
  PYTHONPATH=. python -m durable_sync.connectors.notion.bootstrap   # 2) authorize (once)
  PYTHONPATH=. python -m durable_sync.connectors.notion.start       # 3) hand token to workflow
  # 4) a worker must be running so the workflow can answer the token query.

Two raw-CLI steps then prove the whole new path:

  # A) create a real Notion DB from the inferred schema (the new feature):
  PYTHONPATH=. python -m durable_sync.bootstrap_schema \
      --source tests.live_schema_fixture:SOURCE \
      --destination tests.live_schema_fixture:DESTINATION \
      --name "DURABLE-SYNC SCHEMA-GEN (throwaway — safe to trash)"
  #   -> prints the inferred columns + "created schema -> destination id: <DS>"

  # B) upsert the rows into that generated DB (proves the schema actually works):
  export NOTION_DATA_SOURCE_ID=<DS from step A>
  PYTHONPATH=. python -m tests.live_schema_fixture        # run twice -> idempotent
"""
from __future__ import annotations

import asyncio
import datetime as dt
import os

from durable_sync.core import Record, SourceSpec
from durable_sync.connectors.notion.destination import NotionDestination

_RECORDS = [
    Record(primary_key="100", properties={
        "Name": "Alpha", "Repo ID": "100", "Stars": 5,
        "Archived": False, "Created": dt.date(2024, 1, 2)}),
    Record(primary_key="200", properties={
        "Name": "Beta", "Repo ID": "200", "Stars": 9,
        "Archived": True, "Created": dt.date(2024, 6, 7)}),
]


class _Source:
    name = "schema-fixture"

    def specs(self) -> list[SourceSpec]:
        return [SourceSpec(key="unit", interval_minutes=60)]

    async def fetch(self, spec, only_items=None) -> list[Record]:
        return _RECORDS


SOURCE = _Source()
# data_source_id empty -> bootstrap_schema's ensure_schema creates the DB; the
# upsert step (below) reads the created id back from NOTION_DATA_SOURCE_ID. No
# token_provider -> the default workflow-owned one (queries OAuthTokenWorkflow).
DESTINATION = NotionDestination(
    os.environ.get("NOTION_DATA_SOURCE_ID", ""),
    title_property="Name", key_property="Repo ID", synced_property="Last synced",
    date_properties={"Created"},
)


async def _upsert() -> None:
    now = dt.datetime.now(dt.timezone.utc)
    created = updated = 0
    async with DESTINATION.connect() as s:
        existing = await s.query_existing_ids()
        for r in _RECORDS:
            if r.primary_key in existing:
                await s.update(existing[r.primary_key], r, now); updated += 1
            else:
                await s.create(r, now); created += 1
    print({"created": created, "updated": updated, "existing_before": len(existing)})


if __name__ == "__main__":
    if not os.environ.get("NOTION_DATA_SOURCE_ID"):
        raise SystemExit("set NOTION_DATA_SOURCE_ID to the id bootstrap_schema printed (step 1)")
    asyncio.run(_upsert())
