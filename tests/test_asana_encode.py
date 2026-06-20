"""Pure unit tests for the Asana Record->task encoding (no network)."""
from __future__ import annotations

import datetime as dt

from durable_sync.core import Record
from durable_sync.destinations.asana.destination import AsanaDestination, _encode_task

NOW = dt.datetime(2026, 6, 19, 12, 0, tzinfo=dt.timezone.utc)


def _dest() -> AsanaDestination:
    return AsanaDestination(
        project_gid="P123",
        title_property="Name",
        field_map={
            "Stars": {"custom_field": "CF_STARS"},
            "Last updated": "due_on",     # native date field
            "Done": "completed",          # native bool field
            "Languages": {"custom_field": "CF_LANG"},
            "Seed": {"custom_field": "CF_SEED"},
        },
        create_only_properties={"Seed"},
    )


def test_create_encoding():
    rec = Record(
        primary_key="r1",
        properties={
            "Name": "Alpha", "Stars": 5, "Last updated": "2026-06-11",
            "Done": True, "Languages": ["Python", "Go"], "Seed": "orig",
            "Unmapped": "dropme", "Nada": None,
        },
        body="hello body",
    )
    data = _encode_task(_dest(), rec, NOW, creating=True)
    assert data["name"] == "Alpha"
    assert data["notes"] == "hello body"
    assert data["due_on"] == "2026-06-11"        # native date
    assert data["completed"] is True             # native bool
    assert data["custom_fields"]["CF_STARS"] == 5
    assert data["custom_fields"]["CF_LANG"] == "Python, Go"   # list joined
    assert data["custom_fields"]["CF_SEED"] == "orig"         # seed written on create
    assert "Unmapped" not in repr(data)          # unmapped prop dropped
    assert data["projects"] == ["P123"]
    assert data["external"] == {"gid": "r1"}     # idempotency handle


def test_update_skips_external_projects_and_create_only():
    rec = Record(primary_key="r1", properties={"Name": "Alpha2", "Stars": 9, "Seed": "new"}, body="b")
    data = _encode_task(_dest(), rec, NOW, creating=False)
    assert "external" not in data and "projects" not in data   # never re-set on update
    assert data["name"] == "Alpha2"
    assert data["custom_fields"]["CF_STARS"] == 9              # objective field refreshes
    assert "CF_SEED" not in data.get("custom_fields", {})      # create-only seed NOT overwritten


def test_synced_custom_field_stamp():
    d = _dest()
    d.synced_custom_field_gid = "CF_SYNCED"
    data = _encode_task(d, Record(primary_key="r1", properties={"Name": "A"}), NOW, creating=True)
    assert data["custom_fields"]["CF_SYNCED"] == "2026-06-19"


def test_is_auth_error():
    assert AsanaDestination.is_auth_error(RuntimeError("Asana GET /tasks -> 401: Not Authorized"))
    assert not AsanaDestination.is_auth_error(RuntimeError("500 server boom"))


def test_no_aux_hooks():
    # A REST/PAT destination needs no auth workflow — proving aux_* is optional.
    d = _dest()
    assert not hasattr(d, "aux_workflows")
    assert not hasattr(d, "aux_activities")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ✓ {name}")
    print("\nASANA ENCODE TESTS PASS ✅")
