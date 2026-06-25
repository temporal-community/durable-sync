"""Pure unit tests for the Jira Record->issue encoding (no network)."""
from __future__ import annotations

import datetime as dt

from durable_sync.core import Record
from durable_sync.connectors.jira.destination import JiraDestination, _encode_issue

NOW = dt.datetime(2026, 6, 19, 12, 0, tzinfo=dt.timezone.utc)


def _dest() -> JiraDestination:
    return JiraDestination(
        project_key="ENG",
        issue_type="Task",
        title_property="Summary",
        field_map={
            "Labels": "labels",                          # native array field
            "Priority": "priority",                      # native reference field
            "Due": "duedate",                            # native date field
            "Story points": {"custom_field": "customfield_10016"},
            "Seed": {"custom_field": "customfield_10099"},
        },
        create_only_properties={"Seed"},
    )


def test_create_encoding():
    rec = Record(
        primary_key="10042",
        properties={
            "Summary": "Fix the thing", "Labels": ["back end", "urgent"],
            "Priority": "High", "Due": "2026-07-01", "Story points": 5,
            "Seed": "orig", "Unmapped": "dropme", "Nada": None,
        },
        body="line one\n\nline two",
    )
    fields = _encode_issue(_dest(), rec, creating=True)
    assert fields["summary"] == "Fix the thing"
    assert fields["labels"] == ["back_end", "urgent"]        # spaces sanitized
    assert fields["priority"] == {"name": "High"}            # reference object
    assert fields["duedate"] == "2026-07-01"
    assert fields["customfield_10016"] == 5
    assert fields["customfield_10099"] == "orig"             # seed written on create
    assert "Unmapped" not in repr(fields)                    # unmapped prop dropped
    assert fields["project"] == {"key": "ENG"}
    assert fields["issuetype"] == {"name": "Task"}
    # body -> ADF doc, one paragraph per non-empty line
    doc = fields["description"]
    assert doc["type"] == "doc" and len(doc["content"]) == 2


def test_update_skips_project_issuetype_and_create_only():
    rec = Record(
        primary_key="10042",
        properties={"Summary": "Fix it better", "Story points": 9, "Seed": "new"},
        body="b",
    )
    fields = _encode_issue(_dest(), rec, creating=False)
    assert "project" not in fields and "issuetype" not in fields   # never re-set on update
    assert fields["summary"] == "Fix it better"
    assert fields["customfield_10016"] == 9                        # objective field refreshes
    assert "customfield_10099" not in fields                       # create-only seed NOT overwritten


def test_is_auth_error():
    assert JiraDestination.is_auth_error(
        RuntimeError("Jira POST /rest/api/3/issue -> 401: Unauthorized"))
    assert JiraDestination.is_auth_error(
        RuntimeError("Jira PUT /rest/api/3/issue/10042 -> 403: Forbidden"))
    assert not JiraDestination.is_auth_error(RuntimeError("500 server boom"))


def test_no_aux_hooks():
    # A REST/token destination needs no auth workflow — proving aux_* is optional.
    d = _dest()
    assert not hasattr(d, "aux_workflows")
    assert not hasattr(d, "aux_activities")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ✓ {name}")
    print("\nJIRA ENCODE TESTS PASS ✅")
