"""Pure unit tests for the Jira issue->Record normalization + ADF flatten (no network)."""
from __future__ import annotations

import datetime as dt

from durable_sync.connectors.jira import JiraSource
from durable_sync.connectors.jira import api

BASE = "https://acme.atlassian.net"


def _issue(**fields):
    base = {
        "summary": "Fix the thing",
        "status": {"name": "In Progress"},
        "issuetype": {"name": "Bug"},
        "assignee": {"displayName": "Angie Byron"},
        "reporter": {"displayName": "Bob"},
        "priority": {"name": "High"},
        "labels": ["backend", "urgent"],
        "created": "2026-06-19T12:00:00.000-0700",
        "updated": "2026-06-20T09:30:00.000-0700",
        "description": {
            "type": "doc", "version": 1,
            "content": [{"type": "paragraph",
                         "content": [{"type": "text", "text": "Steps to reproduce."}]}],
        },
    }
    base.update(fields)
    return {"id": "10042", "key": "ENG-7", "fields": base}


def test_basic_mapping():
    rec = JiraSource()._to_record(_issue(), BASE)
    assert rec.primary_key == "10042"          # the immutable id, NOT the key
    p = rec.properties
    assert p["Summary"] == "Fix the thing"
    assert p["Type"] == "Issue" and p["Source"] == "Jira"
    assert p["Issue Key"] == "ENG-7"
    assert p["Issue Type"] == "Bug" and p["Status"] == "In Progress" and p["Priority"] == "High"
    assert p["Assignee"] == "Angie Byron" and p["Reporter"] == "Bob"
    assert p["Labels"] == ["backend", "urgent"]
    assert p["URL"] == "https://acme.atlassian.net/browse/ENG-7"
    assert isinstance(p["Created"], dt.datetime) and p["Created"].year == 2026
    assert rec.body == "Steps to reproduce."


def test_missing_fields_omitted_and_defaults():
    rec = JiraSource()._to_record(
        {"id": "11", "key": "", "fields": {}}, BASE)
    p = rec.properties
    assert p["Summary"] == "(no summary)"
    assert p["Labels"] == []
    assert p["Assignee"] == ""                  # neutral empty, not None
    assert "URL" not in p                        # no key -> URL omitted (None dropped)
    assert "Created" not in p                    # absent datetime dropped
    assert rec.body is None


def test_adf_to_text_paragraphs_and_lists():
    adf = {
        "type": "doc", "version": 1,
        "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": "Intro."}]},
            {"type": "bulletList", "content": [
                {"type": "listItem", "content": [
                    {"type": "paragraph", "content": [{"type": "text", "text": "one"}]}]},
                {"type": "listItem", "content": [
                    {"type": "paragraph", "content": [{"type": "text", "text": "two"}]}]},
            ]},
        ],
    }
    text = api.adf_to_text(adf)
    assert "Intro." in text and "one" in text and "two" in text


def test_adf_to_text_string_and_none_passthrough():
    assert api.adf_to_text("plain old text") == "plain old text"
    assert api.adf_to_text(None) is None


def test_parse_dt_fallback():
    assert api.parse_dt("not a date") == "not a date"
    assert api.parse_dt(None) is None


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ✓ {name}")
    print("\nJIRA NORMALIZE TESTS PASS ✅")
