"""Pure unit tests for the Notion MCP result parsers + NotionSource row->Record
mapping (no network). These also guard the destination, which shares client.py."""
from __future__ import annotations

from durable_sync.connectors.notion import client
from durable_sync.connectors.notion import NotionSource


# --- client.py parsers ------------------------------------------------------

def test_rows_from_result_json_list_and_wrapped():
    assert client.rows_from_result('[{"id": "p1", "Name": "X"}]') == [{"id": "p1", "Name": "X"}]
    # wrapped under a results/rows/data key
    assert client.rows_from_result('{"results": [{"id": "p2"}]}') == [{"id": "p2"}]
    assert client.rows_from_result("") == []


def test_rows_from_markdown_fallback():
    md = "| id | Name |\n| --- | --- |\n| p1 | Alpha |\n| p2 | Beta |"
    assert client.rows_from_result(md) == [
        {"id": "p1", "Name": "Alpha"}, {"id": "p2", "Name": "Beta"},
    ]


def test_page_id_from_row_strips_url_query():
    assert client.page_id_from_row({"id": "abc123"}) == "abc123"
    assert client.page_id_from_row({"url": "https://notion.so/Title-abc?v=9"}) == "Title-abc"
    assert client.page_id_from_row({"Name": "no id here"}) is None


def test_row_columns_drops_plumbing_keys():
    assert client.row_columns({"id": "p1", "url": "u", "Name": "X", "Type": "Event"}) == {
        "Name": "X", "Type": "Event",
    }


def test_query_sql_requires_order_for_pagination():
    sql = client.query_sql("DS1", order_by="Name", limit=50, offset=100)
    assert 'collection://DS1' in sql and 'ORDER BY "Name"' in sql and "LIMIT 50 OFFSET 100" in sql
    assert "ORDER BY" not in client.query_sql("DS1")   # omitted when no order column


# --- NotionSource normalizer ------------------------------------------------

def test_to_record_keys_on_page_id_and_drops_plumbing():
    src = NotionSource("DS1")
    rec = src._to_record({"id": "page-1", "Name": "Launch", "Type": "Event"})
    assert rec is not None
    assert rec.primary_key == "page-1"
    assert rec.properties == {"Name": "Launch", "Type": "Event"}   # id stripped


def test_to_record_skips_rows_with_no_page_id():
    # Can't be keyed idempotently -> dropped (never key on a column value).
    assert NotionSource("DS1")._to_record({"Name": "orphan"}) is None


def test_source_spec_shape():
    [spec] = NotionSource("DS1", interval_minutes=15).specs()
    assert spec.key == "ds:DS1" and spec.interval_minutes == 15
    assert spec.params == {"data_source_id": "DS1"}


def test_source_exposes_oauth_aux():
    # Notion-as-source must bring the OAuth token workflow so the worker registers it.
    src = NotionSource("DS1")
    assert src.aux_workflows() and src.aux_activities()


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))
