"""Pure unit tests for the Notion MCP result parsers + NotionSource row->Record
mapping (no network). These also guard the destination, which shares client.py."""
from __future__ import annotations

import asyncio

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


# --- value decode (inverse of the destination encoder) ----------------------

def test_decode_value_sentinels_and_multiselect():
    assert client.decode_value("__YES__") is True
    assert client.decode_value("__NO__") is False
    assert client.decode_value('["Java","Python"]') == ["Java", "Python"]   # multi-select
    assert client.decode_value("plain text") == "plain text"
    assert client.decode_value("[1, 2]") == "[1, 2]"          # not all strings -> left as text
    assert client.decode_value("[not json") == "[not json"    # invalid JSON -> left as text
    assert client.decode_value(None) is None


def test_to_record_decodes_by_default():
    rec = NotionSource("DS1")._to_record(
        {"id": "p1", "Emeritus": "__NO__", "Preferred SDKs": '["Go","Rust"]', "Name": "Ann"})
    assert rec.properties == {"Emeritus": False, "Preferred SDKs": ["Go", "Rust"], "Name": "Ann"}


def test_decode_can_be_disabled():
    rec = NotionSource("DS1", decode=False)._to_record({"id": "p1", "Emeritus": "__NO__"})
    assert rec.properties == {"Emeritus": "__NO__"}            # raw values preserved


# --- database -> data source resolution -------------------------------------

class _FakeMCP:
    def __init__(self, fetch_text="", fail=False):
        self._text = fetch_text
        self._fail = fail
    async def call(self, name, arguments):
        if self._fail:
            raise RuntimeError("notion-fetch unavailable")
        return self._text


def test_resolve_strips_collection_prefix():
    out = asyncio.run(client.resolve_data_source_id(_FakeMCP(), "collection://abc-123"))
    assert out == "abc-123"


def test_resolve_extracts_data_source_from_fetch():
    fetched = 'database ... <data-source url="collection://53df7196-c6ae-4c88-a564-f11f54b785db">'
    out = asyncio.run(client.resolve_data_source_id(_FakeMCP(fetched), "12f8fc56-7738-805c-acc9-c846f14847a8"))
    assert out == "53df7196-c6ae-4c88-a564-f11f54b785db"


def test_resolve_is_graceful_on_failure_or_no_match():
    # fetch unavailable -> return input unchanged (never worse than no resolution)
    assert asyncio.run(client.resolve_data_source_id(_FakeMCP(fail=True), "dbid")) == "dbid"
    # fetch returns something with no collection:// -> input unchanged
    assert asyncio.run(client.resolve_data_source_id(_FakeMCP("no match here"), "dbid")) == "dbid"


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))
