"""Pure unit tests for neutral Schema -> Notion CREATE TABLE DDL (no network)."""
from __future__ import annotations

import datetime as dt

import pytest

from durable_sync.core import Record
from durable_sync.schema import Column, Kind, Role, Schema, infer_schema
from durable_sync.connectors.notion.ddl import schema_to_ddl


def test_ddl_matches_smoke_shape():
    # The hand-written DDL in tests/smoke_notion.py, reproduced from inference.
    recs = [Record(primary_key="1",
                   properties={"Name": "Alpha", "Repo ID": "1", "Stars": 5})]
    s = infer_schema(recs, title="Name", key="Repo ID", synced="Last synced")
    assert schema_to_ddl(s) == (
        'CREATE TABLE ("Name" TITLE, "Repo ID" RICH_TEXT, '
        '"Stars" NUMBER, "Last synced" DATE)'
    )


def test_all_kinds_map():
    cols = (
        Column("Title", Kind.TEXT, Role.TITLE),
        Column("Txt", Kind.TEXT),
        Column("Num", Kind.NUMBER),
        Column("Done", Kind.CHECKBOX),
        Column("Tags", Kind.MULTI_SELECT),
        Column("State", Kind.SELECT),
        Column("When", Kind.DATE),
    )
    ddl = schema_to_ddl(Schema(columns=cols))
    assert ddl == (
        'CREATE TABLE ("Title" TITLE, "Txt" RICH_TEXT, "Num" NUMBER, '
        '"Done" CHECKBOX, "Tags" MULTI_SELECT, "State" SELECT, "When" DATE)'
    )


def test_title_role_overrides_kind():
    # A title column reports TITLE even though its neutral kind is TEXT.
    s = Schema(columns=(Column("Name", Kind.TEXT, Role.TITLE),))
    assert schema_to_ddl(s) == 'CREATE TABLE ("Name" TITLE)'


def test_quotes_in_name_are_doubled():
    s = Schema(columns=(Column('Wei"rd', Kind.TEXT, Role.TITLE),))
    assert schema_to_ddl(s) == 'CREATE TABLE ("Wei""rd" TITLE)'


def test_missing_title_raises():
    s = Schema(columns=(Column("Txt", Kind.TEXT),))
    with pytest.raises(ValueError):
        schema_to_ddl(s)
