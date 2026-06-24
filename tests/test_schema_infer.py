"""Pure unit tests for neutral schema inference (no network).

Covers the Layer-1 engine: type mapping, roles, conflicts, overrides, column order,
and determinism. See durable_sync/schema.py.
"""
from __future__ import annotations

import datetime as dt

from durable_sync.core import Record
from durable_sync.schema import Column, Kind, Role, Schema, infer_schema


def _rec(pk: str, **props):
    return Record(primary_key=pk, properties=props)


def _kind(schema: Schema, name: str) -> Kind:
    col = schema.by_name(name)
    assert col is not None, f"{name!r} not in {[c.name for c in schema]}"
    return col.kind


def test_neutral_type_mapping():
    recs = [_rec("1", Name="Alpha", Stars=5, Ratio=1.5, Archived=False,
                 Topics=["a", "b"], Due=dt.date(2026, 1, 2),
                 At=dt.datetime(2026, 1, 2, 3, 4))]
    s = infer_schema(recs, title="Name")
    assert _kind(s, "Name") == Kind.TEXT
    assert _kind(s, "Stars") == Kind.NUMBER
    assert _kind(s, "Ratio") == Kind.NUMBER
    assert _kind(s, "Archived") == Kind.CHECKBOX     # bool, NOT number
    assert _kind(s, "Topics") == Kind.MULTI_SELECT
    assert _kind(s, "Due") == Kind.DATE and s.by_name("Due").has_time is False
    assert _kind(s, "At") == Kind.DATE and s.by_name("At").has_time is True


def test_bool_before_int():
    # bool subclasses int; a True must not be read as a NUMBER.
    s = infer_schema([_rec("1", Name="x", Flag=True)], title="Name")
    assert _kind(s, "Flag") == Kind.CHECKBOX


def test_title_always_emitted_even_if_absent_from_sample():
    s = infer_schema([_rec("1", Stars=1)], title="Name")
    title = s.title
    assert title is not None and title.name == "Name"
    assert title.role is Role.TITLE and title.kind == Kind.TEXT


def test_key_role_and_default_text():
    s = infer_schema([_rec("1", Name="x")], title="Name", key="Repo ID")
    col = s.by_name("Repo ID")
    assert col is not None and col.role is Role.KEY and col.kind == Kind.TEXT


def test_synced_emitted_as_trailing_date():
    s = infer_schema([_rec("1", Name="x", Stars=1)],
                     title="Name", synced="Last synced")
    last = s.columns[-1]
    assert last.name == "Last synced"
    assert last.role is Role.SYNCED and last.kind == Kind.DATE


def test_all_none_column_skipped():
    s = infer_schema([_rec("1", Name="x", Mystery=None)], title="Name")
    assert s.by_name("Mystery") is None


def test_all_none_kept_if_title_key_or_override():
    # The key column must survive even when every sampled value is None.
    s = infer_schema([_rec("1", Name="x", Empty=None)],
                     title="Name", key="Empty")
    assert s.by_name("Empty") is not None and s.by_name("Empty").role is Role.KEY
    # ...or when an override pins it.
    s2 = infer_schema([_rec("1", Name="x", Empty=None)],
                      title="Name", overrides={"Empty": Kind.SELECT})
    assert _kind(s2, "Empty") == Kind.SELECT


def test_mixed_types_fall_back_to_text():
    recs = [_rec("1", Name="x", V=5), _rec("2", Name="y", V="oops")]
    assert _kind(infer_schema(recs, title="Name"), "V") == Kind.TEXT


def test_overrides_force_kind_and_coerce_from_string():
    recs = [_rec("1", Name="x", State="open", Topics=["a"])]
    s = infer_schema(recs, title="Name",
                     overrides={"State": "SELECT", "Topics": Kind.MULTI_SELECT})
    assert _kind(s, "State") == Kind.SELECT          # str would infer TEXT
    assert _kind(s, "Topics") == Kind.MULTI_SELECT


def test_override_by_enum_name_string():
    s = infer_schema([_rec("1", Name="x")], title="Name",
                     overrides={"Name": "TEXT"})
    assert _kind(s, "Name") == Kind.TEXT


def test_column_order_matches_smoke_ddl():
    # smoke_notion.py: Name (title), Repo ID (key), Stars, Last synced.
    recs = [_rec("1", Name="x", Stars=5, **{"Repo ID": "1"})]
    s = infer_schema(recs, title="Name", key="Repo ID", synced="Last synced")
    assert [c.name for c in s] == ["Name", "Repo ID", "Stars", "Last synced"]


def test_deterministic():
    recs = [_rec("1", Name="x", Stars=5, Topics=["a"]),
            _rec("2", Name="y", Stars=9, Topics=["b"])]
    a = infer_schema(recs, title="Name", key="Name", synced="Last synced")
    b = infer_schema(recs, title="Name", key="Name", synced="Last synced")
    assert a == b


def test_unknown_override_kind_raises():
    import pytest
    with pytest.raises(ValueError):
        infer_schema([_rec("1", Name="x")], title="Name",
                     overrides={"Name": "bogus"})
