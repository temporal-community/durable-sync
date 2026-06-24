"""Offline tests for the generic bootstrap_schema CLI (Layer 3).

Drives the CLI against an in-repo wired fixture (MemoryDestination), so the whole
sample -> infer -> ensure_schema path runs with no network.
"""
from __future__ import annotations

import asyncio

import pytest

from durable_sync import bootstrap_schema as bs
from durable_sync.schema import Kind, Role

_FIX = "tests._schema_cli_fixture"


def test_resolve_and_helpers():
    assert bs._resolve(f"{_FIX}:SOURCE").name == "fixture"
    with pytest.raises(SystemExit):
        bs._resolve("tests._schema_cli_fixture")          # missing ':OBJECT'
    assert bs._overrides(["State=SELECT", "X=number"]) == {"State": "SELECT", "X": "number"}
    with pytest.raises(SystemExit):
        bs._overrides(["bogus"])


def test_cli_end_to_end_offline(capsys):
    bs.main([
        "--source", f"{_FIX}:SOURCE",
        "--destination", f"{_FIX}:DESTINATION",
        "--name", "Repos",
        "--override", "Topics=MULTI_SELECT",
    ])
    # The fixture destination captured the schema the CLI inferred + handed it.
    from tests import _schema_cli_fixture as fix
    schema = fix.DESTINATION.schema
    assert schema is not None and schema.name == "Repos"
    # title/key/synced defaulted off the destination's own properties.
    assert schema.title.name == "Name"
    assert schema.by_name("Repo ID").role is Role.KEY
    assert schema.columns[-1].name == "Last synced" and schema.columns[-1].role is Role.SYNCED
    assert schema.by_name("Topics").kind is Kind.MULTI_SELECT
    assert schema.by_name("Stars").kind is Kind.NUMBER

    out = capsys.readouterr().out
    assert "sampled 2 record(s)" in out
    assert "inferred schema 'Repos'" in out


def test_cli_errors_when_destination_lacks_hook():
    # A destination without ensure_schema is rejected with a clear message.
    with pytest.raises(SystemExit):
        bs.main(["--source", f"{_FIX}:SOURCE",
                 "--destination", "durable_sync.core:Record"])
