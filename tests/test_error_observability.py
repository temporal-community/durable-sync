"""Error observability: the workflow records the ROOT cause in `last_error`
(not Temporal's generic 'Activity task failed'), and the sync activity unwraps
solo ExceptionGroups so the leaf survives into history.

Motivated live: every failure in the Spotify/ListenBrainz bring-up required
spelunking `temporal workflow show` JSON because `last_error` was generic."""
from __future__ import annotations

from temporalio.exceptions import ApplicationError

from durable_sync.activities import _unwrap_solo_group
from durable_sync.workflows.sync import _describe_error


def test_describe_walks_chain_and_drops_generic_wrapper():
    leaf = RuntimeError("Spotify PUT /me/tracks -> 403: Forbidden")
    mid = ApplicationError("Destination authorization is no longer valid", type="AuthError")
    top = Exception("Activity task failed")
    top.__cause__ = mid
    mid.__cause__ = leaf

    out = _describe_error(top)
    assert out.startswith("Destination authorization is no longer valid")  # generic top dropped
    assert "403: Forbidden" in out                                          # leaf surfaced


def test_describe_includes_exceptiongroup_leaf():
    leaf = RuntimeError("MusicBrainz GET /isrc/x -> 400: Invalid isrc.")
    grp = ExceptionGroup("unhandled errors in a TaskGroup (1 sub-exception)", [leaf])
    assert "400: Invalid isrc." in _describe_error(grp)


def test_describe_plain_and_none():
    assert _describe_error(ValueError("boom")) == "boom"
    assert _describe_error(None) == "unknown error"


def test_describe_dedups_repeated_messages():
    # A wrapper re-raising the same text shouldn't print it twice.
    leaf = RuntimeError("same")
    top = RuntimeError("same")
    top.__cause__ = leaf
    assert _describe_error(top) == "same"


def test_unwrap_solo_group_collapses_to_leaf():
    leaf = RuntimeError("real error")
    assert _unwrap_solo_group(ExceptionGroup("g", [leaf])) is leaf
    # nested solo groups collapse fully
    assert _unwrap_solo_group(ExceptionGroup("o", [ExceptionGroup("i", [leaf])])) is leaf


def test_unwrap_leaves_multi_exception_group_and_plain_alone():
    multi = ExceptionGroup("g", [RuntimeError("a"), RuntimeError("b")])
    assert _unwrap_solo_group(multi) is multi   # >1 sub-exception: ambiguous, keep
    plain = ValueError("x")
    assert _unwrap_solo_group(plain) is plain
