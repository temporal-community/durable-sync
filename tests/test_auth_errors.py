"""Unit tests for the shared auth-error matcher (durable_sync.core).

Guards the regression that motivated extracting it: a bare ``"401" in msg``
substring false-positives on UUIDs/request-ids and wrongly pauses a workflow.
The Asana destination had drifted back to that form; these lock the fix in for
every destination that delegates to `auth_error_in_chain`.
"""
from __future__ import annotations

from durable_sync.core import auth_error_in_chain
from durable_sync.connectors.asana.destination import AsanaDestination


def test_word_boundary_no_false_positive_on_request_id():
    # '401e' inside an id must NOT read as a 401 (the documented gotcha).
    assert auth_error_in_chain(RuntimeError("-> 500: id 7b2f-401e-bad")) is False
    assert auth_error_in_chain(RuntimeError("request 403abc not found")) is False


def test_real_auth_failures_match():
    assert auth_error_in_chain(RuntimeError("-> 401: Unauthorized")) is True
    assert auth_error_in_chain(RuntimeError("-> 403: Forbidden")) is True   # 403 now caught
    assert auth_error_in_chain(RuntimeError("invalid_grant")) is True
    assert auth_error_in_chain(RuntimeError("invalid_token")) is True


def test_non_auth_errors_dont_match():
    assert auth_error_in_chain(RuntimeError("500 server boom")) is False
    assert auth_error_in_chain(RuntimeError("429 rate limited")) is False


def test_walks_cause_chain_and_groups():
    inner = RuntimeError("-> 401: Unauthorized")
    outer = RuntimeError("sync failed")
    outer.__cause__ = inner
    assert auth_error_in_chain(outer) is True
    group = BaseExceptionGroup("g", [ValueError("nope"), RuntimeError("403 forbidden")])
    assert auth_error_in_chain(group) is True


def test_extra_needles():
    # Asana's own phrasing, supplied by the destination.
    assert auth_error_in_chain(RuntimeError("you are not authorized"),
                               extra_needles=("not authorized",)) is True
    assert auth_error_in_chain(RuntimeError("you are not authorized")) is False  # without it


def test_asana_destination_delegates_correctly():
    # The bug, exercised through the real destination method.
    assert AsanaDestination.is_auth_error(RuntimeError("Asana GET /tasks -> 401: Not Authorized"))
    assert AsanaDestination.is_auth_error(RuntimeError("Asana PUT /tasks/9 -> 403: Forbidden"))
    assert not AsanaDestination.is_auth_error(RuntimeError("Asana GET /x -> 500: gid 7b2-401e-bad"))
    assert not AsanaDestination.is_auth_error(RuntimeError("500 server boom"))


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ✓ {name}")
    print("\nAUTH ERROR TESTS PASS ✅")


# --- #4: status-code classification beats body-scanning ----------------------
from durable_sync.core import DestinationHTTPError, auth_error_in_chain


def test_status_403_classifies_as_auth():
    assert auth_error_in_chain(DestinationHTTPError(403, "Asana POST /x -> 403: nope")) is True
    assert auth_error_in_chain(DestinationHTTPError(401, "unauth")) is True


def test_non_auth_status_with_403_in_body_is_not_auth():
    # A 422/500 whose response body happens to contain "403" must NOT pause the
    # workflow — the regression #4 fixes.
    e = DestinationHTTPError(422, "Contentful PUT entries -> 422: rate is 403 per min")
    assert auth_error_in_chain(e) is False
    e2 = DestinationHTTPError(500, "server error: code 403 mentioned in trace")
    assert auth_error_in_chain(e2) is False


def test_plain_exception_still_uses_word_boundary_regex():
    # Exceptions without a status_code keep the legacy text path.
    assert auth_error_in_chain(RuntimeError("boom 401 boom")) is True
    assert auth_error_in_chain(RuntimeError("id 401e9f not auth-related")) is False
