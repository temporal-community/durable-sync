"""SourceSyncWorkflow — the generic per-source entity workflow.

One long-lived workflow per source unit. It is its own durable, interruptible
timer: it sleeps for `interval_minutes` but wakes early on a `sync_now` signal
(e.g. from a webhook). It answers a `status` query between runs and uses
continue-as-new to keep history bounded forever. No Temporal Schedule needed —
the loop IS the periodicity.

Determinism: timestamps come from workflow.now() (never datetime.now()); signal
handlers only flip flags (no I/O); the query handler is read-only. Activities are
invoked BY NAME, so this module never imports their (closure) implementations.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from durable_sync.activities import FETCH_SOURCE, SYNC_RECORDS, FetchPage
    from durable_sync.core import Record, SourceSpec  # noqa: F401  (Record used in annotations)

# Max records handed to one SYNC_RECORDS activity, so a single upsert payload stays
# well under Temporal's 2MB limit even when a source returns a large page. A
# CONSTANT (not env-driven) so the command sequence is deterministic across workers
# and replays; changing it is a workflow-shape change (guard with patching/versioning).
_SYNC_CHUNK_SIZE = 500


def _is_auth_failure(err: BaseException | None) -> bool:
    """Walk the cause chain for a non-retryable AuthError from the sync activity.
    Pure/deterministic — safe in a workflow (inspects exception types only)."""
    while err is not None:
        if isinstance(err, ApplicationError) and err.type == "AuthError":
            return True
        err = err.__cause__
    return False


def _describe_error(err: BaseException | None) -> str:
    """Flatten an exception — its `__cause__` chain plus any `ExceptionGroup`
    leaves — into one readable line for `last_error`, so a `status` query surfaces
    the ROOT cause (e.g. 'Spotify PUT /me/tracks -> 403: Forbidden') instead of the
    generic top-level 'Activity task failed'. Deterministic (inspects messages/types
    only), so it is safe to call inside the workflow."""
    parts: list[str] = []

    def visit(e: BaseException | None, depth: int) -> None:
        if e is None or depth > 20:
            return
        # ApplicationError carries a clean `.message`; fall back to str()/type name.
        msg = (getattr(e, "message", None) or str(e) or type(e).__name__).strip()
        if msg and (not parts or parts[-1] != msg):
            parts.append(msg)
        for sub in getattr(e, "exceptions", ()) or ():   # ExceptionGroup leaves
            visit(sub, depth + 1)
        visit(e.__cause__, depth + 1)

    visit(err, 0)
    # Drop a leading generic Temporal wrapper once we have something more specific.
    while len(parts) > 1 and parts[0].lower().rstrip(".") in (
        "activity task failed", "activity error",
    ):
        parts.pop(0)
    return " ← ".join(parts) if parts else "unknown error"


@dataclass
class SourceState:
    """Everything carried across continue-as-new boundaries."""
    spec: SourceSpec
    paused: bool = False
    runs_completed: int = 0
    # A sync_now that arrived but hasn't run yet must survive continue-as-new,
    # else a targeted webhook refresh (its named items) is silently lost at the
    # history-roll boundary. Carried here and restored in __init__.
    sync_requested: bool = False
    pending_items: list[str] = field(default_factory=list)


@dataclass
class StatusView:
    key: str
    paused: bool
    interval_minutes: int
    runs_completed: int
    last_run: str | None
    last_stats: dict | None
    last_error: str | None
    sync_pending: bool


@workflow.defn
class SourceSyncWorkflow:
    @workflow.init
    def __init__(self, state: SourceState) -> None:
        self._state = state
        # Restore a sync that was requested before the last continue-as-new.
        self._sync_requested = state.sync_requested
        self._pending_items: list[str] = list(state.pending_items)  # targeted items from sync_now
        self._last_run: str | None = None
        self._last_stats: dict | None = None
        self._last_error: str | None = None

    @workflow.run
    async def run(self, state: SourceState) -> None:
        while True:
            # Durable timer that a signal can cut short: wake on interval OR when
            # sync_now flips the flag.
            try:
                await workflow.wait_condition(
                    lambda: self._sync_requested,
                    timeout=timedelta(minutes=self._state.spec.interval_minutes),
                )
            except asyncio.TimeoutError:
                pass  # interval elapsed -> a scheduled run

            if self._state.paused:
                await workflow.wait_condition(lambda: not self._state.paused)
                continue

            # A sync_now may name specific items (targeted webhook); else full sync.
            only_items = self._pending_items or None
            self._pending_items = []
            self._sync_requested = False

            await self._run_once(only_items)
            self._state.runs_completed += 1

            # Eternal workflow -> roll history so it never grows unbounded. Persist
            # any sync_now that landed during/after this run so it isn't dropped at
            # the boundary (a webhook's named items must survive the history roll).
            if workflow.info().is_continue_as_new_suggested():
                await workflow.wait_condition(workflow.all_handlers_finished)
                self._state.sync_requested = self._sync_requested
                self._state.pending_items = self._pending_items
                workflow.continue_as_new(args=[self._state])

    async def _run_once(self, only_items: list[str] | None) -> None:
        try:
            totals = {"total": 0, "created": 0, "updated": 0, "skipped": 0}
            cursor: str | None = None
            # Paged fetch -> chunked upsert, so neither payload through history is
            # unbounded. Each SYNC_RECORDS re-queries existing ids, so a duplicate
            # key split across chunks still resolves to an update, not a 2nd create.
            while True:
                page: FetchPage = await workflow.execute_activity(
                    FETCH_SOURCE,
                    args=[self._state.spec, only_items, cursor],
                    start_to_close_timeout=timedelta(minutes=10),
                    heartbeat_timeout=timedelta(seconds=60),
                    retry_policy=RetryPolicy(maximum_attempts=5),
                    result_type=FetchPage,
                )
                records = page.records
                for i in range(0, len(records), _SYNC_CHUNK_SIZE):
                    chunk = records[i:i + _SYNC_CHUNK_SIZE]
                    stats = await workflow.execute_activity(
                        SYNC_RECORDS,
                        args=[chunk],
                        start_to_close_timeout=timedelta(minutes=15),
                        heartbeat_timeout=timedelta(seconds=60),
                        retry_policy=RetryPolicy(
                            maximum_attempts=5,
                            non_retryable_error_types=["ConfigError", "AuthError"],
                        ),
                    )
                    for k in totals:
                        totals[k] += stats.get(k, 0)
                cursor = page.next_cursor
                if cursor is None:
                    break
            self._last_stats = totals
            self._last_error = None
        except Exception as e:  # noqa: BLE001 - record, don't kill the loop
            self._last_error = _describe_error(e)
            if _is_auth_failure(e):
                # Refresh token revoked/expired -> only a human can fix it. Pause
                # so the timer loop stops hammering a dead credential; a human
                # re-auths, then sends `resume` to catch up.
                self._state.paused = True
                workflow.logger.error(
                    "Auth failure for %s — pausing until re-auth + `resume` signal",
                    self._state.spec.key,
                )
            else:
                workflow.logger.error("Sync failed for %s: %s", self._state.spec.key, e)
        finally:
            self._last_run = workflow.now().isoformat()

    # --- Signals (flip flags only; no I/O, so handlers stay non-async) -------

    @workflow.signal
    def sync_now(self, items: list[str] | None = None) -> None:
        """Trigger an immediate sync. Optionally name specific items (e.g. a
        single repo from a push webhook) for a targeted refresh."""
        if items:
            self._pending_items.extend(items)
        self._sync_requested = True

    @workflow.signal
    def set_interval(self, minutes: int) -> None:
        # Clamp to >=1: a 0/negative interval makes the durable timer fire
        # immediately every loop, busy-spinning full syncs back to back.
        self._state.spec.interval_minutes = max(1, int(minutes))

    # *_ absorbs a stray signal payload (e.g. `--input '[]'`). A signal handler
    # that raises POISONS the workflow task (it re-fails forever), so no-arg
    # signals must tolerate an unexpected arg rather than throw.
    @workflow.signal
    def pause(self, *_: object) -> None:
        self._state.paused = True

    @workflow.signal
    def resume(self, *_: object) -> None:
        self._state.paused = False
        self._sync_requested = True  # catch up immediately on resume

    # --- Query (read-only) ---------------------------------------------------

    @workflow.query
    def status(self) -> StatusView:
        return StatusView(
            key=self._state.spec.key,
            paused=self._state.paused,
            interval_minutes=self._state.spec.interval_minutes,
            runs_completed=self._state.runs_completed,
            last_run=self._last_run,
            last_stats=self._last_stats,
            last_error=self._last_error,
            sync_pending=self._sync_requested,
        )
