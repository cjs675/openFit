"""Resumable, incremental sync engine.

Design: each stream is an independent :class:`Stream` with its own cursor in
``sync_state``. A sync run walks the streams, and a stream that fails marks
itself ``failed`` without aborting the run. Backfill walks day-by-day and
commits its cursor after each day, so an interrupted multi-year backfill
resumes from where it stopped instead of restarting.

All writes are UPSERTs against natural keys, so re-running any window is safe.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Callable, Iterable

from . import parsers as P
from .client import AuthError, RateLimited, ZeppClient, ZeppError

log = logging.getLogger(__name__)

DEFAULT_BACKFILL_DAYS = 30


@dataclass
class StreamResult:
    stream: str
    rows: int = 0
    status: str = "ok"
    error: str | None = None
    requests: int = 0


@dataclass
class SyncReport:
    results: list[StreamResult] = field(default_factory=list)

    @property
    def total_rows(self) -> int:
        return sum(r.rows for r in self.results)

    @property
    def ok(self) -> bool:
        return all(r.status == "ok" for r in self.results)

    def failed(self) -> list[StreamResult]:
        return [r for r in self.results if r.status != "ok"]


class Stream:
    """A single logical data stream backed by one or more API calls."""

    def __init__(
        self,
        name: str,
        fetch: Callable[[ZeppClient, date, date], Iterable[tuple]],
        *,
        describe: str = "",
    ) -> None:
        self.name = name
        self.fetch = fetch
        self.describe = describe


class SyncEngine:
    def __init__(
        self,
        conn: sqlite3.Connection,
        client: ZeppClient,
        *,
        min_interval: float = 0.25,
    ) -> None:
        self.conn = conn
        self.client = client
        self.min_interval = min_interval
        self._last_request_at = 0.0

    # -- pacing --------------------------------------------------------

    def _throttle(self) -> None:
        """Keep a floor between requests; Zepp rate-limits aggressively."""
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_request_at = time.monotonic()

    # -- cursor state --------------------------------------------------

    def get_cursor(self, stream: str) -> str | None:
        row = self.conn.execute(
            "SELECT cursor FROM sync_state WHERE stream = ?", (stream,)
        ).fetchone()
        return row["cursor"] if row else None

    def _set_cursor(self, stream: str, cursor: str | None) -> None:
        self.conn.execute(
            "INSERT INTO sync_state (stream, cursor) VALUES (?, ?) "
            "ON CONFLICT(stream) DO UPDATE SET cursor = excluded.cursor",
            (stream, cursor),
        )

    def _record_result(self, result: StreamResult) -> None:
        self.conn.execute(
            """
            INSERT INTO sync_state (stream, last_run_at, last_success_at, status, error, rows_written)
            VALUES (?, datetime('now'), CASE WHEN ? = 'ok' THEN datetime('now') ELSE NULL END, ?, ?, ?)
            ON CONFLICT(stream) DO UPDATE SET
                last_run_at = datetime('now'),
                last_success_at = CASE WHEN excluded.status = 'ok'
                                       THEN datetime('now') ELSE sync_state.last_success_at END,
                status = excluded.status,
                error = excluded.error,
                rows_written = sync_state.rows_written + excluded.rows_written
            """,
            (result.stream, result.status, result.status, result.error, result.rows),
        )
        self.conn.commit()

    # -- upsert helpers -----------------------------------------------

    def _upsert(self, table: str, rows: list[dict], conflict: str) -> int:
        """Insert rows, updating on ``conflict`` (a column or column list).

        Column names are taken from the destination table rather than from
        the dict keys, so parser-side helper keys (e.g. sleep's ``_stages``)
        can never leak into the generated SQL.
        """
        if not rows:
            return 0

        table_columns = [
            row["name"]
            for row in self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        ]
        if not table_columns:
            raise ValueError(f"unknown table {table!r}")

        columns = [col for col in table_columns if any(col in row for row in rows)]
        if not columns:
            return 0

        conflict_columns = [c.strip() for c in conflict.split(",")]
        placeholders = ", ".join("?" for _ in columns)
        column_list = ", ".join(columns)
        assignments = ", ".join(
            f"{col} = excluded.{col}" for col in columns if col not in conflict_columns
        )
        sql = (
            f"INSERT INTO {table} ({column_list}) VALUES ({placeholders}) "
            f"ON CONFLICT({conflict}) DO UPDATE SET {assignments}"
        )
        payload = [tuple(row.get(col) for col in columns) for row in rows]
        self.conn.executemany(sql, payload)
        return len(payload)

    # -- public API ----------------------------------------------------

    def run_streams(
        self,
        streams: dict[str, Stream],
        *,
        start: date,
        end: date | None = None,
        continue_on_error: bool = True,
    ) -> SyncReport:
        end = end or date.today()
        report = SyncReport()

        for name, stream in streams.items():
            log.info("syncing stream %s (%s)", name, stream.describe or "data")
            try:
                rows = list(stream.fetch(self.client, start, end))
                written = self._write(name, rows)
                self._record_result(StreamResult(stream=name, rows=written))
                report.results.append(StreamResult(stream=name, rows=written))
            except AuthError as exc:
                # Terminal: no point retrying other streams with a dead token.
                log.error("auth failure on %s: %s", name, exc)
                self._record_result(
                    StreamResult(stream=name, status="auth_error", error=str(exc))
                )
                report.results.append(
                    StreamResult(stream=name, status="auth_error", error=str(exc))
                )
                if not continue_on_error:
                    raise
                break
            except (RateLimited, ZeppError, ValueError) as exc:
                log.error("stream %s failed: %s", name, exc)
                result = StreamResult(stream=name, status="error", error=str(exc))
                self._record_result(result)
                report.results.append(result)
                if not continue_on_error:
                    raise

        return report

    def _write(self, stream: str, rows: list[tuple]) -> int:
        """Dispatch parsed rows to the right table for this stream."""
        if not rows:
            return 0

        if stream == "hr_samples_per_second":
            return self._upsert(
                "hr_samples_per_second",
                [
                    {"ts_utc": ts, "bpm": bpm, "quality": quality}
                    for ts, bpm, quality in rows
                ],
                "ts_utc",
            )
        if stream == "hr_samples_aggregate":
            return self._upsert(
                "hr_samples_aggregate",
                [
                    {
                        "ts_utc": ts,
                        "bpm": bpm,
                        "avg_bpm": avg,
                        "max_bpm": high,
                        "min_bpm": low,
                    }
                    for ts, bpm, avg, high, low in rows
                ],
                "ts_utc",
            )
        if stream == "hrv":
            return self._upsert(
                "hrv_samples",
                [
                    {
                        "ts_utc": ts,
                        "rmssd_ms": rmssd,
                        "sdnn_ms": sdnn,
                        "source": source,
                    }
                    for ts, rmssd, sdnn, source in rows
                ],
                "ts_utc, source",
            )
        if stream == "sleep":
            return self._write_sleep(rows)
        if stream == "workouts":
            return self._upsert("workouts", rows, "workout_id")
        if stream == "spo2":
            return self._upsert(
                "spo2_samples",
                [{"ts_utc": ts, "kind": kind, "spo2": value} for ts, kind, value in rows],
                "ts_utc, kind",
            )
        if stream == "spo2_night":
            return self._upsert("spo2_night_windows", rows, "date_local")
        if stream == "stress":
            return self._upsert(
                "stress_samples",
                [{"ts_utc": ts, "score": score, "source": source} for ts, score, source in rows],
                "ts_utc, source",
            )
        if stream == "temperature":
            return self._upsert(
                "temperature_samples",
                [
                    {
                        "ts_utc": ts,
                        "delta_centi_c": delta,
                        "absolute_c": absolute,
                        "baseline_centi_c": baseline,
                        "score": score,
                        "source": source,
                    }
                    for ts, delta, absolute, baseline, score, source in rows
                ],
                "ts_utc, source",
            )
        if stream == "recovery":
            return self._upsert("recovery_daily", rows, "date_local")
        if stream == "daily_activity":
            return self._upsert("daily_activity", rows, "date_local")
        if stream == "weight":
            return self._upsert(
                "weight_records",
                [{"ts_utc": ts, "kg": kg, "source": source} for ts, kg, source in rows],
                "ts_utc, source",
            )
        raise ValueError(f"no writer registered for stream {stream!r}")

    def _write_sleep(self, sessions: list[dict]) -> int:
        stage_rows: list[dict] = []
        for session in sessions:
            stage_rows.extend(
                {
                    "session_id": session["session_id"],
                    "seq": seq,
                    "stage": stage,
                    "start_ts_utc": start,
                    "end_ts_utc": end,
                }
                for seq, stage, start, end in session.get("_stages", [])
            )

        written = self._upsert("sleep_sessions", sessions, "session_id")
        if stage_rows:
            self.conn.executemany(
                "INSERT INTO sleep_stages (session_id, seq, stage, start_ts_utc, end_ts_utc) "
                "VALUES (:session_id, :seq, :stage, :start_ts_utc, :end_ts_utc) "
                "ON CONFLICT(session_id, seq) DO UPDATE SET "
                "stage = excluded.stage, "
                "start_ts_utc = excluded.start_ts_utc, "
                "end_ts_utc = excluded.end_ts_utc",
                stage_rows,
            )
        return written

    # -- backfill ------------------------------------------------------

    def backfill(
        self,
        stream_name: str,
        fetch_one_day: Callable[[ZeppClient, date], list[tuple]],
        write: Callable[[str, list[tuple]], int],
        *,
        start: date,
        end: date | None = None,
        stop_on_error: bool = False,
    ) -> int:
        """Walk day-by-day from ``start``, committing a cursor each day."""
        end = end or date.today()
        cursor = self.get_cursor(stream_name)
        resume_from = (
            date.fromisoformat(cursor) if cursor else start
        )
        if resume_from < start:
            resume_from = start

        total = 0
        day = resume_from
        while day <= end:
            try:
                self._throttle()
                rows = fetch_one_day(self.client, day)
                written = write(stream_name, rows)
                self.conn.commit()
                total += written
                # Advance past the day just completed.
                next_cursor = (day + timedelta(days=1)).isoformat()
                self._set_cursor(stream_name, next_cursor)
                self.conn.commit()
                self._record_result(StreamResult(stream=stream_name, rows=written))
                day += timedelta(days=1)
            except AuthError:
                raise
            except (ZeppError, ValueError) as exc:
                log.error("backfill %s failed on %s: %s", stream_name, day, exc)
                self._record_result(
                    StreamResult(
                        stream=stream_name, status="error", error=f"{day}: {exc}"
                    )
                )
                if stop_on_error:
                    raise
                day += timedelta(days=1)
        return total
