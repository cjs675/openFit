"""SQLite schema for local Zepp/Helio Strap data.

Design notes
------------
Time-series first: raw samples live in their own tables, and anything
derived is a VIEW rather than a materialised copy. Nothing is aggregated at
write time, so a future algorithm change never requires a re-import.

Idempotency: every table has a natural primary key derived from upstream
identifiers (or time buckets), and all writes are UPSERTs. Re-running a sync
over the same window is always safe.

Per-second HR is deliberately isolated in `hr_samples_per_second`. At
86400 rows/day it is two orders of magnitude larger than everything else
combined, and it needs its own retention policy without dragging the rest of
the database down with it.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_VERSION = 1

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- One row per logical stream we know how to sync. Lets backfill resume and
-- lets a failing stream be retried without touching the healthy ones.
CREATE TABLE IF NOT EXISTS sync_state (
    stream        TEXT PRIMARY KEY,
    cursor        TEXT,
    last_run_at   TEXT,
    last_success_at TEXT,
    status        TEXT,
    error         TEXT,
    rows_written  INTEGER NOT NULL DEFAULT 0
);

-- Per-second HR is stored bucketed to the second in UTC. `quality` carries
-- the upstream confidence flag when present; NULL means "not reported".
CREATE TABLE IF NOT EXISTS hr_samples_per_second (
    ts_utc  TEXT NOT NULL,
    bpm     INTEGER NOT NULL,
    quality INTEGER,
    PRIMARY KEY (ts_utc)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_hr_ps_date
    ON hr_samples_per_second (substr(ts_utc, 1, 10));

-- Coarser HR resolution (typically 1/minute) used by Zepp's summary endpoints.
-- Kept separate so a rerun of the fine-grained stream never clobbers it.
CREATE TABLE IF NOT EXISTS hr_samples_aggregate (
    ts_utc  TEXT NOT NULL,
    bpm     INTEGER NOT NULL,
    avg_bpm INTEGER,
    max_bpm INTEGER,
    min_bpm INTEGER,
    PRIMARY KEY (ts_utc)
) WITHOUT ROWID;

-- HRV. Zepp reports RMSSD (ms) as the headline value; SDNN is stored when
-- present because it changes the overnight-autonomic picture.
CREATE TABLE IF NOT EXISTS hrv_samples (
    ts_utc   TEXT NOT NULL,
    rmssd_ms REAL NOT NULL,
    sdnn_ms  REAL,
    source   TEXT,
    PRIMARY KEY (ts_utc, source)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS sleep_sessions (
    session_id        TEXT PRIMARY KEY,
    date_local        TEXT NOT NULL,
    start_ts_utc      TEXT NOT NULL,
    end_ts_utc        TEXT NOT NULL,
    duration_min      INTEGER,
    deep_min          INTEGER,
    light_min         INTEGER,
    rem_min           INTEGER,
    awake_min         INTEGER,
    score             INTEGER,
    breath_rate       REAL,
    snore_events      INTEGER,
    sleep_score_detail TEXT
);

CREATE INDEX IF NOT EXISTS idx_sleep_date
    ON sleep_sessions (date_local);

-- Stage-level detail for the hypnogram. `seq` preserves upstream ordering so
-- a malformed upstream timeline can still be rendered as-is.
CREATE TABLE IF NOT EXISTS sleep_stages (
    session_id TEXT NOT NULL REFERENCES sleep_sessions(session_id) ON DELETE CASCADE,
    seq        INTEGER NOT NULL,
    stage      TEXT NOT NULL,
    start_ts_utc TEXT NOT NULL,
    end_ts_utc   TEXT NOT NULL,
    PRIMARY KEY (session_id, seq)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS workouts (
    workout_id    TEXT PRIMARY KEY,
    sport_type    TEXT,
    sport_name    TEXT,
    start_ts_utc  TEXT NOT NULL,
    end_ts_utc    TEXT NOT NULL,
    duration_min  REAL,
    distance_m    REAL,
    avg_hr        INTEGER,
    max_hr        INTEGER,
    avg_hr_pct    INTEGER,
    max_hr_pct    INTEGER,
    calories      REAL,
    training_load REAL,
    training_effect TEXT,
    source        TEXT
);

CREATE INDEX IF NOT EXISTS idx_workout_start
    ON workouts (start_ts_utc);

-- High-resolution HR captured inside a workout, independent of the 1Hz feed.
CREATE TABLE IF NOT EXISTS workout_hr_samples (
    workout_id TEXT NOT NULL REFERENCES workouts(workout_id) ON DELETE CASCADE,
    ts_utc     TEXT NOT NULL,
    bpm        INTEGER NOT NULL,
    PRIMARY KEY (workout_id, ts_utc)
) WITHOUT ROWID;

-- SpO2. Zepp distinguishes spot measurements (daytime taps, ~1 reading)
-- from continuous sleep monitoring (per-minute). `kind` keeps them apart.
CREATE TABLE IF NOT EXISTS spo2_samples (
    ts_utc TEXT NOT NULL,
    kind   TEXT NOT NULL,
    spo2   INTEGER NOT NULL,
    PRIMARY KEY (ts_utc, kind)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_spo2_date
    ON spo2_samples (substr(ts_utc, 1, 10));

-- Oxygen Desaturation Index and Obstructive Sleep Apnoea risk windows.
CREATE TABLE IF NOT EXISTS spo2_night_windows (
    date_local TEXT PRIMARY KEY,
    odi        REAL,
    osa_level  TEXT,
    odi_events INTEGER
);

CREATE TABLE IF NOT EXISTS stress_samples (
    ts_utc TEXT NOT NULL,
    score  INTEGER NOT NULL,
    source TEXT,
    PRIMARY KEY (ts_utc, source)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_stress_date
    ON stress_samples (substr(ts_utc, 1, 10));

-- Zepp reports skin temperature as a DELTA from a personal baseline in
-- hundredths of a degree C (26 == +0.26 C above baseline), not an absolute
-- value. Storing it this way keeps that semantic explicit and reversible.
CREATE TABLE IF NOT EXISTS temperature_samples (
    ts_utc          TEXT NOT NULL,
    delta_centi_c   INTEGER,
    absolute_c      REAL,
    baseline_centi_c INTEGER,
    score           INTEGER,
    source          TEXT,
    PRIMARY KEY (ts_utc, source)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_temp_date
    ON temperature_samples (substr(ts_utc, 1, 10));

-- BioCharge / Exertion / training load / PAI: one row per local day.
CREATE TABLE IF NOT EXISTS recovery_daily (
    date_local       TEXT PRIMARY KEY,
    biocharge        INTEGER,
    biocharge_status TEXT,
    exertion         INTEGER,
    exertion_status  TEXT,
    training_load    INTEGER,
    training_load_ratio REAL,
    vo2_max          REAL,
    pai              INTEGER,
    recovery_time_min INTEGER,
    updated_at       TEXT
);

CREATE TABLE IF NOT EXISTS daily_activity (
    date_local      TEXT PRIMARY KEY,
    steps           INTEGER,
    distance_m      REAL,
    calories_active INTEGER,
    calories_total  INTEGER,
    standing_hours  REAL,
    exercise_minutes INTEGER,
    sit_minutes     INTEGER,
    updated_at      TEXT
);

CREATE TABLE IF NOT EXISTS weight_records (
    ts_utc    TEXT NOT NULL,
    kg        REAL,
    source    TEXT,
    PRIMARY KEY (ts_utc, source)
) WITHOUT ROWID;

-- Raw payloads, retained verbatim. When Zepp changes a response shape we
-- can re-derive old data without another round-trip.
CREATE TABLE IF NOT EXISTS raw_payloads (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint   TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    status     INTEGER,
    body       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_endpoint
    ON raw_payloads (endpoint, fetched_at);

-- Per-second HR arrives as COS file manifests pointing at zip archives
-- rather than inline samples, so download progress is tracked separately.
CREATE TABLE IF NOT EXISTS hr_blob_manifest (
    blob_id    TEXT PRIMARY KEY,
    url        TEXT,
    date_local TEXT,
    state      TEXT NOT NULL,
    bytes      INTEGER,
    rows       INTEGER,
    error      TEXT,
    updated_at TEXT
);

-- Views -------------------------------------------------------------

CREATE VIEW IF NOT EXISTS v_daily_heart_rate AS
SELECT substr(ts_utc, 1, 10) AS date_local,
       COUNT(*)    AS sample_count,
       AVG(bpm)    AS avg_bpm,
       MIN(bpm)    AS min_bpm,
       MAX(bpm)    AS max_bpm
FROM hr_samples_per_second
GROUP BY date_local;

CREATE VIEW IF NOT EXISTS v_sleep_summary AS
SELECT date_local,
       duration_min,
       deep_min,
       light_min,
       rem_min,
       awake_min,
       score,
       ROUND(100.0 * COALESCE(deep_min, 0)
             / NULLIF(deep_min + light_min + rem_min, 0), 1) AS deep_pct
FROM sleep_sessions;

CREATE VIEW IF NOT EXISTS v_daily_overview AS
SELECT d.date_local,
       d.steps,
       d.distance_m,
       d.calories_active,
       r.biocharge,
       r.exertion,
       r.training_load,
       r.pai,
       s.duration_min AS sleep_min,
       s.score        AS sleep_score,
       h.avg_hr       AS resting_avg_hr,
       v.odi          AS spo2_odi,
       v.osa_level    AS osa_level,
       (SELECT AVG(score) FROM stress_samples st
         WHERE substr(st.ts_utc, 1, 10) = d.date_local) AS avg_stress
FROM daily_activity d
LEFT JOIN recovery_daily r USING (date_local)
LEFT JOIN (SELECT date_local,
                  MAX(duration_min) AS duration_min,
                  MAX(score)        AS score
           FROM sleep_sessions GROUP BY date_local) s USING (date_local)
LEFT JOIN (SELECT date_local, AVG(avg_bpm) AS avg_hr
           FROM v_daily_heart_rate GROUP BY date_local) h USING (date_local)
LEFT JOIN spo2_night_windows v USING (date_local)
ORDER BY d.date_local DESC;
"""


def connect(path: str | Path, *, create_schema: bool = True) -> sqlite3.Connection:
    """Open a SQLite connection with the schema applied."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    if create_schema:
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (str(SCHEMA_VERSION),),
        )
        conn.commit()
    return conn
