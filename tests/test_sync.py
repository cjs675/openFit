"""Tests for the schema, sync engine, and client retry behaviour."""

from __future__ import annotations

import sqlite3

import pytest
import requests

from zepplocal import schema
from zepplocal.client import AuthError, RateLimited, ZeppClient, ZeppError
from zepplocal.config import Config
from zepplocal.sync import SyncEngine


@pytest.fixture
def conn():
    connection = schema.connect(":memory:")
    yield connection
    connection.close()


@pytest.fixture
def config(tmp_path):
    return Config(
        app_token="test-token-value",
        user_id="12345",
        host="api-mifit-us2.zepp.com",
        db_path=tmp_path / "test.db",
    )


class TestSchema:
    def test_creates_all_tables(self, conn):
        names = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        expected = {
            "hr_samples_per_second", "hrv_samples", "sleep_sessions",
            "sleep_stages", "workouts", "spo2_samples", "stress_samples",
            "temperature_samples", "recovery_daily", "daily_activity",
            "sync_state", "raw_payloads",
        }
        assert expected <= names

    def test_records_schema_version(self, conn):
        value = conn.execute(
            "SELECT value FROM meta WHERE key='schema_version'"
        ).fetchone()[0]
        assert value == str(schema.SCHEMA_VERSION)

    def test_hr_view_aggregates(self, conn):
        conn.executemany(
            "INSERT INTO hr_samples_per_second (ts_utc, bpm) VALUES (?, ?)",
            [("2025-06-15T00:00:00Z", 60), ("2025-06-15T00:00:01Z", 80),
             ("2025-06-15T00:00:02Z", 70)],
        )
        row = conn.execute("SELECT * FROM v_daily_heart_rate").fetchone()
        assert row["sample_count"] == 3
        assert row["avg_bpm"] == pytest.approx(70.0)

    def test_sleep_cascade_deletes_stages(self, conn):
        conn.execute(
            "INSERT INTO sleep_sessions (session_id, date_local, start_ts_utc, end_ts_utc) "
            "VALUES ('s1', '2025-06-15', '2025-06-15T01:00:00Z', '2025-06-15T09:00:00Z')"
        )
        conn.execute(
            "INSERT INTO sleep_stages (session_id, seq, stage, start_ts_utc, end_ts_utc) "
            "VALUES ('s1', 0, 'deep', '2025-06-15T01:00:00Z', '2025-06-15T02:00:00Z')"
        )
        conn.execute("DELETE FROM sleep_sessions WHERE session_id='s1'")
        assert conn.execute("SELECT COUNT(*) FROM sleep_stages").fetchone()[0] == 0

    def test_daily_overview_joins_across_tables(self, conn):
        conn.execute(
            "INSERT INTO daily_activity (date_local, steps) VALUES ('2025-06-15', 9000)"
        )
        conn.execute(
            "INSERT INTO recovery_daily (date_local, biocharge) VALUES ('2025-06-15', 80)"
        )
        row = conn.execute(
            "SELECT * FROM v_daily_overview WHERE date_local='2025-06-15'"
        ).fetchone()
        assert row["steps"] == 9000
        assert row["biocharge"] == 80


class TestSyncEngine:
    def _engine(self, conn):
        class FakeClient:
            def get(self, path, params=None):
                return {}

        return SyncEngine(conn, FakeClient(), min_interval=0.0)

    def test_upsert_is_idempotent(self, conn):
        engine = self._engine(conn)
        engine._write("hr_samples_per_second", [("2025-06-15T00:00:00Z", 60, None)])
        engine._write("hr_samples_per_second", [("2025-06-15T00:00:00Z", 99, None)])
        conn.commit()

        result = conn.execute(
            "SELECT bpm FROM hr_samples_per_second WHERE ts_utc='2025-06-15T00:00:00Z'"
        ).fetchone()
        assert result["bpm"] == 99
        assert conn.execute("SELECT COUNT(*) FROM hr_samples_per_second").fetchone()[0] == 1

    def test_upsert_ignores_non_column_keys(self, conn):
        # Parser helpers like `_stages` must not leak into generated SQL.
        engine = self._engine(conn)
        engine._write(
            "sleep",
            [
                {
                    "session_id": "s1", "date_local": "2025-06-15",
                    "start_ts_utc": "2025-06-15T01:00:00Z",
                    "end_ts_utc": "2025-06-15T09:00:00Z",
                    "duration_min": 480, "deep_min": 90, "light_min": 300,
                    "rem_min": 60, "awake_min": 30, "score": 88,
                    "breath_rate": None, "snore_events": None,
                    "sleep_score_detail": None,
                    "_stages": [],
                }
            ],
        )
        conn.commit()
        assert conn.execute("SELECT COUNT(*) FROM sleep_sessions").fetchone()[0] == 1

    def test_cursor_roundtrips(self, conn):
        engine = self._engine(conn)
        engine._set_cursor("stress", "2025-06-15")
        conn.commit()
        assert engine.get_cursor("stress") == "2025-06-15"

    def test_unknown_stream_raises(self, conn):
        engine = self._engine(conn)
        with pytest.raises(ValueError, match="no writer"):
            engine._write("not_a_stream", [("x",)])

    def test_sleep_write_persists_stages(self, conn):
        engine = self._engine(conn)
        sessions = [
            {
                "session_id": "s1", "date_local": "2025-06-15",
                "start_ts_utc": "2025-06-15T01:00:00Z",
                "end_ts_utc": "2025-06-15T09:00:00Z",
                "duration_min": 480, "deep_min": 90, "light_min": 300,
                "rem_min": 60, "awake_min": 30, "score": 88,
                "breath_rate": None, "snore_events": None,
                "sleep_score_detail": None,
                "_stages": [(0, "deep", "2025-06-15T01:00:00Z", "2025-06-15T02:00:00Z")],
            }
        ]
        engine._write("sleep", sessions)
        conn.commit()
        assert conn.execute("SELECT COUNT(*) FROM sleep_stages").fetchone()[0] == 1

    def test_backfill_advances_cursor_per_day(self, conn):
        engine = self._engine(conn)
        fetched: list[str] = []

        def fetch_one_day(_client, day):
            fetched.append(day.isoformat())
            return [("2025-06-15T00:00:00Z", 60, None)]

        def write(stream, rows):
            engine._write("hr_samples_per_second", [
                {"ts_utc": ts, "bpm": bpm, "quality": None} for ts, bpm, _ in rows
            ])
            return len(rows)

        from datetime import date

        total = engine.backfill(
            "hr_per_second",
            fetch_one_day,
            write,
            start=date(2025, 6, 1),
            end=date(2025, 6, 3),
        )
        assert total == 3
        assert len(fetched) == 3
        assert engine.get_cursor("hr_per_second") == "2025-06-04"

    def test_backfill_resumes_from_cursor(self, conn):
        engine = self._engine(conn)
        engine._set_cursor("stress", "2025-06-03")
        conn.commit()

        fetched: list[str] = []

        def fetch_one_day(_client, day):
            fetched.append(day.isoformat())
            return []

        from datetime import date

        engine.backfill(
            "stress", fetch_one_day, lambda s, r: 0,
            start=date(2025, 6, 1), end=date(2025, 6, 5),
        )
        assert fetched == ["2025-06-03", "2025-06-04", "2025-06-05"]

    def test_one_failing_stream_does_not_abort_others(self, conn):
        class FlakyClient:
            def get(self, path, params=None):
                if "bad" in path:
                    raise ZeppError("boom")
                return {}

        engine = SyncEngine(conn, FlakyClient(), min_interval=0.0)

        from zepplocal.sync import Stream

        streams = {
            "bad": Stream("bad", lambda c, s, e: (_ for _ in ()).throw(ZeppError("boom"))),
            "good": Stream("good", lambda c, s, e: []),
        }
        report = engine.run_streams(streams, start=__import__("datetime").date(2025, 6, 1))
        assert not report.ok
        assert len(report.failed()) == 1
        assert {r.stream for r in report.results} == {"bad", "good"}

    def test_auth_error_is_terminal(self, conn):
        from datetime import date
        from zepplocal.sync import Stream

        def boom(_client, start, end):
            raise AuthError("token expired")

        engine = SyncEngine(conn, object(), min_interval=0.0)
        streams = {
            "a": Stream("a", boom),
            "b": Stream("b", boom),
        }
        report = engine.run_streams(streams, start=date(2025, 6, 1))
        # The break on AuthError means the second stream is never attempted.
        assert len(report.results) == 1
        assert report.results[0].status == "auth_error"


class _Resp:
    def __init__(self, status_code=200, body="{}", headers=None):
        self.status_code = status_code
        self.text = body
        self.headers = headers or {}
        self.ok = 200 <= status_code < 300

    def json(self):
        import json

        return json.loads(self.text)


class _Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.headers = {}
        self.calls = 0

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        return self.responses.pop(0)


class TestStreamDefinitions:
    def test_build_streams_constructs(self):
        """Regression: Stream() describe arg must be passed as a keyword.

        Passing it positionally raised TypeError at import-of-use time, which
        broke every real sync before a single request was made.
        """
        from zepplocal.streams import build_streams

        streams = build_streams("12345")
        assert len(streams) >= 10
        for name, stream in streams.items():
            assert stream.name == name
            assert callable(stream.fetch)
            assert stream.describe


class TestConfigLocalCommands:
    def test_local_commands_work_without_credentials(self, monkeypatch, tmp_path):
        """Regression: `status`/`export` must not require a token."""
        from zepplocal.config import load_config

        monkeypatch.delenv("ZEPP_APP_TOKEN", raising=False)
        monkeypatch.delenv("ZEPP_USER_ID", raising=False)
        monkeypatch.delenv("ZEPP_CONFIG", raising=False)
        monkeypatch.chdir(tmp_path)
        config = load_config(tmp_path / "x.db", require_credentials=False)
        assert config.app_token == ""
        assert config.db_path == tmp_path / "x.db"

    def test_network_commands_still_require_credentials(self, monkeypatch, tmp_path):
        from zepplocal.config import ConfigError, load_config

        monkeypatch.delenv("ZEPP_APP_TOKEN", raising=False)
        monkeypatch.delenv("ZEPP_USER_ID", raising=False)
        monkeypatch.delenv("ZEPP_CONFIG", raising=False)
        monkeypatch.chdir(tmp_path)
        with pytest.raises(ConfigError):
            load_config(tmp_path / "x.db", require_credentials=True)


class TestExport:
    def test_export_to_stdout(self, tmp_path, capsys):
        from zepplocal.cli import main
        from zepplocal import schema

        db = tmp_path / "s.db"
        c = schema.connect(db)
        c.execute(
            "INSERT INTO daily_activity (date_local, steps) VALUES ('2025-06-15', 5000)"
        )
        c.commit()
        c.close()

        assert main(["--db", str(db), "export", "daily"]) == 0
        assert "2025-06-15" in capsys.readouterr().out

    def test_export_to_file(self, tmp_path, capsys):
        from zepplocal.cli import main
        from zepplocal import schema

        db = tmp_path / "e.db"
        c = schema.connect(db)
        c.execute(
            "INSERT INTO daily_activity (date_local, steps) VALUES ('2025-06-15', 5000)"
        )
        c.commit()
        c.close()

        out = tmp_path / "nested" / "daily.csv"
        assert main(["--db", str(db), "export", "daily", "--out", str(out)]) == 0
        assert "2025-06-15" in out.read_text()

    def test_export_empty_is_nonzero(self, tmp_path):
        from zepplocal.cli import main
        from zepplocal import schema

        db = tmp_path / "empty.db"
        schema.connect(db).close()
        assert main(["--db", str(db), "export", "daily"]) == 1


class TestEndToEnd:
    def test_all_streams_write_and_join(self, conn):
        """Every registered stream parses and writes, and views still join."""
        from zepplocal import parsers as P
        from zepplocal.streams import build_streams

        engine = SyncEngine(conn, type("F", (), {"get": lambda s, p, params=None: {}})(),
                            min_interval=0.0)
        build_streams("999")

        payloads = {
            "hr_samples_per_second": {"data": [{"time": 1750000000 + i, "heartRate": 60 + i} for i in range(5)]},
            "hrv": {"hrvList": [{"time": 1750000000, "rmssd": 42}]},
            "sleep": {"sleepList": [{"id": "s1", "startTime": 1750000000, "endTime": 1750003600, "score": 88}]},
            "workouts": {"sportList": [{"id": "w1", "startTime": 1750000000, "endTime": 1750003600}]},
            "spo2": [{"time": 1750000000, "spo2": 97}],
            "spo2_night": {"list": [{"date": "2025-06-15", "odi": 4.2}]},
            "stress": [{"time": 1750000000, "stress": 31}],
            "temperature": [{"time": 1750000000, "skinTempCalibrated": 26}],
            "recovery": {"list": [{"date": "2025-06-15", "biocharge": 78}]},
            "daily_activity": {"list": [{"date": "2025-06-15", "steps": 10234}]},
            "weight": {"weightRecords": [{"time": 1750000000, "weight": 74.2}]},
        }
        parsers = {
            "hr_samples_per_second": P.parse_hr_samples, "hrv": P.parse_hrv_samples,
            "sleep": P.parse_sleep, "workouts": P.parse_workouts,
            "spo2": P.parse_spo2, "spo2_night": P.parse_spo2_night,
            "stress": P.parse_stress, "temperature": P.parse_temperature,
            "recovery": P.parse_recovery, "daily_activity": P.parse_daily_activity,
            "weight": P.parse_weight,
        }

        for name, parse in parsers.items():
            assert engine._write(name, parse(payloads[name])) > 0, name
        conn.commit()

        row = conn.execute(
            "SELECT * FROM v_daily_overview WHERE date_local='2025-06-15'"
        ).fetchone()
        assert row["steps"] == 10234
        assert row["biocharge"] == 78
        assert row["spo2_odi"] == 4.2


class TestClient:
    def test_success_returns_json(self, config):
        session = _Session([_Resp(body='{"data": [{"a": 1}]}')])
        client = ZeppClient(config, session=session)
        assert client.get("/test") == {"data": [{"a": 1}]}

    def test_401_raises_auth_error(self, config):
        client = ZeppClient(config, session=_Session([_Resp(401)]), max_retries=1)
        with pytest.raises(AuthError):
            client.get("/test")

    def test_404_raises_with_path_hint(self, config):
        client = ZeppClient(config, session=_Session([_Resp(404)]), max_retries=1)
        with pytest.raises(ZeppError, match="unknown endpoint"):
            client.get("/test")

    def test_retries_then_succeeds(self, config, monkeypatch):
        monkeypatch.setattr("zepplocal.client.time.sleep", lambda _s: None)
        import random

        monkeypatch.setattr(random, "uniform", lambda a, b: 0)
        session = _Session([_Resp(503), _Resp(body='{"ok":true}')])
        client = ZeppClient(config, session=session, max_retries=3)
        assert client.get("/test") == {"ok": True}
        assert session.calls == 2

    def test_raises_after_retry_exhaustion(self, config, monkeypatch):
        monkeypatch.setattr("zepplocal.client.time.sleep", lambda _s: None)
        monkeypatch.setattr("zepplocal.client.random.uniform", lambda a, b: 0)
        session = _Session([_Resp(429), _Resp(429)])
        client = ZeppClient(config, session=session, max_retries=2)
        with pytest.raises(RateLimited):
            client.get("/test")
        assert session.calls == 2

    def test_body_level_auth_error_is_detected(self, config):
        # Zepp reports token failure with HTTP 200 and an error code in-body.
        session = _Session([_Resp(body='{"error": "0102", "result": "error"}')])
        client = ZeppClient(config, session=session)
        with pytest.raises(AuthError, match="in the body"):
            client.get("/test")

    def test_non_json_raises_with_snippet(self, config):
        session = _Session([_Resp(body="<html>gateway timeout</html>")])
        client = ZeppClient(config, session=session)
        with pytest.raises(ZeppError, match="non-JSON"):
            client.get("/test")

    def test_send_token_header(self, config):
        session = _Session([_Resp(body="{}")])
        client = ZeppClient(config, session=session)
        client.get("/test")
        assert session.headers["apptoken"] == config.app_token


class TestConfig:
    def test_missing_token_raises(self, monkeypatch, tmp_path):
        from zepplocal.config import ConfigError, load_config

        monkeypatch.delenv("ZEPP_APP_TOKEN", raising=False)
        monkeypatch.delenv("ZEPP_USER_ID", raising=False)
        monkeypatch.delenv("ZEPP_CONFIG", raising=False)
        monkeypatch.chdir(tmp_path)
        with pytest.raises(ConfigError, match="app_token"):
            load_config(db_path=tmp_path / "x.db")

    def test_env_overrides_file(self, monkeypatch, tmp_path):
        from zepplocal.config import load_config, write_config

        path = write_config(
            tmp_path / "config.json",
            app_token="file-token", user_id="1", host="api-mifit.zepp.com",
        )
        monkeypatch.setenv("ZEPP_APP_TOKEN", "env-token")
        config = load_config(tmp_path / "x.db", config_path=path)
        assert config.app_token == "env-token"

    def test_write_config_sets_600(self, tmp_path):
        from zepplocal.config import write_config

        path = write_config(
            tmp_path / "c.json", app_token="t", user_id="1", host="h"
        )
        assert oct(path.stat().st_mode)[-3:] == "600"

    def test_token_is_masked(self, config):
        assert "test-token-value" not in config.masked_token()
