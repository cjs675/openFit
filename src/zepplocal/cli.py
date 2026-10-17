"""Command-line interface for zepplocal."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from . import schema
from .client import AuthError, ZeppClient, ZeppError
from .config import DEFAULT_HOSTS, ConfigError, load_config, write_config
from .streams import build_streams
from .sync import SyncEngine

log = logging.getLogger("zepplocal")


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)-7s %(name)s: %(message)s",
    )


def _parse_date(value: str | None, fallback: date) -> date:
    if not value:
        return fallback
    if value == "today":
        return date.today()
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(f"invalid date {value!r}; expected YYYY-MM-DD") from exc


# Commands -------------------------------------------------------------


def cmd_init(args: argparse.Namespace) -> int:
    host = args.host
    if not host:
        print("Region host is required and must match your Zepp account.")
        print("Known hosts:")
        for region, value in DEFAULT_HOSTS.items():
            print(f"  {region:<7} {value}")
        print("\nRe-check by logging into user.huami.com and reading the host")
        print("from any api-mifit*.zepp.com request.")
        return 2

    if host not in DEFAULT_HOSTS.values():
        print(f"warning: {host} is not a known host. Double-check your region.",
              file=sys.stderr)

    path = write_config(
        args.config_path or "config.json",
        app_token=args.token,
        user_id=args.user_id,
        host=host,
        db_path=args.db,
    )
    print(f"wrote {path} (mode 600)")
    print(f"database: {args.db}")
    return 0


def cmd_probe(args: argparse.Namespace) -> int:
    """Hit every endpoint once and report shape, without writing data rows."""
    config = load_config(args.db, config_path=args.config_path)
    client = ZeppClient(config)
    conn = schema.connect(config.db_path)
    uid = config.user_id

    probes: list[tuple[str, str, dict]] = [
        ("hr", f"/users/{uid}/heartRate", {"fromDate": args.start, "toDate": args.end}),
        ("hrv", "/v2/users/me/events",
         {"eventType": "hrv", "subType": "hrv_rmssd", "fromDate": args.start, "toDate": args.end}),
        ("sleep", "/v1/data/band_data.json", {"fromDate": args.start, "toDate": args.end}),
        ("spo2", "/v2/users/me/events",
         {"eventType": "all_day_spo2", "subType": "all_day_spo2", "fromDate": args.start, "toDate": args.end}),
        ("stress", "/v2/users/me/events",
         {"eventType": "all_day_stress", "subType": "stress", "fromDate": args.start, "toDate": args.end}),
        ("temperature", "/v2/users/me/events",
         {"eventType": "all_day_temperature", "subType": "temperature", "fromDate": args.start, "toDate": args.end}),
        ("recovery", f"/v2/watch/users/{uid}/WatchSportStatistics/SPORT_LOAD",
         {"fromDate": args.start, "toDate": args.end}),
        ("spo2_night", f"/users/{uid}/events/dateString", {"fromDate": args.start, "toDate": args.end}),
        ("weight", f"/users/{uid}/members/-1/weightRecords", {}),
    ]

    exit_code = 0
    for name, path, params in probes:
        try:
            body = client.get(path, params=params)
            count = _count_records(body)
            print(f"  ok    {name:<12} {count:>6} record(s)  {path}")
        except AuthError as exc:
            print(f"  AUTH  {name:<12} {exc}", file=sys.stderr)
            return 3
        except ZeppError as exc:
            print(f"  FAIL  {name:<12} {exc}", file=sys.stderr)
            exit_code = 1

    stored = conn.execute("SELECT COUNT(*) AS n FROM raw_payloads").fetchone()["n"]
    print(f"\nraw payloads stored: {stored} (in {config.db_path})")
    conn.close()
    return exit_code


def _count_records(body) -> int:
    from . import parsers as P

    try:
        records = P.iter_records(body)
    except Exception:
        return 0
    return len(records)


def cmd_sync(args: argparse.Namespace) -> int:
    config = load_config(args.db, config_path=args.config_path)
    conn = schema.connect(config.db_path)
    client = ZeppClient(config, keep_raw=not args.no_raw)
    engine = SyncEngine(conn, client, min_interval=args.interval)

    streams = build_streams(config.user_id)
    if args.stream:
        wanted = set(args.stream)
        unknown = wanted - set(streams)
        if unknown:
            print(f"unknown stream(s): {', '.join(sorted(unknown))}", file=sys.stderr)
            print(f"available: {', '.join(sorted(streams))}", file=sys.stderr)
            return 2
        streams = {k: v for k, v in streams.items() if k in wanted}

    end = _parse_date(args.end, date.today())
    if args.days:
        start = end - timedelta(days=args.days)
    else:
        start = _parse_date(args.start, end - timedelta(days=30))

    print(f"syncing {start} .. {end}  (streams: {len(streams)})")
    report = engine.run_streams(streams, start=start, end=end)

    print()
    for result in report.results:
        status = "ok" if result.status == "ok" else result.status
        line = f"  {result.stream:<24} {result.rows:>8} rows  {status}"
        if result.error:
            line += f"\n      {result.error}"
        print(line)
    print(f"\ntotal: {report.total_rows} rows -> {config.db_path}")
    conn.close()
    return 0 if report.ok else 1


def cmd_status(args: argparse.Namespace) -> int:
    config = load_config(
        args.db, config_path=args.config_path, require_credentials=False
    )
    conn = schema.connect(config.db_path)

    print(f"database: {config.db_path}")
    if config.host:
        print(f"host:     {config.host}")
    if config.app_token:
        print(f"token:    {config.masked_token()}")
    else:
        print("token:    <not configured>")
    print()

    print("rows by stream:")
    tables = [
        "hr_samples_per_second", "hr_samples_aggregate", "hrv_samples",
        "sleep_sessions", "sleep_stages", "workouts", "workout_hr_samples",
        "spo2_samples", "spo2_night_windows", "stress_samples",
        "temperature_samples", "recovery_daily", "daily_activity",
        "weight_records",
    ]
    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]
        print(f"  {table:<26} {count:>10}")

    span = conn.execute(
        "SELECT MIN(substr(ts_utc,1,10)) AS lo, MAX(substr(ts_utc,1,10)) AS hi "
        "FROM hr_samples_per_second"
    ).fetchone()
    if span and span["lo"]:
        print(f"\nheart-rate coverage: {span['lo']} .. {span['hi']}")

    sleep_span = conn.execute(
        "SELECT MIN(date_local) AS lo, MAX(date_local) AS hi FROM sleep_sessions"
    ).fetchone()
    if sleep_span and sleep_span["lo"]:
        print(f"sleep coverage:      {sleep_span['lo']} .. {sleep_span['hi']}")

    rows = conn.execute(
        "SELECT stream, cursor, status, last_success_at, error FROM sync_state "
        "ORDER BY stream"
    ).fetchall()
    if rows:
        print("\nsync state:")
        for row in rows:
            status = row["status"] or "pending"
            print(f"  {row['stream']:<24} cursor={row['cursor'] or '-':<12} {status}")
            if row["error"]:
                print(f"      {row['error']}")

    conn.close()
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    """Export a view to CSV."""
    config = load_config(
        args.db, config_path=args.config_path, require_credentials=False
    )
    conn = schema.connect(config.db_path)

    view_map = {
        "daily": "v_daily_overview",
        "sleep": "v_sleep_summary",
        "hr": "v_daily_heart_rate",
    }
    view = view_map.get(args.view)
    if not view:
        print(f"unknown view {args.view!r}; choose from {', '.join(view_map)}",
              file=sys.stderr)
        return 2

    sql = f"SELECT * FROM {view}"
    params: list = []
    if args.from_date or args.to_date:
        date_col = {
            "v_daily_overview": "date_local",
            "v_sleep_summary": "date_local",
            "v_daily_heart_rate": "date_local",
        }[view]
        clauses = []
        if args.from_date:
            clauses.append(f"{date_col} >= ?")
            params.append(args.from_date)
        if args.to_date:
            clauses.append(f"{date_col} <= ?")
            params.append(args.to_date)
        sql += " WHERE " + " AND ".join(clauses)

    rows = conn.execute(sql, params).fetchall()
    if not rows:
        print("no rows matched", file=sys.stderr)
        return 1

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", newline="") as handle:
            _write_csv(handle, rows)
        print(f"wrote {len(rows)} rows to {out_path}", file=sys.stderr)
    else:
        _write_csv(sys.stdout, rows)

    conn.close()
    return 0


def _write_csv(handle: Any, rows: list) -> None:
    writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(dict(r) for r in rows)


def cmd_schema(args: argparse.Namespace) -> int:
    """Print the schema, for the iOS port."""
    conn = schema.connect(":memory:" if args.memory else "data/schema-dump.db")
    for row in conn.execute(
        "SELECT type, name, sql FROM sqlite_master "
        "WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY type, name"
    ):
        print(f"-- {row['type']}: {row['name']}")
        print(row["sql"] + ";\n")
    conn.close()
    return 0


# Parser ---------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="zepplocal",
        description="Local-first sync for AmazFit Helio Strap data",
    )
    parser.add_argument("--db", help="path to SQLite database")
    parser.add_argument("--config", dest="config_path", help="path to config.json")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init", help="write config.json")
    p_init.add_argument("--token", required=True, help="Zepp apptoken")
    p_init.add_argument("--user-id", required=True)
    p_init.add_argument("--host", required=True, help="region host, e.g. api-mifit-us2.zepp.com")
    p_init.set_defaults(func=cmd_init)

    p_probe = sub.add_parser("probe", help="test every endpoint, write nothing")
    p_probe.add_argument("--start", default=(date.today() - timedelta(days=3)).isoformat())
    p_probe.add_argument("--end", default=date.today().isoformat())
    p_probe.set_defaults(func=cmd_probe)

    p_sync = sub.add_parser("sync", help="fetch and store data")
    p_sync.add_argument("--start", help="start date YYYY-MM-DD")
    p_sync.add_argument("--end", help="end date YYYY-MM-DD")
    p_sync.add_argument("--days", type=int, help="shorthand for last N days")
    p_sync.add_argument("--stream", action="append", help="limit to a stream (repeatable)")
    p_sync.add_argument("--interval", type=float, default=0.25, help="min seconds between requests")
    p_sync.add_argument("--no-raw", action="store_true", help="skip raw payload archive")
    p_sync.set_defaults(func=cmd_sync)

    p_status = sub.add_parser("status", help="show row counts and sync state")
    p_status.set_defaults(func=cmd_status)

    p_export = sub.add_parser("export", help="export a view to CSV")
    p_export.add_argument("view", choices=["daily", "sleep", "hr"])
    p_export.add_argument("--from-date")
    p_export.add_argument("--to-date")
    p_export.add_argument("--out")
    p_export.set_defaults(func=cmd_export)

    p_schema = sub.add_parser("schema", help="print SQL schema")
    p_schema.add_argument("--memory", action="store_true", default=True)
    p_schema.set_defaults(func=cmd_schema)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)
    try:
        return args.func(args)
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    except AuthError as exc:
        print(f"auth error: {exc}", file=sys.stderr)
        return 3
    except ZeppError as exc:
        print(f"api error: {exc}", file=sys.stderr)
        return 4
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
