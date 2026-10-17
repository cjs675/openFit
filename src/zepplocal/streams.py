"""Endpoint definitions mapping Zepp API paths to parsers and tables.

Paths here are taken from the reverse-engineered community tooling
(zepp-health-cli, zepp-bridge) and are known to drift. Every one is treated
as a hypothesis: a 404 or empty response is reported per-stream rather than
aborting the run, so a single wrong path never blocks the rest of your data.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Callable

from . import parsers as P
from .client import ZeppClient
from .sync import Stream

# Zepp uses "today" as a relative keyword on several endpoints.
def _window(start: date, end: date) -> dict[str, Any]:
    return {"fromDate": start.isoformat(), "toDate": end.isoformat()}


def _events_params(start: date, end: date, event_type: str, sub_type: str) -> dict:
    params = _window(start, end)
    params["eventType"] = event_type
    params["subType"] = sub_type
    return params


def build_streams(user_id: str) -> dict[str, Stream]:
    """Construct the stream table bound to a specific user id."""

    def hr_second(_client: ZeppClient, start: date, end: date):
        payload = _client.get(
            f"/users/{user_id}/heartRate", params=_window(start, end)
        )
        return P.parse_hr_samples(payload)

    def hrv(_client: ZeppClient, start: date, end: date):
        payload = _client.get(
            "/v2/users/me/events",
            params=_events_params(start, end, "hrv", "hrv_rmssd"),
        )
        return P.parse_hrv_samples(payload)

    def sleep(_client: ZeppClient, start: date, end: date):
        payload = _client.get("/v1/data/band_data.json", params=_window(start, end))
        return P.parse_sleep(payload)

    def workouts(_client: ZeppClient, start: date, end: date):
        rows: list[dict] = []
        for sport in ("run", "walk", "workout", "strength_training"):
            payload = _client.get(f"/v1/sport/{sport}/history.json", params=_window(start, end))
            rows.extend(P.parse_workouts(payload))
        return rows

    def spo2(_client: ZeppClient, start: date, end: date):
        payload = _client.get(
            "/v2/users/me/events",
            params=_events_params(start, end, "all_day_spo2", "all_day_spo2"),
        )
        return P.parse_spo2(payload)

    def spo2_night(_client: ZeppClient, start: date, end: date):
        payload = _client.get(
            f"/users/{user_id}/events/dateString", params=_window(start, end)
        )
        return P.parse_spo2_night(payload)

    def stress(_client: ZeppClient, start: date, end: date):
        payload = _client.get(
            "/v2/users/me/events",
            params=_events_params(start, end, "all_day_stress", "stress"),
        )
        return P.parse_stress(payload)

    def temperature(_client: ZeppClient, start: date, end: date):
        payload = _client.get(
            "/v2/users/me/events",
            params=_events_params(start, end, "all_day_temperature", "temperature"),
        )
        return P.parse_temperature(payload)

    def recovery(_client: ZeppClient, start: date, end: date):
        payload = _client.get(
            f"/v2/watch/users/{user_id}/WatchSportStatistics/SPORT_LOAD",
            params=_window(start, end),
        )
        return P.parse_recovery(payload)

    def daily_activity(_client: ZeppClient, start: date, end: date):
        payload = _client.get(
            "/v2/users/me/events",
            params=_events_params(start, end, "daily-health", "steps"),
        )
        return P.parse_daily_activity(payload)

    def weight(_client: ZeppClient, start: date, end: date):
        payload = _client.get(f"/users/{user_id}/members/-1/weightRecords")
        return P.parse_weight(payload)

    return {
        "hr_samples_per_second": Stream("hr_samples_per_second", hr_second, describe="1Hz heart rate"),
        "hrv": Stream("hrv", hrv, describe="HRV / RMSSD"),
        "sleep": Stream("sleep", sleep, describe="sleep sessions + stages"),
        "workouts": Stream("workouts", workouts, describe="workouts / exercises"),
        "spo2": Stream("spo2", spo2, describe="blood oxygen"),
        "spo2_night": Stream("spo2_night", spo2_night, describe="ODI / OSA windows"),
        "stress": Stream("stress", stress, describe="stress scores"),
        "temperature": Stream("temperature", temperature, describe="skin temperature"),
        "recovery": Stream("recovery", recovery, describe="BioCharge / training load"),
        "daily_activity": Stream("daily_activity", daily_activity, describe="steps / activity"),
        "weight": Stream("weight", weight, describe="weight records"),
    }


# Per-second HR is served as COS zip archives referenced from a manifest
# rather than inline JSON, so it has its own fetch path.
def fetch_hr_blob_manifest(
    client: ZeppClient, start: date, end: date
) -> list[dict]:
    payload = client.get("/users/me/fileInfo/events", params=_window(start, end))
    records = P.iter_records(payload, "list", "events", "fileList")
    rows: list[dict] = []
    for record in records:
        blob_id = record.get("blobId") or record.get("id")
        if not blob_id:
            continue
        rows.append(
            {
                "blob_id": str(blob_id),
                "url": record.get("url") or record.get("downloadUrl"),
                "date_local": str(
                    record.get("date") or record.get("dateString") or ""
                )[:10],
                "state": "pending",
                "bytes": None,
                "rows": None,
                "error": None,
                "updated_at": None,
            }
        )
    return rows
