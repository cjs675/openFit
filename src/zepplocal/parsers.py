"""Normalisation of raw Zepp payloads into flat rows.

Zepp's API is not schema-stable: field names drift between endpoints and
response versions, timestamps appear as epoch seconds, epoch milliseconds,
and formatted strings, and a single "list" field may arrive as a JSON-encoded
string rather than an array. Rather than hard-code assumptions that break on
the next app release, everything here is written defensively and unknown
shapes degrade to ``None`` instead of raising.

This means a partially-understood payload still imports the fields we do
recognise, and the raw body is preserved in ``raw_payloads`` for re-parsing.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

# Zepp timestamps come as seconds or milliseconds since epoch. Anything past
# ~year 33658 in seconds is certainly milliseconds, but a tighter test avoids
# misreading legitimate far-future values.
_MS_THRESHOLD = 100_000_000_000

_DATE_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
)


class ParseError(ValueError):
    """Raised only when a payload cannot be interpreted at all."""


# Coercion helpers -----------------------------------------------------


def to_utc_iso(value: Any) -> str | None:
    """Coerce an epoch or formatted timestamp to an ISO-8601 UTC string."""
    if value is None or value == "":
        return None

    if isinstance(value, (int, float)):
        seconds = value / 1000 if abs(value) >= _MS_THRESHOLD else value
        try:
            return (
                datetime.fromtimestamp(seconds, tz=timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
        except (OverflowError, OSError, ValueError):
            return None

    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    if re.fullmatch(r"-?\d+(\.\d+)?", text):
        return to_utc_iso(float(text))

    candidate = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        parsed = None
        for fmt in _DATE_FORMATS:
            try:
                parsed = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        if parsed is None:
            return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def to_local_date(ts_utc: str | None, offset_minutes: int = 0) -> str | None:
    """Map a UTC timestamp onto the user's local calendar day."""
    if not ts_utc:
        return None
    try:
        parsed = datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
    except ValueError:
        return None
    if offset_minutes:
        parsed = parsed + timedelta(minutes=offset_minutes)
    return parsed.date().isoformat()


def to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> int | None:
    number = to_float(value)
    return None if number is None else int(number)


def first_present(payload: dict, *names: str) -> Any:
    """Return the first key that exists and is non-null."""
    for name in names:
        if name in payload and payload[name] is not None:
            return payload[name]
    return None


def as_list(value: Any) -> list[dict]:
    """Coerce a value into a list of dicts, decoding JSON strings."""
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            return []
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def unwrap(payload: Any) -> Any:
    """Strip the common ``{"result": ...}`` / ``{"data": [...]}`` envelope."""
    if isinstance(payload, dict):
        for key in ("data", "result", "list"):
            if key in payload and isinstance(payload[key], (dict, list, str)):
                inner = payload[key]
                if key == "data" and isinstance(inner, list):
                    return inner
                if key in ("result", "list") and not isinstance(inner, str):
                    return unwrap(inner)
                if key == "result" and isinstance(inner, str):
                    try:
                        return json.loads(inner)
                    except json.JSONDecodeError:
                        return payload
    return payload


def iter_records(payload: Any, *list_names: str) -> list[dict]:
    """Find the record list inside a response envelope.

    Zepp nests data inconsistently; we try known key names, then fall back to
    scanning for the first array of dicts anywhere in the structure.
    """
    body = unwrap(payload)

    if isinstance(body, list):
        return [item for item in body if isinstance(item, dict)]

    if isinstance(body, dict):
        for name in list_names:
            if name in body:
                return as_list(body[name])
        for value in body.values():
            if isinstance(value, list):
                records = [item for item in value if isinstance(item, dict)]
                if records:
                    return records
            if isinstance(value, str) and value.strip().startswith("["):
                records = as_list(value)
                if records:
                    return records
    return []


# Stream parsers -------------------------------------------------------


def parse_hr_samples(payload: Any) -> list[tuple[str, int, int | None]]:
    """Parse per-second or per-minute HR samples."""
    records = iter_records(payload, "heartRateList", "heartRate", "hrList", "list")
    rows: list[tuple[str, int, int | None]] = []
    for record in records:
        ts = to_utc_iso(
            first_present(record, "time", "timestamp", "ts", "createTime", "startTime")
        )
        bpm = to_int(first_present(record, "heartRate", "heartrate", "value", "bpm"))
        if ts and bpm:
            quality = to_int(first_present(record, "quality", "confidence"))
            rows.append((ts, bpm, quality))
    return rows


def parse_hrv_samples(payload: Any) -> list[tuple[str, float, float | None, str]]:
    records = iter_records(payload, "hrvList", "hrv", "list")
    rows: list[tuple[str, float, float | None, str]] = []
    for record in records:
        ts = to_utc_iso(first_present(record, "time", "timestamp", "ts", "date"))
        rmssd = to_float(
            first_present(record, "rmssd", "rmssdMs", "hrv", "value", "latestHrv")
        )
        if not ts or rmssd is None:
            continue
        sdnn = to_float(first_present(record, "sdnn", "sdnnMs"))
        source = str(first_present(record, "source", "type") or "hrv")
        rows.append((ts, rmssd, sdnn, source))
    return rows


def parse_sleep(payload: Any) -> list[dict]:
    """Parse sleep sessions plus their stage segments."""
    records = iter_records(
        payload, "sleepList", "sleepData", "list", "sleeps", "data"
    )
    sessions: list[dict] = []

    for record in records:
        start = to_utc_iso(
            first_present(record, "startTime", "beginTime", "sleepStart", "start")
        )
        end = to_utc_iso(first_present(record, "endTime", "sleepEnd", "end"))
        if not start or not end:
            continue

        session_id = str(
            first_present(record, "id", "uuid", "sleepId", "sessionId")
            or f"{start}_{end}"
        )

        stages: list[tuple[int, str, str, str]] = []
        stage_records = as_list(
            first_present(record, "stages", "stageList", "sleepStages", "deepInfo")
        )
        for seq, stage_record in enumerate(stage_records):
            stage_start = to_utc_iso(
                first_present(stage_record, "startTime", "start", "beginTime")
            )
            stage_end = to_utc_iso(
                first_present(stage_record, "endTime", "end", "finishTime")
            )
            stage_name = first_present(stage_record, "stage", "type", "stageType")
            if stage_start and stage_end and stage_name is not None:
                stages.append((seq, str(stage_name), stage_start, stage_end))

        sessions.append(
            {
                "session_id": session_id,
                "date_local": to_local_date(start),
                "start_ts_utc": start,
                "end_ts_utc": end,
                "duration_min": to_int(first_present(record, "duration", "total")),
                "deep_min": to_int(
                    first_present(record, "deepDuration", "deep", "deepSleepDuration")
                ),
                "light_min": to_int(
                    first_present(record, "lightDuration", "light", "lightSleepDuration")
                ),
                "rem_min": to_int(first_present(record, "remDuration", "rem")),
                "awake_min": to_int(
                    first_present(record, "awakeDuration", "awake", "wakeDuration")
                ),
                "score": to_int(first_present(record, "score", "sleepScore")),
                "breath_rate": to_float(
                    first_present(record, "breathRate", "breathingRate", "avgBreathRate")
                ),
                "snore_events": to_int(first_present(record, "snoreCount", "snore")),
                "sleep_score_detail": _detail_json(
                    first_present(record, "scoreDetail", "details", "sleepScoreDetail")
                ),
                "_stages": stages,
            }
        )
    return sessions


def _detail_json(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, sort_keys=True)
    except TypeError:
        return str(value)


def parse_workouts(payload: Any) -> list[dict]:
    records = iter_records(payload, "sportList", "workoutList", "list", "elements")
    workouts: list[dict] = []
    for record in records:
        start = to_utc_iso(
            first_present(record, "startTime", "start", "beginTime", "timestamp")
        )
        if not start:
            continue
        end = to_utc_iso(first_present(record, "endTime", "end")) or start
        workout_id = str(
            first_present(record, "id", "uuid", "workoutId", "sportId")
            or f"{start}_{record.get('sportType')}"
        )
        workouts.append(
            {
                "workout_id": workout_id,
                "sport_type": _as_str(
                    first_present(record, "sportType", "type", "sport")
                ),
                "sport_name": _as_str(
                    first_present(record, "sportName", "name", "sportTypeName")
                ),
                "start_ts_utc": start,
                "end_ts_utc": end,
                "duration_min": to_float(first_present(record, "duration", "total")),
                "distance_m": to_float(
                    first_present(record, "distance", "totalDistance")
                ),
                "avg_hr": to_int(first_present(record, "avgHeartRate", "avgHr")),
                "max_hr": to_int(first_present(record, "maxHeartRate", "maxHr")),
                "avg_hr_pct": to_int(
                    first_present(record, "avgHeartRatePct", "avgHrPct")
                ),
                "max_hr_pct": to_int(
                    first_present(record, "maxHeartRatePct", "maxHrPct")
                ),
                "calories": to_float(first_present(record, "calories", "kcal")),
                "training_load": to_float(
                    first_present(record, "trainingLoad", "load")
                ),
                "training_effect": _as_str(
                    first_present(record, "trainingEffect", "effect", "trainingEffectName")
                ),
                "source": _as_str(first_present(record, "source")) or "history",
            }
        )
    return workouts


def parse_spo2(payload: Any) -> list[tuple[str, str, int]]:
    records = iter_records(payload, "spo2List", "list", "bloodOxygenList")
    rows: list[tuple[str, str, int]] = []
    for record in records:
        ts = to_utc_iso(
            first_present(record, "time", "timestamp", "ts", "date", "createTime")
        )
        value = to_int(
            first_present(record, "spo2", "value", "bloodOxygen", "oxygen", "spO2")
        )
        if not ts or not value:
            continue
        kind = str(first_present(record, "type", "kind", "source") or "spot")
        rows.append((ts, kind, value))
    return rows


def parse_spo2_night(payload: Any) -> list[dict]:
    records = iter_records(payload, "list", "spo2List", "data")
    rows: list[dict] = []
    for record in records:
        day = to_local_date(
            to_utc_iso(first_present(record, "date", "time", "timestamp"))
        ) or _as_str(first_present(record, "date"))
        if not day:
            continue
        rows.append(
            {
                "date_local": str(day)[:10],
                "odi": to_float(first_present(record, "odi", "odiValue")),
                "osa_level": _as_str(
                    first_present(record, "osaLevel", "osa", "level", "riskLevel")
                ),
                "odi_events": to_int(first_present(record, "odiCount", "eventCount")),
            }
        )
    return rows


def parse_stress(payload: Any) -> list[tuple[str, int, str]]:
    records = iter_records(payload, "stressList", "list", "stress")
    rows: list[tuple[str, int, str]] = []
    for record in records:
        ts = to_utc_iso(first_present(record, "time", "timestamp", "ts", "createTime"))
        score = to_int(first_present(record, "stress", "value", "score", "stressLevel"))
        if not ts or score is None:
            continue
        rows.append((ts, score, str(first_present(record, "source") or "day")))
    return rows


def parse_temperature(payload: Any) -> list[tuple]:
    records = iter_records(
        payload, "temperatureList", "list", "skinTemperatureList", "tempList"
    )
    rows: list[tuple] = []
    for record in records:
        ts = to_utc_iso(
            first_present(record, "time", "timestamp", "ts", "date", "createTime")
        )
        if not ts:
            continue
        rows.append(
            (
                ts,
                to_int(
                    first_present(
                        record, "skinTempCalibrated", "delta", "temperatureDelta"
                    )
                ),
                to_float(first_present(record, "skinTempAbsolute", "absolute")),
                to_int(first_present(record, "skinTempBaseLine", "baseline")),
                to_int(first_present(record, "skinTempScore", "score")),
                str(first_present(record, "source") or "day"),
            )
        )
    return rows


def parse_recovery(payload: Any) -> list[dict]:
    records = iter_records(
        payload, "trainingLoadList", "list", "data", "chargeList"
    )
    rows: list[dict] = []
    for record in records:
        raw_day = first_present(record, "date", "dateString", "time", "day")
        if raw_day is None:
            continue
        day = to_local_date(to_utc_iso(raw_day)) or str(raw_day)[:10]
        rows.append(
            {
                "date_local": day,
                "biocharge": to_int(
                    first_present(record, "biocharge", "bioCharge", "charge", "score")
                ),
                "biocharge_status": _as_str(
                    first_present(record, "biochargeStatus", "chargeStatus", "status")
                ),
                "exertion": to_int(
                    first_present(record, "exertion", "exertionScore")
                ),
                "exertion_status": _as_str(
                    first_present(record, "exertionStatus")
                ),
                "training_load": to_int(
                    first_present(record, "trainingLoad", "load", "loadRatio")
                ),
                "training_load_ratio": to_float(
                    first_present(record, "trainingLoadRatio", "loadRatio")
                ),
                "vo2_max": to_float(first_present(record, "vo2Max", "vo2", "VO2_MAX")),
                "pai": to_int(first_present(record, "pai", "PAI")),
                "recovery_time_min": to_int(
                    first_present(record, "recoveryTime", "recovery")
                ),
                "updated_at": to_utc_iso(
                    first_present(record, "updateTime", "timestamp")
                ),
            }
        )
    return rows


def parse_daily_activity(payload: Any) -> list[dict]:
    records = iter_records(payload, "dailyList", "list", "data", "activities")
    rows: list[dict] = []
    for record in records:
        raw_day = first_present(record, "date", "dateString", "day", "time")
        if raw_day is None:
            continue
        day = to_local_date(to_utc_iso(raw_day)) or str(raw_day)[:10]
        rows.append(
            {
                "date_local": day,
                "steps": to_int(first_present(record, "steps", "step")),
                "distance_m": to_float(
                    first_present(record, "distance", "totalDistance")
                ),
                "calories_active": to_int(
                    first_present(record, "calories", "activeCalories", "activeKilocalories")
                ),
                "calories_total": to_int(
                    first_present(record, "totalCalories", "totalKilocalories")
                ),
                "standing_hours": to_float(
                    first_present(record, "standingHours", "standHours")
                ),
                "exercise_minutes": to_int(
                    first_present(record, "exerciseMinutes", "exerciseTime")
                ),
                "sit_minutes": to_int(first_present(record, "sitMinutes", "sitTime")),
                "updated_at": to_utc_iso(
                    first_present(record, "updateTime", "timestamp")
                ),
            }
        )
    return rows


def parse_weight(payload: Any) -> list[tuple[str, float | None, str]]:
    records = iter_records(payload, "weightRecords", "list", "data")
    rows: list[tuple[str, float | None, str]] = []
    for record in records:
        ts = to_utc_iso(first_present(record, "time", "date", "timestamp", "createTime"))
        if not ts:
            continue
        rows.append(
            (ts, to_float(first_present(record, "weight", "value", "kg")), "manual")
        )
    return rows


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def dedupe(rows: Iterable[tuple], key_len: int = 2) -> list[tuple]:
    """Collapse duplicate keys, keeping the last occurrence.

    Zepp frequently returns overlapping windows when queried by date range;
    the later record is the more recently corrected one. ``key_len`` is the
    number of leading columns forming the natural key (usually the timestamp
    plus a source/kind discriminator).
    """
    seen: dict[tuple, tuple] = {}
    for row in rows:
        seen[tuple(row[:key_len])] = row
    return list(seen.values())
