"""Tests for payload normalisation.

These focus on the shapes that actually break Zepp clients in practice:
JSON-encoded strings where arrays are expected, epoch seconds vs
milliseconds, and unknown field names.
"""

from __future__ import annotations

import json

import pytest

from zepplocal import parsers as P


class TestTimestamps:
    def test_epoch_seconds(self):
        assert P.to_utc_iso(1750000000) == "2025-06-15T15:06:40Z"

    def test_epoch_milliseconds(self):
        assert P.to_utc_iso(1750000000000) == "2025-06-15T15:06:40Z"

    def test_numeric_string_is_coerced(self):
        assert P.to_utc_iso("1750000000") == "2025-06-15T15:06:40Z"

    def test_iso_with_zulu(self):
        assert P.to_utc_iso("2025-06-15T15:06:40Z") == "2025-06-15T15:06:40Z"

    def test_iso_with_offset_normalises_to_utc(self):
        assert P.to_utc_iso("2025-06-15T17:06:40+02:00") == "2025-06-15T15:06:40Z"

    def test_naive_string_assumed_utc(self):
        assert P.to_utc_iso("2025-06-15 15:06:40") == "2025-06-15T15:06:40Z"

    def test_unparseable_returns_none(self):
        assert P.to_utc_iso("not a date") is None

    def test_none_and_empty(self):
        assert P.to_utc_iso(None) is None
        assert P.to_utc_iso("") is None

    def test_absurd_epoch_does_not_raise(self):
        assert P.to_utc_iso(1e20) is None


class TestLocalDate:
    def test_utc_day(self):
        assert P.to_local_date("2025-06-15T15:06:40Z") == "2025-06-15"

    def test_offset_shifts_day_boundary(self):
        # 01:00 UTC is still the previous day in UTC-5.
        assert P.to_local_date("2025-06-15T01:00:00Z", -300) == "2025-06-14"
        assert P.to_local_date("2025-06-15T01:00:00Z", 300) == "2025-06-15"


class TestCoercion:
    def test_to_int_from_string(self):
        assert P.to_int("42") == 42

    def test_to_int_from_float(self):
        assert P.to_int(42.9) == 42

    def test_bool_is_not_a_number(self):
        assert P.to_int(True) is None

    def test_to_float_none(self):
        assert P.to_float(None) is None

    def test_first_present_skips_nulls(self):
        assert P.first_present({"a": None, "b": 7}, "a", "b") == 7

    def test_first_present_falls_back_to_alias(self):
        assert P.first_present({"rmssdMs": 33}, "rmssd", "rmssdMs") == 33


class TestListHandling:
    def test_json_encoded_string_is_decoded(self):
        assert P.as_list('[{"a":1}]') == [{"a": 1}]

    def test_dict_becomes_single_item_list(self):
        assert P.as_list({"a": 1}) == [{"a": 1}]

    def test_garbage_string_yields_empty(self):
        assert P.as_list("{{{") == []

    def test_non_dict_items_filtered(self):
        assert P.as_list([{"a": 1}, 5, "x"]) == [{"a": 1}]

    def test_iter_records_finds_bare_list(self):
        assert P.iter_records([{"a": 1}]) == [{"a": 1}]

    def test_iter_records_unwraps_data_envelope(self):
        assert P.iter_records({"data": [{"a": 1}]}) == [{"a": 1}]

    def test_iter_records_falls_back_to_scanning(self):
        assert P.iter_records({"weirdKey": [{"a": 1}]}) == [{"a": 1}]

    def test_iter_records_on_empty(self):
        assert P.iter_records({}) == []
        assert P.iter_records(None) == []


class TestStreamParsers:
    def test_hr_samples(self):
        payload = {
            "data": [
                {"time": 1750000000, "heartRate": 62, "quality": 1},
                {"timestamp": 1750000060, "heartRate": 71},
            ]
        }
        rows = P.parse_hr_samples(payload)
        assert len(rows) == 2
        assert rows[0][1] == 62
        assert rows[1][1] == 71

    def test_hr_samples_skip_incomplete(self):
        payload = {"data": [{"time": 1750000000}, {"heartRate": 70}]}
        assert P.parse_hr_samples(payload) == []

    def test_hrv_samples(self):
        payload = {"hrvList": [{"time": 1750000000, "rmssd": 42, "sdnn": 55}]}
        rows = P.parse_hrv_samples(payload)
        assert rows[0][1] == 42.0
        assert rows[0][2] == 55.0

    def test_sleep_with_stages(self):
        payload = {
            "sleepList": [
                {
                    "id": "s1",
                    "startTime": 1750000000,
                    "endTime": 17500030000,
                    "deepDuration": 90,
                    "score": 88,
                    "stages": [
                        {"stage": "deep", "startTime": 1750000000, "endTime": 1750005400},
                        {"stage": "light", "startTime": 1750005400, "endTime": 1750009000},
                    ],
                }
            ]
        }
        sessions = P.parse_sleep(payload)
        assert len(sessions) == 1
        session = sessions[0]
        assert session["session_id"] == "s1"
        assert session["deep_min"] == 90
        assert session["score"] == 88
        assert len(session["_stages"]) == 2
        assert session["_stages"][0][1] == "deep"

    def test_sleep_synthesises_id_when_missing(self):
        payload = [{"startTime": 1750000000, "endTime": 1750003600}]
        sessions = P.parse_sleep(payload)
        assert sessions[0]["session_id"].startswith("2025-")

    def test_sleep_skips_sessions_without_end(self):
        assert P.parse_sleep([{"startTime": 1750000000}]) == []

    def test_workouts(self):
        payload = {
            "sportList": [
                {
                    "id": "w1",
                    "sportType": 8,
                    "startTime": 1750000000,
                    "endTime": 1750003600,
                    "avgHeartRate": 140,
                    "maxHeartRate": 178,
                    "distance": 5000,
                }
            ]
        }
        workouts = P.parse_workouts(payload)
        assert workouts[0]["workout_id"] == "w1"
        assert workouts[0]["avg_hr"] == 140
        assert workouts[0]["distance_m"] == 5000.0

    def test_spo2(self):
        payload = [{"time": 1750000000, "spo2": 97, "type": "sleep"}]
        rows = P.parse_spo2(payload)
        assert rows[0][2] == 97

    def test_spo2_rejects_zero(self):
        # 0 SpO2 is not physiologically meaningful and signals a bad parse.
        assert P.parse_spo2([{"time": 1750000000, "spo2": 0}]) == []

    def test_temperature_delta_is_hundredths(self):
        payload = [{"time": 1750000000, "skinTempCalibrated": 26}]
        rows = P.parse_temperature(payload)
        assert rows[0][1] == 26

    def test_recovery_daily(self):
        payload = {"list": [{"date": "2025-06-15", "biocharge": 72, "pai": 45}]}
        rows = P.parse_recovery(payload)
        assert rows[0]["date_local"] == "2025-06-15"
        assert rows[0]["biocharge"] == 72

    def test_stress(self):
        payload = [{"time": 1750000000, "stress": 30}]
        assert P.parse_stress(payload)[0][1] == 30

    def test_daily_activity(self):
        payload = {"list": [{"date": "2025-06-15", "steps": 8000, "distance": 6000}]}
        rows = P.parse_daily_activity(payload)
        assert rows[0]["steps"] == 8000

    def test_unknown_shape_yields_empty_not_error(self):
        assert P.parse_hr_samples({"totally": "unexpected"}) == []
        assert P.parse_sleep("garbage") == []
        assert P.parse_workouts(None) == []


class TestDedupe:
    def test_keeps_last_occurrence(self):
        rows = [("t1", "a", 60), ("t1", "a", 70)]
        result = P.dedupe(rows, key_len=2)
        assert len(result) == 1
        assert result[0][2] == 70

    def test_preserves_distinct_keys(self):
        rows = [("t1", 60), ("t2", 70)]
        assert len(P.dedupe(rows, key_len=2)) == 2
