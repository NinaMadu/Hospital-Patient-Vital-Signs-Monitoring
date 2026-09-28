"""Tests for the daily lab-file simulator (simulators/lab_batch/main.py)."""
import csv
import json
from datetime import datetime, timedelta, timezone

import pytest

from simulators.lab_batch.main import (
    COLUMNS,
    LAB_CATALOGUE,
    LabSimulator,
    file_path,
    marker_path,
    run_forever,
    write_day,
)

EPOCH = datetime(2026, 9, 28, tzinfo=timezone.utc)
DAY_S = 300
TESTS = list(LAB_CATALOGUE)


def make_sim(**kw) -> LabSimulator:
    return LabSimulator(epoch=EPOCH, day_seconds=DAY_S, tests=TESTS, **kw)


def out_of_range(r) -> bool:
    return not r["reference_low"] <= r["result_value"] <= r["reference_high"]


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ---- content ----------------------------------------------------------------


def test_rows_have_contract_columns_and_valid_values():
    rows = make_sim().rows_for_day(0)
    assert rows
    for r in rows:
        assert tuple(r) == COLUMNS
        assert r["test_type"] in LAB_CATALOGUE
        assert r["unit"] == LAB_CATALOGUE[r["test_type"]].unit
        assert r["reference_low"] < r["reference_high"]
        assert r["result_value"] >= 0


def test_collected_at_falls_inside_the_sim_day():
    day = 4
    start = EPOCH + timedelta(seconds=day * DAY_S)
    for r in make_sim().rows_for_day(day):
        assert start <= datetime.fromisoformat(r["collected_at"]) < start + timedelta(seconds=DAY_S)


def test_same_day_is_deterministic_and_days_differ():
    sim = make_sim()
    assert sim.rows_for_day(2) == make_sim().rows_for_day(2)
    assert sim.rows_for_day(2) != sim.rows_for_day(3)
    assert make_sim(seed=1).rows_for_day(2) != make_sim(seed=2).rows_for_day(2)


def test_abnormal_patient_always_present_with_far_high_crp_and_lactate():
    sim = make_sim(missing_patient_probability=0.9, missing_result_probability=0.9)
    for day in range(20):
        p7 = {r["test_type"]: r for r in sim.rows_for_day(day) if r["patient_id"] == "P007"}
        assert set(p7) == set(TESTS)
        for test in ("crp", "lactate"):
            assert p7[test]["result_value"] > 1.5 * p7[test]["reference_high"]


def test_feed_contains_missing_patients_missing_tests_and_out_of_range_values():
    sim = make_sim()
    days = [sim.rows_for_day(d) for d in range(30)]
    all_rows = [r for rows in days for r in rows]
    # Some day has a patient with no labs at all (-> LAB_UNAVAILABLE downstream).
    assert any(len({r["patient_id"] for r in rows}) < 15 for rows in days)
    # Some present patient is missing a single test.
    per_patient_day = {}
    for d, rows in enumerate(days):
        for r in rows:
            per_patient_day.setdefault((d, r["patient_id"]), set()).add(r["test_type"])
    assert any(len(tests) < len(TESTS) for tests in per_patient_day.values())
    # Mild and far out-of-range values, on both sides, for patients other than P007.
    others = [r for r in all_rows if r["patient_id"] != "P007" and out_of_range(r)]
    assert any(r["result_value"] > 1.5 * r["reference_high"] for r in others)
    assert any(r["reference_high"] < r["result_value"] <= 1.5 * r["reference_high"] for r in others)
    assert any(r["result_value"] < r["reference_low"] for r in others)


def test_zero_probabilities_give_a_complete_normal_feed():
    sim = make_sim(missing_patient_probability=0, missing_result_probability=0,
                   out_of_range_probability=0, abnormal_patients=())
    rows = sim.rows_for_day(0)
    assert len(rows) == 15 * len(TESTS)
    assert not any(out_of_range(r) for r in rows)


def test_negative_day_and_unknown_test_are_rejected():
    with pytest.raises(ValueError):
        make_sim().rows_for_day(-1)
    with pytest.raises(ValueError):
        LabSimulator(epoch=EPOCH, day_seconds=DAY_S, tests=["glucose"])


# ---- files ------------------------------------------------------------------


def test_write_day_creates_csv_and_marker_only(tmp_path):
    sim = make_sim()
    path = write_day(sim, tmp_path, 5)
    assert path == file_path(tmp_path, 5)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["labs_day=5.csv", "labs_day=5.csv._SUCCESS"]

    rows = read_csv(path)
    assert tuple(rows[0]) == COLUMNS
    marker = json.loads(marker_path(tmp_path, 5).read_text())
    assert marker["sim_day"] == 5
    assert marker["rows"] == len(rows) == len(sim.rows_for_day(5))
    assert marker["patients"] == len({r["patient_id"] for r in rows})


def test_existing_day_is_skipped_unless_forced(tmp_path):
    sim = make_sim()
    write_day(sim, tmp_path, 1)
    file_path(tmp_path, 1).write_text("tampered")
    assert write_day(sim, tmp_path, 1) is None
    assert file_path(tmp_path, 1).read_text() == "tampered"

    assert write_day(sim, tmp_path, 1, force=True) is not None
    assert len(read_csv(file_path(tmp_path, 1))) == len(sim.rows_for_day(1))


def test_rewriting_a_day_gives_the_same_file(tmp_path):
    sim = make_sim()
    write_day(sim, tmp_path, 2)
    first = file_path(tmp_path, 2).read_bytes()
    write_day(sim, tmp_path, 2, force=True)
    assert file_path(tmp_path, 2).read_bytes() == first


def test_failed_write_leaves_no_visible_file(tmp_path, monkeypatch):
    import simulators.lab_batch.main as lab

    def crash(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr(lab.os, "replace", crash)
    with pytest.raises(OSError):
        write_day(make_sim(), tmp_path, 0)
    assert not file_path(tmp_path, 0).exists()
    assert not marker_path(tmp_path, 0).exists()


# ---- clock loop -------------------------------------------------------------


def test_run_forever_writes_each_day_after_it_ends(tmp_path):
    clock = {"now": EPOCH + timedelta(seconds=2 * DAY_S + 10)}  # 10 s into day 2

    def sleep(seconds):
        clock["now"] += timedelta(seconds=seconds)

    run_forever(
        make_sim(), tmp_path,
        now=lambda: clock["now"],
        sleep=sleep,
        should_stop=lambda: clock["now"] >= EPOCH + timedelta(seconds=4 * DAY_S + 5),
    )
    # Day 1 is caught up at start; days 2 and 3 are written as they end; day 4 is still running.
    written = sorted(p.name for p in tmp_path.glob("labs_day=*.csv"))
    assert written == ["labs_day=1.csv", "labs_day=2.csv", "labs_day=3.csv"]


# ---- metrics (B10) ----------------------------------------------------------


def test_write_day_updates_metrics(tmp_path):
    from prometheus_client import REGISTRY

    def value(name):
        return REGISTRY.get_sample_value(name) or 0

    files_before, rows_before = value("ward_lab_sim_files_written_total"), value("ward_lab_sim_rows_written_total")
    sim = make_sim()
    write_day(sim, tmp_path, 6)
    rows = sim.rows_for_day(6)

    assert value("ward_lab_sim_files_written_total") == files_before + 1
    assert value("ward_lab_sim_rows_written_total") == rows_before + len(rows)
    assert value("ward_lab_sim_last_written_sim_day") == 6
    assert value("ward_lab_sim_last_file_timestamp_seconds") > 0
    assert value("ward_lab_sim_missing_patients") == 15 - len({r["patient_id"] for r in rows})

    write_day(sim, tmp_path, 6)            # skipped: already written -> no change
    assert value("ward_lab_sim_files_written_total") == files_before + 1


def test_skipped_day_still_reports_the_existing_file(tmp_path):
    from prometheus_client import REGISTRY

    sim = make_sim()
    write_day(sim, tmp_path, 8)
    write_day(sim, tmp_path, 9)
    write_day(sim, tmp_path, 8)            # restart catching up an existing day
    assert REGISTRY.get_sample_value("ward_lab_sim_last_written_sim_day") == 8
    assert REGISTRY.get_sample_value("ward_lab_sim_last_file_timestamp_seconds") == \
        marker_path(tmp_path, 8).stat().st_mtime
