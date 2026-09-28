"""Daily pathology-lab file simulator.

Owner: Member B.
When simulated day N ends, writes <landing_dir>/labs_day=N.csv atomically, then the marker
labs_day=N.csv._SUCCESS. The Airflow DAG waits for the marker, so it never reads a half-written
file. Some patients have no labs (-> LAB_UNAVAILABLE) and some results are out of range.

    python -m simulators.lab_batch.main                              # follow the sim clock
    python -m simulators.lab_batch.main --day 3                      # write one day now
    python -m simulators.lab_batch.main --day 3 --force              # rewrite an existing day
    python -m simulators.lab_batch.main --day 0 --output-dir data/landing/labs   # from the host

Each day's contents come from a random generator seeded with (seed, day), so writing a day
again gives exactly the same file. The reference range is written into every row, so the
batch layer checks ranges without its own copy of the lab catalogue.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
import random
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from common import metrics
from common.config import get_settings
from common.lab_feed import COLUMNS, file_path, marker_path  # the file contract, shared with the DAG
from common.logger import get_logger


@dataclass(frozen=True)
class LabTest:
    unit: str
    low: float
    high: float
    decimals: int
    abnormal_side: str  # which way results usually go wrong: "high", "low" or "both"


# Adult reference ranges, simplified for a simulation (not clinical guidance).
LAB_CATALOGUE: dict[str, LabTest] = {
    "potassium":   LabTest("mmol/L",   3.5,   5.1, 1, "both"),
    "creatinine":  LabTest("umol/L",  60.0, 110.0, 0, "high"),
    "haemoglobin": LabTest("g/L",    120.0, 170.0, 0, "low"),
    "wbc":         LabTest("10^9/L",   4.0,  11.0, 1, "both"),
    "crp":         LabTest("mg/L",     0.0,   5.0, 1, "high"),
    "lactate":     LabTest("mmol/L",   0.5,   2.0, 1, "high"),
}

# Tests that are always far too high for an `abnormal_patients` entry (a septic picture,
# which fits the tachycardia + falling SpO2 spike in the vitals demo).
ABNORMAL_PATIENT_TESTS = ("crp", "lactate")


log = get_logger("lab-simulator")

# Served on /metrics (lab_simulator.metrics_port) while the service runs; Prometheus scrapes it.
FILES_WRITTEN = metrics.counter("lab_sim_files_written", "Lab files written by the simulator")
ROWS_WRITTEN = metrics.counter("lab_sim_rows_written", "Lab result rows written")
LAST_DAY = metrics.gauge("lab_sim_last_written_sim_day", "Sim day of the newest lab file written")
LAST_FILE_TIME = metrics.gauge("lab_sim_last_file_timestamp_seconds",
                               "Unix time the newest lab file was written")
MISSING_PATIENTS = metrics.gauge("lab_sim_missing_patients",
                                 "Patients with no labs in the newest file (-> LAB_UNAVAILABLE)")
OUT_OF_RANGE = metrics.gauge("lab_sim_out_of_range_results", "Out-of-range results in the newest file")


# Same formula as common.sim_clock: sim_day = floor((ts - SIM_EPOCH) / SIM_DAY_SECONDS).
# Kept local because LabSimulator takes epoch/day_seconds as arguments, so tests can run it
# on their own clock instead of the global settings.
def _sim_day(ts: datetime, epoch: datetime, day_seconds: int) -> int:
    return math.floor((ts - epoch).total_seconds() / day_seconds)


def _day_start(day: int, epoch: datetime, day_seconds: int) -> datetime:
    return epoch + timedelta(seconds=day * day_seconds)


class LabSimulator:
    """Builds one day's lab rows. Pure Python: no files, no clock, easy to test."""

    def __init__(
        self,
        epoch: datetime,
        day_seconds: int,
        tests: list[str],
        patient_count: int = 15,
        id_prefix: str = "P",
        seed: int = 7,
        missing_result_probability: float = 0.05,
        missing_patient_probability: float = 0.10,
        out_of_range_probability: float = 0.12,
        far_out_of_range_share: float = 0.30,
        abnormal_patients: tuple[str, ...] = ("P007",),
    ):
        unknown = set(tests) - set(LAB_CATALOGUE)
        if unknown:
            raise ValueError(f"no reference range for lab tests: {sorted(unknown)}")
        self.epoch = epoch
        self.day_seconds = day_seconds
        self.tests = list(tests)
        self.patients = [f"{id_prefix}{i:03d}" for i in range(1, patient_count + 1)]
        self.seed = seed
        self.missing_result_probability = missing_result_probability
        self.missing_patient_probability = missing_patient_probability
        self.out_of_range_probability = out_of_range_probability
        self.far_out_of_range_share = far_out_of_range_share
        self.abnormal_patients = set(abnormal_patients)

    def rows_for_day(self, day: int) -> list[dict]:
        """All lab rows collected during simulated day `day`, sorted by patient and test."""
        if day < 0:
            raise ValueError(f"sim day must be >= 0, got {day}")
        # A string seed is hashed with SHA-512, so it is stable across runs and machines.
        rng = random.Random(f"labs:{self.seed}:{day}")
        day_start = _day_start(day, self.epoch, self.day_seconds)
        rows = []
        for pid in self.patients:
            abnormal_patient = pid in self.abnormal_patients
            # Draw every random number in a fixed order, so one patient's result never
            # shifts another patient's values.
            patient_missing = rng.random() < self.missing_patient_probability
            # One blood draw per patient, somewhere in the middle 80 % of the day.
            collected_at = day_start + timedelta(seconds=rng.uniform(0.1, 0.9) * self.day_seconds)
            for test in self.tests:
                value = self._value(rng, LAB_CATALOGUE[test], abnormal_patient and test in ABNORMAL_PATIENT_TESTS)
                test_missing = rng.random() < self.missing_result_probability
                if abnormal_patient:
                    patient_missing = test_missing = False  # keep the demo patient visible
                if patient_missing or test_missing:
                    continue
                spec = LAB_CATALOGUE[test]
                rows.append({
                    "patient_id": pid,
                    "test_type": test,
                    "result_value": value,
                    "unit": spec.unit,
                    "reference_low": spec.low,
                    "reference_high": spec.high,
                    "collected_at": collected_at.isoformat(timespec="seconds"),
                })
        return rows

    def _value(self, rng: random.Random, spec: LabTest, force_far_high: bool) -> float:
        width = spec.high - spec.low
        normal = min(max(rng.gauss((spec.low + spec.high) / 2, width / 6), spec.low), spec.high)
        out_of_range = rng.random() < self.out_of_range_probability
        far = rng.random() < self.far_out_of_range_share
        side = spec.abnormal_side if spec.abnormal_side != "both" else rng.choice(["high", "low"])
        mild_factor, far_factor = rng.uniform(1.05, 1.4), rng.uniform(1.6, 2.5)

        if force_far_high:
            value = spec.high * far_factor
        elif not out_of_range:
            value = normal
        elif side == "low" and spec.low > 0:
            # "Far" below means under low / 1.5 (the mirror of 1.5x above high).
            value = spec.low / (far_factor if far else mild_factor)
        else:
            value = spec.high * (far_factor if far else mild_factor)
        return round(value, spec.decimals)


# ----------------------------------------------------------------- files --

def _write_atomic(path: Path, text: str) -> None:
    """Write to a hidden temp file in the same folder, flush to disk, then rename.

    A rename within one folder is atomic: readers see either no file or the whole file.
    The leading dot keeps the temp file out of `labs_day=*.csv` patterns.
    """
    tmp = path.with_name(f".{path.name}.tmp")
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def write_day(sim: LabSimulator, output_dir: Path, day: int, force: bool = False) -> Path | None:
    """Write labs_day=N.csv and its _SUCCESS marker. Returns None if the day already exists."""
    output_dir.mkdir(parents=True, exist_ok=True)
    target, marker = file_path(output_dir, day), marker_path(output_dir, day)
    if marker.exists() and not force:
        log.info("day_skipped", sim_day=day, reason="already written", path=str(target))
        # The file exists, so after a restart the "newest file" gauges still tell the truth.
        LAST_DAY.set(day)
        LAST_FILE_TIME.set(marker.stat().st_mtime)
        return None

    marker.unlink(missing_ok=True)  # on --force, readers must not see an old marker with new data
    rows = sim.rows_for_day(day)

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    _write_atomic(target, buf.getvalue())

    # The marker is written last and says what the DAG should find in the file.
    patients = sorted({r["patient_id"] for r in rows})
    _write_atomic(marker, json.dumps({
        "sim_day": day,
        "rows": len(rows),
        "patients": len(patients),
        "written_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }) + "\n")

    out_of_range = sum(
        1 for r in rows if not r["reference_low"] <= r["result_value"] <= r["reference_high"]
    )
    missing = len(sim.patients) - len(patients)
    FILES_WRITTEN.inc()
    ROWS_WRITTEN.inc(len(rows))
    LAST_DAY.set(day)
    LAST_FILE_TIME.set(time.time())
    MISSING_PATIENTS.set(missing)
    OUT_OF_RANGE.set(out_of_range)
    log.info("day_written", sim_day=day, path=str(target), rows=len(rows),
             patients=len(patients), missing_patients=missing, out_of_range=out_of_range)
    return target


# ------------------------------------------------------------------ main --

def build_simulator(seed: int | None = None) -> LabSimulator:
    s = get_settings()
    lab = s.lab_simulator
    return LabSimulator(
        epoch=s.sim_clock.epoch,
        day_seconds=s.sim_clock.day_seconds,
        tests=list(lab.tests),
        patient_count=s.patients.count,
        id_prefix=s.patients.id_prefix,
        seed=lab.random_seed if seed is None else seed,
        missing_result_probability=lab.missing_result_probability,
        missing_patient_probability=lab.missing_patient_probability,
        out_of_range_probability=lab.out_of_range_probability,
        far_out_of_range_share=lab.far_out_of_range_share,
        abnormal_patients=tuple(lab.abnormal_patients),
    )


def parse_args(argv=None):
    s = get_settings()
    p = argparse.ArgumentParser(description="Daily lab-results file simulator")
    p.add_argument("--day", type=int, help="write this sim day now and exit")
    p.add_argument("--force", action="store_true", help="rewrite a day that already exists")
    p.add_argument("--output-dir", type=Path, default=Path(s.lab_simulator.landing_dir))
    p.add_argument("--seed", type=int, default=None, help="default: lab_simulator.random_seed")
    return p.parse_args(argv)


def run_forever(sim: LabSimulator, output_dir: Path, now=lambda: datetime.now(timezone.utc),
                sleep=time.sleep, should_stop=lambda: False) -> None:
    """Write each day's file just after that day ends. Catches up the last finished day on start."""
    current = _sim_day(now(), sim.epoch, sim.day_seconds)
    if current < 0:
        log.info("waiting_for_epoch", epoch=sim.epoch.isoformat(), current_sim_day=current)
    next_day = max(current - 1, 0)  # the most recent day that has already ended
    while not should_stop():
        day_end = _day_start(next_day + 1, sim.epoch, sim.day_seconds)
        wait = (day_end - now()).total_seconds()
        if wait > 0:
            sleep(min(wait, 1.0))  # short naps so SIGTERM stops us quickly
            continue
        write_day(sim, output_dir, next_day)
        next_day += 1


def main(argv=None) -> int:
    args = parse_args(argv)
    sim = build_simulator(args.seed)

    if args.day is not None:
        write_day(sim, args.output_dir, args.day, force=args.force)
        return 0

    stopping = False

    def stop(signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    port = get_settings().lab_simulator.metrics_port
    metrics.start_metrics_server(port)
    log.info("started", output_dir=str(args.output_dir), seed=sim.seed, metrics_port=port,
             day_seconds=sim.day_seconds, epoch=sim.epoch.isoformat())
    run_forever(sim, args.output_dir, should_stop=lambda: stopping)
    log.info("stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
