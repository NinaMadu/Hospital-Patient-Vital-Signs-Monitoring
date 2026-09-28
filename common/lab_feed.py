"""The daily lab-file contract: names, columns, validation and loading into lab_results.

Owner: Member B.
Shared by the writer (simulators/lab_batch) and the reader (airflow/dags/daily_lab_consolidation),
so both sides agree on what a valid labs_day=N.csv is. Plain Python (csv + psycopg2):
testable without Airflow or Spark.

    labs_day=N.csv            the data, written atomically by the simulator
    labs_day=N.csv._SUCCESS   written last; JSON with the expected row count
"""
from __future__ import annotations

import csv
import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable

COLUMNS = (
    "patient_id", "test_type", "result_value", "unit",
    "reference_low", "reference_high", "collected_at",
)
MAX_REPORTED_ERRORS = 20


def file_path(landing_dir: str | Path, day: int) -> Path:
    return Path(landing_dir) / f"labs_day={day}.csv"


def marker_path(landing_dir: str | Path, day: int) -> Path:
    return Path(landing_dir) / f"labs_day={day}.csv._SUCCESS"


@dataclass
class ValidationResult:
    sim_day: int
    path: str
    rows: list[dict] = field(default_factory=list)     # typed rows, ready to load
    errors: list[str] = field(default_factory=list)    # first MAX_REPORTED_ERRORS problems
    error_count: int = 0
    expected_rows: int | None = None                   # from the _SUCCESS marker

    @property
    def ok(self) -> bool:
        return self.error_count == 0

    @property
    def patients(self) -> int:
        return len({r["patient_id"] for r in self.rows})

    def error(self, message: str) -> None:
        self.error_count += 1
        if len(self.errors) < MAX_REPORTED_ERRORS:
            self.errors.append(message)

    def summary(self) -> dict:
        return {"sim_day": self.sim_day, "path": self.path, "rows": len(self.rows),
                "patients": self.patients, "expected_rows": self.expected_rows,
                "error_count": self.error_count, "errors": self.errors}


def _number(value: str) -> float:
    x = float(value)
    if not math.isfinite(x):
        raise ValueError(f"not a finite number: {value!r}")
    return x


def validate_file(
    landing_dir: str | Path,
    day: int,
    known_tests: Iterable[str],
    patient_id_prefix: str,
    day_start: datetime,
    day_end: datetime,
) -> ValidationResult:
    """Check labs_day=N.csv against the contract and return typed rows plus every problem found.

    The whole file is judged: one bad row fails the file (a lab feed is all or nothing).
    """
    path = file_path(landing_dir, day)
    result = ValidationResult(sim_day=day, path=str(path))
    known_tests = set(known_tests)
    pid_pattern = re.compile(rf"^{re.escape(patient_id_prefix)}\d{{3}}$")

    marker = marker_path(landing_dir, day)
    try:
        result.expected_rows = int(json.loads(marker.read_text(encoding="utf-8"))["rows"])
    except FileNotFoundError:
        result.error(f"marker missing: {marker.name}")
    except (ValueError, KeyError, TypeError) as exc:
        result.error(f"marker unreadable: {exc}")

    if not path.is_file():
        result.error(f"file missing: {path.name}")
        return result

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if tuple(reader.fieldnames or ()) != COLUMNS:
            result.error(f"columns {reader.fieldnames} != expected {list(COLUMNS)}")
            return result

        seen: set[tuple] = set()
        for line_no, raw in enumerate(reader, start=2):  # line 1 is the header
            where = f"line {line_no}"
            try:
                row = {
                    "patient_id": raw["patient_id"].strip(),
                    "test_type": raw["test_type"].strip(),
                    "result_value": _number(raw["result_value"]),
                    "unit": raw["unit"].strip(),
                    "reference_low": _number(raw["reference_low"]),
                    "reference_high": _number(raw["reference_high"]),
                    "collected_at": datetime.fromisoformat(raw["collected_at"]),
                }
            except (TypeError, ValueError, AttributeError) as exc:
                result.error(f"{where}: bad value ({exc})")
                continue

            problems = []
            if not pid_pattern.match(row["patient_id"]):
                problems.append(f"patient_id {row['patient_id']!r}")
            if row["test_type"] not in known_tests:
                problems.append(f"unknown test_type {row['test_type']!r}")
            if not row["unit"]:
                problems.append("empty unit")
            if row["result_value"] < 0:
                problems.append(f"negative result {row['result_value']}")
            if row["reference_low"] > row["reference_high"]:
                problems.append("reference_low > reference_high")
            ts = row["collected_at"]
            if ts.tzinfo is None:
                problems.append("collected_at has no timezone")
            elif not day_start <= ts < day_end:
                problems.append(f"collected_at {ts.isoformat()} outside sim day {day}")
            key = (row["patient_id"], row["test_type"], ts)
            if key in seen:
                problems.append("duplicate (patient_id, test_type, collected_at)")
            seen.add(key)

            if problems:
                result.error(f"{where}: " + "; ".join(problems))
            else:
                result.rows.append(row)

        data_lines = reader.line_num - 1

    # Count check against the marker: catches a truncated or partly copied file.
    if result.expected_rows is not None and data_lines != result.expected_rows:
        result.error(f"file has {data_lines} data lines, marker says {result.expected_rows}")
    return result


# ----------------------------------------------------------------- loading --

DELETE_DAY_SQL = "DELETE FROM lab_results WHERE sim_day = %s"
INSERT_SQL = """
INSERT INTO lab_results (sim_day, patient_id, test_type, result_value, unit,
                         reference_low, reference_high, collected_at, source_file)
VALUES %s
"""


def load_rows(conn, day: int, rows: list[dict], source_file: str) -> int:
    """Replace sim day `day` in lab_results with `rows`, in one transaction.

    DELETE + INSERT inside one transaction makes a rerun idempotent: the day ends up with
    exactly this file's rows, and a failure half-way rolls back to the previous state.
    """
    from psycopg2.extras import execute_values

    values = [(day, r["patient_id"], r["test_type"], r["result_value"], r["unit"],
               r["reference_low"], r["reference_high"], r["collected_at"], source_file)
              for r in rows]
    with conn:                        # commit on success, rollback on any exception
        with conn.cursor() as cur:
            cur.execute(DELETE_DAY_SQL, (day,))
            if values:
                execute_values(cur, INSERT_SQL, values)
    return len(values)


def record_health(conn, check_name: str, status: str, value: float | None = None,
                  details: dict | None = None, component: str = "daily_lab_consolidation") -> None:
    """One row in pipeline_health (read by the API and Kasun's Prometheus alerts)."""
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pipeline_health (component, check_name, status, value, details) "
                "VALUES (%s, %s, %s, %s, %s)",
                (component, check_name, status, value, json.dumps(details or {}, default=str)),
            )
