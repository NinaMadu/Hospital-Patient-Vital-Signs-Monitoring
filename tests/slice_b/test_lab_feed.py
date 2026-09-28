"""Lab-file contract (common/lab_feed.py): validation cases and idempotent loading."""
from datetime import datetime, timedelta, timezone

import pytest

from common import lab_feed
from simulators.lab_batch.main import LAB_CATALOGUE, LabSimulator, write_day

EPOCH = datetime(2026, 9, 28, tzinfo=timezone.utc)
DAY_S = 300
DAY = 3


def day_bounds(day=DAY):
    start = EPOCH + timedelta(seconds=day * DAY_S)
    return start, start + timedelta(seconds=DAY_S)


def validate(landing, day=DAY):
    start, end = day_bounds(day)
    return lab_feed.validate_file(landing, day, known_tests=LAB_CATALOGUE, patient_id_prefix="P",
                                  day_start=start, day_end=end)


@pytest.fixture
def landing(tmp_path):
    sim = LabSimulator(epoch=EPOCH, day_seconds=DAY_S, tests=list(LAB_CATALOGUE))
    write_day(sim, tmp_path, DAY)
    return tmp_path


def rewrite(landing, transform, day=DAY):
    """Edit the CSV lines (header is line 0) while keeping the marker unchanged."""
    path = lab_feed.file_path(landing, day)
    lines = path.read_text().splitlines()
    path.write_text("\n".join(transform(lines)) + "\n")


def replace_field(line, index, value):
    parts = line.split(",")
    parts[index] = value
    return ",".join(parts)


# ---- valid file -------------------------------------------------------------


def test_simulator_file_passes_validation(landing):
    r = validate(landing)
    assert r.ok, r.errors
    assert len(r.rows) == r.expected_rows > 0
    assert isinstance(r.rows[0]["result_value"], float)
    assert r.rows[0]["collected_at"].tzinfo is not None
    assert r.summary()["patients"] == r.patients


def test_simulator_and_feed_share_the_contract():
    from simulators.lab_batch import main as sim

    assert sim.COLUMNS is lab_feed.COLUMNS
    assert sim.file_path is lab_feed.file_path and sim.marker_path is lab_feed.marker_path


# ---- invalid files: each must fail with a clear message ---------------------


@pytest.mark.parametrize(
    "transform, message",
    [
        (lambda ls: [ls[0].replace("unit", "units")] + ls[1:], "columns"),
        (lambda ls: [ls[0], replace_field(ls[1], 2, "abc")] + ls[2:], "bad value"),
        (lambda ls: [ls[0], replace_field(ls[1], 2, "nan")] + ls[2:], "bad value"),
        (lambda ls: [ls[0], replace_field(ls[1], 2, "-1.0")] + ls[2:], "negative result"),
        (lambda ls: [ls[0], replace_field(ls[1], 1, "glucose")] + ls[2:], "unknown test_type"),
        (lambda ls: [ls[0], replace_field(ls[1], 0, "X9")] + ls[2:], "patient_id"),
        (lambda ls: [ls[0], replace_field(ls[1], 3, "")] + ls[2:], "empty unit"),
        (lambda ls: [ls[0], replace_field(ls[1], 4, "99")] + ls[2:], "reference_low > reference_high"),
        (lambda ls: [ls[0], replace_field(ls[1], 6, "2026-09-28T00:00:10")] + ls[2:], "no timezone"),
        (lambda ls: [ls[0], replace_field(ls[1], 6, "2026-09-27T00:00:10+00:00")] + ls[2:], "outside sim day"),
        (lambda ls: ls[:2] + [ls[1]] + ls[3:], "duplicate"),
        (lambda ls: ls[:-3], "marker says"),                      # truncated file
    ],
)
def test_invalid_files_are_rejected(landing, transform, message):
    rewrite(landing, transform)
    r = validate(landing)
    assert not r.ok
    assert any(message in e for e in r.errors), r.errors


def test_missing_file_and_marker(tmp_path):
    r = validate(tmp_path)
    assert not r.ok
    assert any("marker missing" in e for e in r.errors)
    assert any("file missing" in e for e in r.errors)


def test_unreadable_marker(landing):
    lab_feed.marker_path(landing, DAY).write_text("not json")
    r = validate(landing)
    assert any("marker unreadable" in e for e in r.errors)


def test_error_list_is_capped_but_counted(landing):
    rewrite(landing, lambda ls: [ls[0]] + [replace_field(line, 1, "glucose") for line in ls[1:]])
    r = validate(landing)
    assert r.error_count == r.expected_rows
    assert len(r.errors) == lab_feed.MAX_REPORTED_ERRORS


# ---- loading ----------------------------------------------------------------


class FakeCursor:
    def __init__(self, log):
        self.log = log

    def execute(self, sql, params=None):
        self.log.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConn:
    """Records SQL and whether the transaction committed or rolled back."""

    def __init__(self):
        self.log, self.outcome = [], None

    def cursor(self):
        return FakeCursor(self.log)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *exc):
        self.outcome = "rollback" if exc_type else "commit"
        return False


def test_load_deletes_the_day_then_inserts_in_one_transaction(landing, monkeypatch):
    inserted = []
    monkeypatch.setattr("psycopg2.extras.execute_values",
                        lambda cur, sql, values: inserted.extend(values))
    rows = validate(landing).rows
    conn = FakeConn()

    assert lab_feed.load_rows(conn, DAY, rows, source_file="labs_day=3.csv") == len(rows)
    assert conn.log[0] == (lab_feed.DELETE_DAY_SQL, (DAY,))
    assert len(inserted) == len(rows) and inserted[0][0] == DAY and inserted[0][-1] == "labs_day=3.csv"
    assert conn.outcome == "commit"


def test_failed_insert_rolls_back(landing, monkeypatch):
    def boom(*a):
        raise RuntimeError("db down")

    monkeypatch.setattr("psycopg2.extras.execute_values", boom)
    conn = FakeConn()
    with pytest.raises(RuntimeError):
        lab_feed.load_rows(conn, DAY, validate(landing).rows, source_file="x")
    assert conn.outcome == "rollback"   # the DELETE is undone too: the old rows stay


def test_empty_day_still_clears_old_rows():
    conn = FakeConn()
    assert lab_feed.load_rows(conn, DAY, [], source_file="x") == 0
    assert conn.log == [(lab_feed.DELETE_DAY_SQL, (DAY,))]
