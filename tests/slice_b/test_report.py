"""Daily risk report (reports/generate_report.py): content, escaping, files, reruns."""
import csv

import pytest

from reports import generate_report as rep

DAY = 77


def risk_row(pid, vital, lab, total, cat, status="AVAILABLE", labs=None, reasons=None,
             lab_day=DAY, previous=None):
    return (pid, vital, reasons, lab, None if lab is None else lab_day, total, cat, status, labs, previous)


ROWS = [
    risk_row("P001", 1, 3, 4, "CONCERNING", labs="haemoglobin:LOW,potassium:HIGH",
             reasons="systolic_bp_high", lab_day=DAY - 1, previous=1),
    risk_row("P002", 0, 0, 0, "NORMAL", previous=2),
    risk_row("P003", 2, None, 2, "WATCH", status="LAB_UNAVAILABLE", reasons="spo2_low"),
    risk_row("P004", 3, 2, 5, "CONCERNING", labs="crp:HIGH", reasons="<script>x</script>", previous=5),
]
RISK_NAMES = ["patient_id", "vital_risk_score", "vital_reasons", "lab_risk_score", "lab_sim_day",
              "total_risk_score", "risk_category", "lab_status", "abnormal_labs", "previous_total"]


class FakeConn:
    def __init__(self, risk_rows, alerts=None, alerts_fail=False):
        self.risk_rows, self.alerts, self.alerts_fail = risk_rows, alerts or [], alerts_fail

    def cursor(self):
        conn = self

        class Cur:
            description = None

            def execute(self, sql, params):
                if "vital_alerts" in sql:
                    if conn.alerts_fail:
                        raise RuntimeError("relation vital_alerts does not exist")
                    self.rows = conn.alerts
                else:
                    self.description = [(n,) for n in RISK_NAMES]
                    self.rows = conn.risk_rows

            def fetchall(self):
                return self.rows

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        return Cur()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


@pytest.fixture
def summary(tmp_path):
    return rep.generate(DAY, tmp_path, conn=FakeConn(ROWS, alerts=[("P001", 3)])), tmp_path


def test_files_written_with_summary(summary):
    s, out = summary
    html_path, csv_path = rep.report_paths(out, DAY)
    assert s["html"] == str(html_path) and html_path.exists() and csv_path.exists()
    assert (s["patients"], s["concerning"], s["lab_unavailable"]) == (4, 2, 1)
    assert s["labs_changed"] == 2          # P001 NORMAL -> CONCERNING, P004 WATCH -> CONCERNING
    assert {p.name for p in out.iterdir()} == {html_path.name, csv_path.name}  # no temp files


def test_csv_sorted_by_severity_with_explaining_columns(summary):
    _, out = summary
    rows = list(csv.DictReader(open(rep.report_paths(out, DAY)[1], encoding="utf-8")))
    assert [r["patient_id"] for r in rows] == ["P004", "P001", "P003", "P002"]
    p1 = next(r for r in rows if r["patient_id"] == "P001")
    assert p1["vital_only_category"] == "NORMAL" and p1["labs_changed_category"] == "True"
    assert p1["change_vs_previous_day"] == "3" and p1["alerts_today"] == "3" and p1["lab_sim_day"] == "76"
    p3 = next(r for r in rows if r["patient_id"] == "P003")
    assert p3["lab_status"] == "LAB_UNAVAILABLE" and p3["lab_risk_score"] == ""
    assert p3["change_vs_previous_day"] == ""   # no row yesterday


def test_html_content_and_escaping(summary):
    _, out = summary
    page = rep.report_paths(out, DAY)[0].read_text(encoding="utf-8")
    assert f"sim day {DAY}" in page
    assert "LAB_UNAVAILABLE" in page and "P003" in page
    assert "NORMAL &rarr; CONCERNING" in page
    assert "<script>" not in page and "&lt;script&gt;" in page
    assert "not clinical guidance" in page


def test_rerun_replaces_files(tmp_path):
    rep.generate(DAY, tmp_path, conn=FakeConn(ROWS))
    rep.generate(DAY, tmp_path, conn=FakeConn(ROWS[:1]))
    rows = list(csv.DictReader(open(rep.report_paths(tmp_path, DAY)[1], encoding="utf-8")))
    assert [r["patient_id"] for r in rows] == ["P001"]


def test_missing_alerts_table_still_renders(tmp_path):
    s = rep.generate(DAY, tmp_path, conn=FakeConn(ROWS, alerts_fail=True))
    assert s["patients"] == 4


def test_no_risk_rows_is_an_error(tmp_path):
    with pytest.raises(LookupError):
        rep.generate(DAY, tmp_path, conn=FakeConn([]))
    assert list(tmp_path.iterdir()) == []
