"""Renders the daily consolidated patient risk report (HTML/CSV) into /data/reports.

Owner: Member B (B9).

    daily_patient_risk (day N, from B8) + previous day's totals + today's alert counts
        -> risk_report_day=N.csv   (one row per patient, for spreadsheets)
        -> risk_report_day=N.html  (self-contained page for the ward team)

    python -m reports.generate_report --sim-day 77                     # inside a container
    python -m reports.generate_report --sim-day 77 --output-dir data/reports

For each patient the report shows the category from vitals alone next to the combined
category, so it is visible where yesterday's lab results changed the risk picture (the
second half of the business question). Files are written atomically; a rerun replaces them.
"""
from __future__ import annotations

import argparse
import csv
import html
import io
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from common import sim_clock
from common.config import get_settings
from common.logger import get_logger
from common.risk_rules import LAB_UNAVAILABLE, category

log = get_logger("risk-report")

SEVERITY = {"CONCERNING": 0, "WATCH": 1, "NORMAL": 2}
CSV_COLUMNS = [
    "sim_day", "patient_id", "risk_category", "total_risk_score", "vital_risk_score",
    "vital_only_category", "lab_risk_score", "lab_sim_day", "lab_status",
    "labs_changed_category", "change_vs_previous_day", "abnormal_labs", "vital_reasons",
    "alerts_today",
]


# -------------------------------------------------------------------- data --

def fetch(conn, day: int) -> list[dict]:
    """Day N's risk rows, enriched with yesterday's total and today's alert count."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT r.patient_id, r.vital_risk_score, r.vital_reasons, r.lab_risk_score,
                      r.lab_sim_day, r.total_risk_score, r.risk_category, r.lab_status,
                      r.abnormal_labs, p.total_risk_score AS previous_total
               FROM daily_patient_risk r
               LEFT JOIN daily_patient_risk p
                      ON p.patient_id = r.patient_id AND p.sim_day = r.sim_day - 1
               WHERE r.sim_day = %s""", (day,))
        names = [d[0] for d in cur.description]
        rows = [dict(zip(names, r)) for r in cur.fetchall()]
    alerts = _alert_counts(conn, day)
    return [enrich(day, r, alerts.get(r["patient_id"], 0)) for r in rows]


def _alert_counts(conn, day: int) -> dict[str, int]:
    """Speed-layer alerts per patient for the day; the report still renders without them."""
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT patient_id, count(*) FROM vital_alerts WHERE sim_day = %s "
                        "GROUP BY patient_id", (day,))
            return dict(cur.fetchall())
    except Exception:  # noqa: BLE001 - alerts are context, not required
        log.warning("alerts_unavailable", sim_day=day)
        return {}


def enrich(day: int, r: dict, alerts_today: int) -> dict:
    """Add the columns that explain the score: vitals-only category, change vs yesterday."""
    vital_only = category(r["vital_risk_score"] or 0)
    previous = r.get("previous_total")
    return {
        **r,
        "sim_day": day,
        "vital_only_category": vital_only,
        "labs_changed_category": r["risk_category"] != vital_only,
        "change_vs_previous_day": None if previous is None else r["total_risk_score"] - previous,
        "alerts_today": alerts_today,
    }


def sort_rows(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=lambda r: (SEVERITY.get(r["risk_category"], 9),
                                       -r["total_risk_score"], r["patient_id"]))


# ----------------------------------------------------------------- render --

def render_csv(rows: list[dict]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


def _cell(value) -> str:
    return "" if value is None else html.escape(str(value))


def _delta(value) -> str:
    if value is None:
        return '<span class="muted">new</span>'
    if value == 0:
        return '<span class="muted">±0</span>'
    return f'<span class="{"up" if value > 0 else "down"}">{value:+d}</span>'


def render_html(day: int, rows: list[dict], generated_at: datetime) -> str:
    counts = {c: sum(1 for r in rows if r["risk_category"] == c) for c in SEVERITY}
    unavailable = [r["patient_id"] for r in rows if r["lab_status"] == LAB_UNAVAILABLE]
    changed = [r for r in rows if r["labs_changed_category"]]
    start, end = sim_clock.day_start(day), sim_clock.day_end(day)

    body_rows = "\n".join(
        f"""<tr>
  <td class="pid">{_cell(r['patient_id'])}</td>
  <td><span class="badge {r['risk_category'].lower()}">{_cell(r['risk_category'])}</span></td>
  <td class="num">{_cell(r['total_risk_score'])}</td>
  <td class="num">{_delta(r['change_vs_previous_day'])}</td>
  <td class="num">{_cell(r['vital_risk_score'])}</td>
  <td>{_cell(r['vital_reasons'])}</td>
  <td class="num">{_cell(r['lab_risk_score']) if r['lab_status'] != LAB_UNAVAILABLE else '<span class="muted">n/a</span>'}</td>
  <td>{_cell(r['abnormal_labs']) if r['lab_status'] != LAB_UNAVAILABLE else '<span class="warn">LAB_UNAVAILABLE</span>'}</td>
  <td class="num">{_cell(r['lab_sim_day'])}</td>
  <td>{'<span class="changed">' + _cell(r['vital_only_category']) + ' &rarr; ' + _cell(r['risk_category']) + '</span>' if r['labs_changed_category'] else ''}</td>
  <td class="num">{_cell(r['alerts_today'])}</td>
</tr>""" for r in rows)

    changed_text = (", ".join(f"<b>{_cell(r['patient_id'])}</b> ({_cell(r['vital_only_category'])} &rarr; "
                              f"{_cell(r['risk_category'])})" for r in changed)
                    or "none today")
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Ward risk report – sim day {day}</title>
<style>
  :root {{ --fg:#1d2433; --muted:#6b7280; --line:#e5e7eb; --bg:#ffffff; --panel:#f8fafc;
          --red:#b42318; --red-bg:#fee4e2; --amber:#b54708; --amber-bg:#fef0c7;
          --green:#067647; --green-bg:#dcfae6; }}
  body {{ font:14px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif; color:var(--fg);
         background:var(--bg); margin:0; padding:24px; }}
  h1 {{ font-size:22px; margin:0 0 4px; }}
  .sub {{ color:var(--muted); margin:0 0 20px; }}
  .question {{ background:var(--panel); border-left:4px solid var(--fg); padding:10px 14px; margin:0 0 20px; }}
  .cards {{ display:flex; gap:12px; flex-wrap:wrap; margin:0 0 20px; }}
  .card {{ border:1px solid var(--line); border-radius:8px; padding:10px 16px; min-width:120px; }}
  .card b {{ display:block; font-size:24px; }}
  .card.concerning b {{ color:var(--red); }} .card.watch b {{ color:var(--amber); }}
  .card.normal b {{ color:var(--green); }}
  .notes p {{ margin:4px 0; }}
  .scroll {{ overflow-x:auto; }}
  table {{ border-collapse:collapse; width:100%; margin-top:16px; }}
  th, td {{ border-bottom:1px solid var(--line); padding:6px 8px; text-align:left; vertical-align:top; }}
  th {{ font-size:12px; text-transform:uppercase; letter-spacing:.03em; color:var(--muted); }}
  td.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
  td.pid {{ font-weight:600; }}
  .badge {{ padding:1px 8px; border-radius:10px; font-size:12px; font-weight:600; }}
  .badge.concerning {{ color:var(--red); background:var(--red-bg); }}
  .badge.watch {{ color:var(--amber); background:var(--amber-bg); }}
  .badge.normal {{ color:var(--green); background:var(--green-bg); }}
  .up {{ color:var(--red); font-weight:600; }} .down {{ color:var(--green); font-weight:600; }}
  .muted {{ color:var(--muted); }} .warn {{ color:var(--amber); font-weight:600; }}
  .changed {{ color:var(--red); }}
  footer {{ color:var(--muted); font-size:12px; margin-top:24px; }}
</style></head>
<body>
<h1>Ward risk report · sim day {day}</h1>
<p class="sub">{start:%Y-%m-%d %H:%M:%S} – {end:%H:%M:%S} UTC · generated {generated_at:%Y-%m-%d %H:%M:%S} UTC</p>
<p class="question"><b>Which patients show concerning vital-sign trends, and how do the latest lab results
change the risk picture?</b> Total score = vital points (daily summary) + lab points (newest labs,
at most one day old). 0–1 NORMAL · 2–3 WATCH · 4+ CONCERNING.</p>
<div class="cards">
  <div class="card concerning"><b>{counts['CONCERNING']}</b>concerning</div>
  <div class="card watch"><b>{counts['WATCH']}</b>watch</div>
  <div class="card normal"><b>{counts['NORMAL']}</b>normal</div>
  <div class="card"><b>{len(unavailable)}</b>labs unavailable</div>
</div>
<div class="notes">
  <p><b>Labs changed the category for:</b> {changed_text}</p>
  <p><b>No recent labs (LAB_UNAVAILABLE, scored on vitals only):</b> {", ".join(unavailable) or "none"}</p>
</div>
<div class="scroll"><table>
<thead><tr><th>Patient</th><th>Category</th><th>Total</th><th>vs yesterday</th><th>Vital pts</th>
<th>Vital reasons</th><th>Lab pts</th><th>Abnormal labs</th><th>Lab day</th><th>Labs changed</th><th>Alerts</th></tr></thead>
<tbody>
{body_rows}
</tbody></table></div>
<footer>Project-defined rules for an academic data-engineering exercise (EC8203) — not clinical guidance.
Source: daily_patient_risk, vital_alerts. Ward Vitals pipeline, batch layer.</footer>
</body></html>
"""


# ------------------------------------------------------------------ write --

def report_paths(output_dir: str | Path, day: int) -> tuple[Path, Path]:
    out = Path(output_dir)
    return out / f"risk_report_day={day}.html", out / f"risk_report_day={day}.csv"


def _write_atomic(path: Path, text: str) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8", newline="")
    os.replace(tmp, path)


def generate(day: int, output_dir: str | Path | None = None, conn=None) -> dict:
    """Build both files for sim day `day`. Raises if B8 has not written that day yet."""
    output_dir = Path(output_dir or get_settings().paths.reports)
    own_conn = conn is None
    if own_conn:
        import psycopg2

        conn = psycopg2.connect(get_settings().postgres.dsn)
    try:
        rows = sort_rows(fetch(conn, day))
    finally:
        if own_conn:
            conn.close()
    if not rows:
        raise LookupError(f"no daily_patient_risk rows for sim day {day}; run risk_consolidation first")

    output_dir.mkdir(parents=True, exist_ok=True)
    html_path, csv_path = report_paths(output_dir, day)
    _write_atomic(csv_path, render_csv(rows))
    _write_atomic(html_path, render_html(day, rows, datetime.now(timezone.utc)))
    summary = {
        "sim_day": day, "patients": len(rows), "html": str(html_path), "csv": str(csv_path),
        "concerning": sum(1 for r in rows if r["risk_category"] == "CONCERNING"),
        "labs_changed": sum(1 for r in rows if r["labs_changed_category"]),
        "lab_unavailable": sum(1 for r in rows if r["lab_status"] == LAB_UNAVAILABLE),
    }
    log.info("report_written", **summary)
    return summary


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Daily consolidated patient risk report (HTML + CSV)")
    p.add_argument("--sim-day", type=int, default=None, help="default: the last completed day")
    p.add_argument("--output-dir", default=None, help="default: paths.reports in app.yaml")
    args = p.parse_args(argv)
    day = args.sim_day if args.sim_day is not None else sim_clock.current_sim_day() - 1
    generate(day, args.output_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
