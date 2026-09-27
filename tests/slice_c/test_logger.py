"""common/logger.py: one JSON object per line with the agreed fields."""
import json
from datetime import datetime

from common.logger import get_logger


def lines(capsys) -> list[dict]:
    return [json.loads(line) for line in capsys.readouterr().out.splitlines()]


def test_contract_fields_and_extra_identifiers(capsys):
    get_logger("test-contract").info("reading_scored", patient_id="P007", sim_day=3)
    [rec] = lines(capsys)
    assert rec["component"] == "test-contract"
    assert rec["event"] == "reading_scored"
    assert rec["severity"] == "INFO"
    assert rec["patient_id"] == "P007" and rec["sim_day"] == 3
    assert datetime.fromisoformat(rec["timestamp"]).tzinfo is not None


def test_severities(capsys):
    log = get_logger("test-severity")
    log.warning("w")
    log.error("e")
    log.critical("c")
    assert [r["severity"] for r in lines(capsys)] == ["WARN", "ERROR", "CRITICAL"]


def test_debug_is_hidden_by_default(capsys):
    get_logger("test-debug").debug("noise")
    assert capsys.readouterr().out == ""


def test_bound_context_is_added_to_every_line(capsys):
    log = get_logger("test-bind", run="r1").bind(sim_day=5)
    log.info("a")
    log.info("b", patient_id="P001")
    first, second = lines(capsys)
    assert first["run"] == "r1" and first["sim_day"] == 5
    assert second["sim_day"] == 5 and second["patient_id"] == "P001"


def test_exception_adds_error_and_traceback(capsys):
    log = get_logger("test-exc")
    try:
        raise ValueError("boom")
    except ValueError:
        log.exception("batch_failed", batch_id=7)
    [rec] = lines(capsys)
    assert rec["severity"] == "ERROR" and rec["batch_id"] == 7
    assert rec["error"] == "ValueError: boom"
    assert "Traceback" in rec["traceback"]


def test_fields_cannot_overwrite_contract_keys(capsys):
    get_logger("test-reserved").info("real_event", severity="fake", component="fake")
    [rec] = lines(capsys)
    assert rec["severity"] == "INFO" and rec["component"] == "test-reserved"
    assert rec["field_severity"] == "fake"


def test_non_json_values_are_stringified(capsys):
    get_logger("test-types").info("t", at=datetime(2026, 9, 28))
    [rec] = lines(capsys)
    assert rec["at"].startswith("2026-09-28")


def test_same_component_twice_prints_once(capsys):
    get_logger("test-once")
    get_logger("test-once").info("only_once")
    assert len(lines(capsys)) == 1
