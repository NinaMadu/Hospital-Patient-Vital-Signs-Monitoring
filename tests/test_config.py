"""Sanity checks on shared configuration files and the config loader (common/config.py)."""
import copy
import pickle
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from common.config import Section, get_settings, load_settings, load_thresholds

ROOT = Path(__file__).resolve().parents[1]


def test_app_config_loads():
    cfg = yaml.safe_load((ROOT / "config" / "app.yaml").read_text())
    assert cfg["sim_clock"]["day_seconds"] > 0
    assert set(cfg["kafka"]["topics"]) == {"vitals", "dlq", "alerts"}


def test_thresholds_categories_cover_scores():
    t = yaml.safe_load((ROOT / "config" / "thresholds.yaml").read_text())
    cats = t["categories"]
    assert cats["NORMAL"]["min"] == 0
    assert cats["WATCH"]["min"] == cats["NORMAL"]["max"] + 1
    assert cats["CONCERNING"]["min"] == cats["WATCH"]["max"] + 1


# ---- common/config.py -----------------------------------------------------


def test_yaml_defaults_without_env():
    s = load_settings(environ={})
    assert s.sim_clock.day_seconds == 300
    assert s.sim_clock.epoch == datetime(2026, 9, 28, tzinfo=timezone.utc)
    assert s.kafka.bootstrap_servers == "localhost:9094"
    assert s.kafka.topics.vitals == "patient-vitals"
    assert s.postgres.host == "localhost"
    assert s.lab_simulator.tests == ["potassium", "creatinine", "haemoglobin", "wbc", "crp", "lactate"]


def test_env_overrides_yaml_and_is_typed():
    s = load_settings(
        environ={
            "SIM_DAY_SECONDS": "60",
            "SIM_EPOCH": "2026-10-01T06:00:00+05:30",
            "KAFKA_BOOTSTRAP_INTERNAL": "kafka:9092",
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "6543",
        }
    )
    assert s.sim_clock.day_seconds == 60  # int, not "60"
    assert s.sim_clock.epoch.utcoffset().total_seconds() == 5.5 * 3600
    assert s.kafka.bootstrap_servers == "kafka:9092"
    assert s.postgres.port == 6543
    assert s.postgres.jdbc_url == "jdbc:postgresql://postgres:6543/ward"
    assert "host=postgres port=6543 dbname=ward" in s.postgres.dsn


def test_empty_env_value_is_ignored():
    assert load_settings(environ={"SIM_DAY_SECONDS": ""}).sim_clock.day_seconds == 300


def test_naive_epoch_is_treated_as_utc():
    s = load_settings(environ={"SIM_EPOCH": "2026-09-28T00:00:00"})
    assert s.sim_clock.epoch.tzinfo is not None
    assert s.sim_clock.epoch == datetime(2026, 9, 28, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "env",
    [
        {"SIM_DAY_SECONDS": "five"},
        {"SIM_DAY_SECONDS": "0"},
        {"POSTGRES_PORT": "abc"},
        {"SIM_EPOCH": "not-a-date"},
    ],
)
def test_invalid_env_values_fail_loudly(env):
    with pytest.raises(ValueError):
        load_settings(environ=env)


def test_missing_config_dir_fails(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_settings(config_dir=tmp_path, environ={})


def test_settings_are_read_only():
    s = load_settings(environ={})
    with pytest.raises(TypeError):
        s.sim_clock["day_seconds"] = 1
    with pytest.raises(TypeError):
        s.kafka.topics.vitals = "other"


def test_unknown_setting_gives_clear_error():
    s = load_settings(environ={})
    with pytest.raises(AttributeError, match="no setting 'nope'"):
        s.kafka.nope


def test_settings_survive_pickle_and_deepcopy():
    # Spark pickles objects to send them to executors.
    s = load_settings(environ={})
    for clone in (pickle.loads(pickle.dumps(s)), copy.deepcopy(s)):
        assert clone == s
        assert isinstance(clone.kafka.topics, Section)
        assert clone.kafka.topics.vitals == "patient-vitals"


def test_get_settings_is_cached(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setenv("SIM_DAY_SECONDS", "120")
    first = get_settings()
    monkeypatch.setenv("SIM_DAY_SECONDS", "999")
    assert get_settings() is first
    assert first.sim_clock.day_seconds == 120
    get_settings.cache_clear()


def test_load_thresholds():
    t = load_thresholds()
    assert t.vitals.spo2_min.below == 92
    assert t.categories.CONCERNING.min == 4
