"""Sanity checks on shared configuration files."""
from pathlib import Path

import yaml

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


def test_api_health():
    from fastapi.testclient import TestClient

    from api.main import app

    assert TestClient(app).get("/health").json() == {"status": "ok"}
