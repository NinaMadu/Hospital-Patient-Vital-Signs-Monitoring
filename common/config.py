"""Configuration loader: config/app.yaml overlaid with environment variables.

Owner: Member B.

Every component (simulators, Spark jobs, Airflow DAGs, API) reads settings through
`get_settings()`, so a value such as the length of a simulated day is defined once.

Precedence (highest wins):
    1. Environment variables listed in ENV_OVERRIDES (set from .env inside the containers)
    2. config/app.yaml
The host has no .env loaded, so local runs and unit tests use the YAML defaults
(e.g. Kafka on localhost:9094), while containers get kafka:9092 from .env.

Usage:
    from common.config import get_settings
    s = get_settings()
    s.sim_clock.day_seconds        # 300
    s.kafka.topics.vitals          # "patient-vitals"
    s.postgres.jdbc_url            # "jdbc:postgresql://postgres:5432/ward"
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Mapping

import yaml

# Repo root is two levels up from this file (common/config.py). Inside the containers the
# repo is mounted at /opt/project, so this resolves to /opt/project/config there.
DEFAULT_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"


def _parse_epoch(value: Any) -> datetime:
    """ISO-8601 string (or datetime) -> timezone-aware datetime. Naive values are taken as UTC."""
    dt = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# Environment variable -> (path inside the settings tree, type to convert to).
ENV_OVERRIDES: dict[str, tuple[tuple[str, ...], Callable[[Any], Any]]] = {
    "SIM_DAY_SECONDS": (("sim_clock", "day_seconds"), int),
    "SIM_EPOCH": (("sim_clock", "epoch"), str),
    "KAFKA_BOOTSTRAP_INTERNAL": (("kafka", "bootstrap_servers"), str),
    "VITALS_TOPIC": (("kafka", "topics", "vitals"), str),
    "DLQ_TOPIC": (("kafka", "topics", "dlq"), str),
    "ALERTS_TOPIC": (("kafka", "topics", "alerts"), str),
    "POSTGRES_HOST": (("postgres", "host"), str),
    "POSTGRES_PORT": (("postgres", "port"), int),
    "POSTGRES_DB": (("postgres", "db"), str),
    "POSTGRES_USER": (("postgres", "user"), str),
    "POSTGRES_PASSWORD": (("postgres", "password"), str),
}


class Section(dict):
    """Read-only dict that also allows attribute access: s.kafka.topics.vitals."""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(f"no setting '{name}' (available: {sorted(self)})") from None

    def _read_only(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError("settings are read-only; change config/app.yaml or .env instead")

    __setattr__ = __setitem__ = __delitem__ = _read_only
    update = pop = popitem = clear = setdefault = _read_only

    def __reduce__(self):
        # Rebuild through the constructor so pickle (used by Spark to ship objects to
        # executors) and copy.deepcopy never call the blocked __setitem__.
        return (Section, (dict(self),))


def _freeze(value: Any) -> Any:
    """Recursively turn nested dicts into Sections."""
    if isinstance(value, Mapping):
        return Section({k: _freeze(v) for k, v in value.items()})
    return value


def _set_path(tree: dict, path: tuple[str, ...], value: Any) -> None:
    for key in path[:-1]:
        tree = tree.setdefault(key, {})
    tree[path[-1]] = value


def _read_yaml(path: Path) -> dict:
    if not path.is_file():
        raise FileNotFoundError(f"config file not found: {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def load_settings(
    config_dir: str | Path | None = None, environ: Mapping[str, str] | None = None
) -> Section:
    """Build the settings tree. Uncached; tests call this with their own environ."""
    config_dir = Path(config_dir or os.environ.get("WARD_CONFIG_DIR") or DEFAULT_CONFIG_DIR)
    environ = os.environ if environ is None else environ

    tree = _read_yaml(config_dir / "app.yaml")

    for var, (path, cast) in ENV_OVERRIDES.items():
        raw = environ.get(var)
        if raw is None or raw == "":
            continue
        try:
            _set_path(tree, path, cast(raw))
        except ValueError as exc:
            raise ValueError(f"environment variable {var}={raw!r} is invalid: {exc}") from None

    # Derived values, computed once here so no component rebuilds them differently.
    clock = tree["sim_clock"]
    clock["epoch"] = _parse_epoch(clock["epoch"])
    if clock["day_seconds"] <= 0:
        raise ValueError("sim_clock.day_seconds must be positive")

    pg = tree["postgres"]
    pg["dsn"] = (
        f"host={pg['host']} port={pg['port']} dbname={pg['db']} "
        f"user={pg['user']} password={pg['password']}"
    )
    pg["jdbc_url"] = f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['db']}"

    return _freeze(tree)


@lru_cache(maxsize=1)
def get_settings() -> Section:
    """Settings for this process, loaded once. Call get_settings.cache_clear() to reload."""
    return load_settings()


def load_thresholds(config_dir: str | Path | None = None) -> Section:
    """Risk-rule thresholds from config/thresholds.yaml (used by common/risk_rules.py)."""
    config_dir = Path(config_dir or os.environ.get("WARD_CONFIG_DIR") or DEFAULT_CONFIG_DIR)
    return _freeze(_read_yaml(config_dir / "thresholds.yaml"))
