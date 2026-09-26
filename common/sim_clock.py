"""Simulated clock: maps real timestamps to simulated days.

Owner: Member A (Day 3).
Contract: sim_day = floor((ts - SIM_EPOCH) / SIM_DAY_SECONDS), read from .env / config/app.yaml.
Used by both simulators, the Spark jobs and the Airflow DAGs so a "day" means the same everywhere.
"""
