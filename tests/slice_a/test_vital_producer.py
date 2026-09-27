"""Vitals simulator output: fields, determinism, scenarios and malformed events (no Kafka)."""
import json
from datetime import datetime, timezone

from simulators.vital_producer.main import FIELDS, VitalSimulator

T0 = datetime(2026, 9, 28, tzinfo=timezone.utc)


def test_reading_has_all_fields_in_range():
    sim = VitalSimulator(seed=1, spike_probability=0.0)
    for pid in sim.patients:
        r = sim.reading(pid, elapsed_s=0, now=T0)
        assert tuple(r) == FIELDS
        assert 40 <= r["heart_rate"] <= 120
        assert 90 <= r["spo2"] <= 100
        assert 35 <= r["temperature"] <= 38


def test_fifteen_patients_with_fixed_beds():
    sim = VitalSimulator()
    assert sim.patients[0] == "P001" and sim.patients[-1] == "P015"
    assert sim.beds["P007"] == "BED-07"


def test_same_seed_same_sequence():
    def run(seed):
        sim = VitalSimulator(seed=seed, malformed_rate=0.2)
        return [sim.maybe_corrupt(sim.reading(p, 0, now=T0)) for p in sim.patients * 4]

    assert run(42) == run(42)
    assert run(42) != run(43)


def test_spike_scenario_only_in_its_window():
    sim = VitalSimulator(scenario="spike", scenario_patient="P007",
                         scenario_start_s=60, scenario_duration_s=180, spike_probability=0.0)
    before = sim.reading("P007", elapsed_s=30, now=T0)
    during = sim.reading("P007", elapsed_s=100, now=T0)
    other = sim.reading("P003", elapsed_s=100, now=T0)
    after = sim.reading("P007", elapsed_s=300, now=T0)
    assert during["heart_rate"] >= 135 and during["spo2"] < 90
    for r in (before, other, after):
        assert r["heart_rate"] < 120 and r["spo2"] > 92


def test_malformed_events_are_actually_invalid():
    sim = VitalSimulator(malformed_rate=1.0)
    for _ in range(50):
        payload = sim.maybe_corrupt(sim.reading("P001", 0, now=T0))
        try:
            event = json.loads(payload)
        except json.JSONDecodeError:
            continue
        hr, spo2 = event.get("heart_rate"), event.get("spo2")
        assert hr is None or spo2 is None or not isinstance(hr, int) or spo2 > 100


def test_stream_paces_each_patient_2_to_5_seconds():
    sim = VitalSimulator(patient_count=3)
    fake_now = [0.0]
    seen: dict[str, list[float]] = {}
    for pid, _ in sim.stream(max_events=30, clock=lambda: fake_now[0],
                             sleep=lambda s: fake_now.__setitem__(0, fake_now[0] + s)):
        seen.setdefault(pid, []).append(fake_now[0])
    assert set(seen) == {"P001", "P002", "P003"}
    for times in seen.values():
        gaps = [b - a for a, b in zip(times, times[1:])]
        assert all(2.0 <= g <= 5.0 for g in gaps)
