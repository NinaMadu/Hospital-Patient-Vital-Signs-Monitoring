"""Vitals simulator output: fields, determinism, scenarios and malformed events (no Kafka)."""
import json
from datetime import datetime, timezone

from common.config import load_thresholds
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


def without_event_id(payload: str) -> str:
    """event_ids are fresh every run; compare everything else."""
    try:
        event = json.loads(payload)
    except json.JSONDecodeError:
        return payload
    event.pop("event_id", None)
    return json.dumps(event)


def test_same_seed_same_sequence():
    def run(seed):
        sim = VitalSimulator(seed=seed, malformed_rate=0.2)
        return [without_event_id(sim.maybe_corrupt(sim.reading(p, 0, now=T0)))
                for p in sim.patients * 4]

    assert run(42) == run(42)
    assert run(42) != run(43)


def test_event_ids_are_unique_across_runs_with_the_same_seed():
    # Same ids in a rerun would make Q3's alert ids (<event_id>:<rule>) look like duplicates.
    def ids(seed):
        sim = VitalSimulator(seed=seed)
        return {sim.reading(p, 0, now=T0)["event_id"] for p in sim.patients}

    first, second = ids(42), ids(42)
    assert len(first) == 15 and not first & second


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


# ---- delivery callback: metrics and logs (A7) ---------------------------------------

class FakeMsg:
    def __init__(self, key, partition):
        self._key, self._partition = key, partition

    def key(self):
        return self._key.encode()

    def partition(self):
        return self._partition


def sample(name, **labels):
    from prometheus_client import REGISTRY

    return REGISTRY.get_sample_value(name, labels) or 0.0


def test_delivery_counts_sent_per_partition_and_last_event_time():
    from simulators.vital_producer.main import DeliveryStats

    stats = DeliveryStats()
    before = sample("ward_producer_events_sent_total", partition="2")
    stats.callback(None, FakeMsg("P007", 2))
    stats.callback(None, FakeMsg("P007", 2))
    assert stats.sent == 2 and stats.partition_of == {"P007": 2}
    assert sample("ward_producer_events_sent_total", partition="2") == before + 2
    assert sample("ward_producer_last_event_timestamp_seconds") > 1.7e9


def test_delivery_error_is_counted_and_logged_as_json(capsys):
    from simulators.vital_producer.main import DeliveryStats

    stats = DeliveryStats()
    before = sample("ward_producer_send_errors_total")
    stats.callback("KafkaError: timed out", FakeMsg("P003", 0))
    assert stats.errors == 1 and stats.sent == 0
    assert sample("ward_producer_send_errors_total") == before + 1
    [line] = [json.loads(x) for x in capsys.readouterr().out.splitlines()]
    assert (line["component"], line["event"], line["severity"]) == ("vital-producer", "send_failed", "ERROR")
    assert line["patient_id"] == "P003"


# ---- demo scenarios (A8) ------------------------------------------------------------

def run_stream(scenario="none", seed=42, seconds=300, start=60, duration=180):
    """Run the stream on a fake clock: [(elapsed_s, patient_id, event), ...]."""
    sim = VitalSimulator(seed=seed, scenario=scenario, scenario_patient="P007",
                         scenario_start_s=start, scenario_duration_s=duration)
    now = [0.0]
    out = []
    for pid, payload in sim.stream(clock=lambda: now[0],
                                   sleep=lambda s: now.__setitem__(0, now[0] + s)):
        if now[0] >= seconds:
            break
        out.append((now[0], pid, json.loads(payload)))
    return out


def breaches(events):
    """(offset, patient, vital) for every reading outside the Q3 thresholds: what alerts fire on."""
    t = load_thresholds().vitals
    rules = {
        "spo2": lambda v: v < t["spo2_min"]["below"],
        "heart_rate": lambda v: v < t["heart_rate_avg"]["below"] or v > t["heart_rate_avg"]["above"],
        "systolic_bp": lambda v: v < t["systolic_bp"]["below"] or v > t["systolic_bp"]["above"],
        "temperature": lambda v: v > t["temperature_max"]["above"],
    }
    return [(round(at, 3), pid, vital) for at, pid, e in events
            for vital, broken in rules.items() if broken(e[vital])]


def p007(events, lo, hi):
    return [e for at, pid, e in events if pid == "P007" and lo <= at < hi]


def test_same_seed_gives_the_same_alerts_every_run():
    for scenario in ("spike", "hr_spike", "spo2_drop", "outage"):
        first = breaches(run_stream(scenario))
        assert first == breaches(run_stream(scenario)), scenario
        assert first != breaches(run_stream(scenario, seed=7)), scenario


def test_hr_spike_raises_only_heart_rate():
    during = p007(run_stream("hr_spike"), 60, 240)
    assert during and all(e["heart_rate"] >= 135 for e in during)
    assert all(e["spo2"] > 92 for e in during)


def test_spo2_drop_falls_steadily_while_heart_rate_climbs():
    events = run_stream("spo2_drop")
    avg = lambda rows, k: sum(e[k] for e in rows) / len(rows)  # noqa: E731
    thirds = [p007(events, 60 + 60 * i, 120 + 60 * i) for i in range(3)]
    spo2 = [avg(rows, "spo2") for rows in thirds]
    hr = [avg(rows, "heart_rate") for rows in thirds]
    assert spo2[0] > spo2[1] > spo2[2] and spo2[0] - spo2[2] >= 5
    assert hr[0] < hr[1] < hr[2]
    assert min(e["spo2"] for e in thirds[2]) < 92          # low enough to alert
    assert all(e["spo2"] > 92 for e in p007(events, 0, 60))


def test_outage_silences_every_patient_then_resumes():
    events = run_stream("outage", start=60, duration=60)
    times = [at for at, _, _ in events]
    assert not [at for at in times if 60 <= at < 120]
    assert {pid for at, pid, _ in events if at < 60} == {f"P{i:03d}" for i in range(1, 16)}
    assert {pid for at, pid, _ in events if at >= 120} == {f"P{i:03d}" for i in range(1, 16)}
