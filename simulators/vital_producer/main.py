"""Bedside vital-sign simulator and Kafka producer.

Owner: Member A.
Emits one JSON reading per patient every 2-5 s to topic patient-vitals, keyed by patient_id.
Supports seeded, deterministic scenarios for the demo, e.g. --scenario spike --patient P007.

    python -m simulators.vital_producer.main                          # normal ward, to Kafka
    python -m simulators.vital_producer.main --scenario spike --patient P007
    python -m simulators.vital_producer.main --malformed-rate 0.02    # some bad events for the DLQ
    python -m simulators.vital_producer.main --dry-run --max-events 20  # print, no Kafka

The simulator (VitalSimulator) is pure Python with no Kafka dependency, so tests can check
its output directly. main() wires it to a confluent-kafka Producer.

Observability (A7): JSON logs through common/logger.py, and Prometheus metrics on
http://vital-producer:8001/metrics (scraped by Prometheus, monitoring profile):
    ward_producer_events_sent_total{partition}     readings Kafka acknowledged, per partition
    ward_producer_send_errors_total                readings Kafka failed to deliver
    ward_producer_last_event_timestamp_seconds     when Kafka last acknowledged a reading
"""
from __future__ import annotations

import argparse
import heapq
import json
import os
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from common import metrics
from common.config import get_settings
from common.logger import get_logger

SETTINGS = get_settings()
PATIENT_COUNT = SETTINGS.patients.count
MIN_INTERVAL_S = float(SETTINGS.vitals_simulator.min_interval_seconds)
MAX_INTERVAL_S = float(SETTINGS.vitals_simulator.max_interval_seconds)
SPIKE_PROBABILITY = SETTINGS.vitals_simulator.abnormal_spike_probability
DEFAULT_SEED = SETTINGS.vitals_simulator.random_seed
METRICS_PORT = int(os.getenv("VITAL_PRODUCER_METRICS_PORT", "8001"))

FIELDS = (
    "event_id", "patient_id", "bed_id", "heart_rate", "spo2",
    "systolic_bp", "diastolic_bp", "temperature", "timestamp",
)


log = get_logger("vital-producer")

EVENTS_SENT = metrics.counter("producer_events_sent", "Vital readings acknowledged by Kafka",
                              ["partition"])
SEND_ERRORS = metrics.counter("producer_send_errors", "Vital readings Kafka failed to deliver")
LAST_EVENT = metrics.gauge("producer_last_event_timestamp_seconds",
                           "Unix time Kafka last acknowledged a vital reading")


@dataclass
class Baseline:
    """A patient's normal resting vitals; each reading is this plus noise."""
    heart_rate: float
    spo2: float
    systolic_bp: float
    diastolic_bp: float
    temperature: float


class VitalSimulator:
    """Generates readings for a ward of patients from one seeded random generator.

    Same seed + same arguments => same sequence of patients, values and event_ids.
    Only the `timestamp` field (wall clock) differs between runs.
    """

    def __init__(
        self,
        seed: int = DEFAULT_SEED,
        patient_count: int = PATIENT_COUNT,
        spike_probability: float = SPIKE_PROBABILITY,
        malformed_rate: float = 0.0,
        scenario: str = "none",
        scenario_patient: str = "P007",
        scenario_start_s: float = 60.0,
        scenario_duration_s: float = 180.0,
    ):
        self.rng = random.Random(seed)
        self.patients = [f"P{i:03d}" for i in range(1, patient_count + 1)]
        self.beds = {pid: f"BED-{i:02d}" for i, pid in enumerate(self.patients, start=1)}
        self.baselines = {pid: self._random_baseline() for pid in self.patients}
        self.spike_probability = spike_probability
        self.malformed_rate = malformed_rate
        self.scenario = scenario
        self.scenario_patient = scenario_patient
        self.scenario_start_s = scenario_start_s
        self.scenario_end_s = scenario_start_s + scenario_duration_s

    def _random_baseline(self) -> Baseline:
        r = self.rng
        return Baseline(
            heart_rate=r.uniform(62, 88),
            spo2=r.uniform(95.5, 99.0),
            systolic_bp=r.uniform(108, 132),
            diastolic_bp=r.uniform(68, 84),
            temperature=r.uniform(36.4, 37.1),
        )

    def next_interval(self) -> float:
        """Seconds until this patient's next reading (2-5 s)."""
        return self.rng.uniform(MIN_INTERVAL_S, MAX_INTERVAL_S)

    def reading(self, patient_id: str, elapsed_s: float, now: datetime | None = None) -> dict:
        """One well-formed reading. `elapsed_s` is seconds since the producer started."""
        r = self.rng
        b = self.baselines[patient_id]
        hr = b.heart_rate + r.gauss(0, 3)
        spo2 = b.spo2 + r.gauss(0, 0.6)
        sys_bp = b.systolic_bp + r.gauss(0, 4)
        dia_bp = b.diastolic_bp + r.gauss(0, 3)
        temp = b.temperature + r.gauss(0, 0.1)

        if self._in_scenario(patient_id, elapsed_s):
            # Sustained deterioration: tachycardia with falling oxygen saturation.
            hr = r.uniform(135, 160)
            spo2 = r.uniform(84, 89)
        elif r.random() < self.spike_probability:
            # A short, random abnormal spike on one vital (a single-reading event).
            kind = r.choice(["heart_rate", "spo2", "systolic_bp", "temperature"])
            if kind == "heart_rate":
                hr = r.uniform(125, 150)
            elif kind == "spo2":
                spo2 = r.uniform(85, 90)
            elif kind == "systolic_bp":
                sys_bp = r.uniform(165, 190)
            else:
                temp = r.uniform(38.3, 39.6)

        now = now or datetime.now(timezone.utc)
        return {
            "event_id": str(uuid.UUID(int=r.getrandbits(128), version=4)),
            "patient_id": patient_id,
            "bed_id": self.beds[patient_id],
            "heart_rate": round(hr),
            "spo2": round(min(spo2, 100.0), 1),
            "systolic_bp": round(sys_bp),
            "diastolic_bp": round(dia_bp),
            "temperature": round(temp, 1),
            "timestamp": now.isoformat(timespec="milliseconds"),
        }

    def _in_scenario(self, patient_id: str, elapsed_s: float) -> bool:
        return (
            self.scenario == "spike"
            and patient_id == self.scenario_patient
            and self.scenario_start_s <= elapsed_s < self.scenario_end_s
        )

    def maybe_corrupt(self, event: dict) -> str:
        """Serialise the event; with probability malformed_rate, break it on purpose.

        Q1 must route these to vitals-dlq instead of crashing or polluting the windows.
        """
        if self.rng.random() >= self.malformed_rate:
            return json.dumps(event)
        bad = dict(event)
        kind = self.rng.choice(["missing_field", "null_value", "out_of_range", "wrong_type", "not_json"])
        if kind == "missing_field":
            bad.pop("heart_rate")
        elif kind == "null_value":
            bad["spo2"] = None
        elif kind == "out_of_range":
            bad["spo2"] = 140.0
        elif kind == "wrong_type":
            bad["heart_rate"] = "fast"
        else:
            return "{not valid json"
        return json.dumps(bad)

    def stream(self, max_events: int | None = None, clock=time.monotonic, sleep=time.sleep):
        """Yield (patient_id, payload) forever (or max_events times), paced in real time.

        A min-heap holds each patient's next due time, so every patient keeps its own
        2-5 s rhythm while one loop serves all of them.
        """
        start = clock()
        due = [(self.next_interval() * self.rng.random(), pid) for pid in self.patients]
        heapq.heapify(due)
        sent = 0
        while max_events is None or sent < max_events:
            due_at, pid = heapq.heappop(due)
            wait = start + due_at - clock()
            if wait > 0:
                sleep(wait)
            payload = self.maybe_corrupt(self.reading(pid, elapsed_s=due_at))
            yield pid, payload
            sent += 1
            heapq.heappush(due, (due_at + self.next_interval(), pid))


# ------------------------------------------------------------------- Kafka --

def build_producer(bootstrap: str):
    from confluent_kafka import Producer

    return Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "vital-producer",
        # Durability: the leader waits for all in-sync replicas before acknowledging.
        "acks": "all",
        # Retries without duplicates or reordering: the broker de-duplicates by
        # (producer id, sequence number), so a retried batch is written once.
        "enable.idempotence": True,
        "retries": 10,
        "retry.backoff.ms": 500,
        "delivery.timeout.ms": 30000,
        # Batch for up to 20 ms: fewer, larger requests at a tiny latency cost.
        "linger.ms": 20,
        # Same key -> same partition, using the Java client's hash (murmur2),
        # so any Kafka client would place a patient on the same partition.
        "partitioner": "murmur2_random",
    })


class DeliveryStats:
    """Counts delivery results, in memory (for the log) and as Prometheus metrics."""

    def __init__(self):
        self.sent = 0
        self.errors = 0
        self.partition_of: dict[str, int] = {}

    def callback(self, err, msg):
        """Called from producer.poll()/flush() once the broker acks (or gives up on) a message."""
        key = msg.key().decode() if msg.key() else None
        if err is not None:
            self.errors += 1
            SEND_ERRORS.inc()
            log.error("send_failed", patient_id=key, error=str(err))
            return
        self.sent += 1
        EVENTS_SENT.labels(partition=str(msg.partition())).inc()
        LAST_EVENT.set_to_current_time()
        previous = self.partition_of.setdefault(key, msg.partition())
        if previous != msg.partition():
            log.warning("partition_changed", patient_id=key, old=previous, new=msg.partition())


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Ward vital-sign simulator -> Kafka")
    p.add_argument("--bootstrap", default=SETTINGS.kafka.bootstrap_servers)
    p.add_argument("--topic", default=SETTINGS.kafka.topics.vitals)
    p.add_argument("--seed", type=int, default=DEFAULT_SEED)
    p.add_argument("--scenario", choices=["none", "spike"], default="none")
    p.add_argument("--patient", default="P007", help="patient for --scenario")
    p.add_argument("--scenario-start", type=float, default=60.0, help="seconds after start")
    p.add_argument("--scenario-duration", type=float, default=180.0)
    p.add_argument("--malformed-rate", type=float, default=0.0)
    p.add_argument("--max-events", type=int, default=None)
    p.add_argument("--dry-run", action="store_true", help="print events instead of sending")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    sim = VitalSimulator(
        seed=args.seed,
        malformed_rate=args.malformed_rate,
        scenario=args.scenario,
        scenario_patient=args.patient,
        scenario_start_s=args.scenario_start,
        scenario_duration_s=args.scenario_duration,
    )

    stopping = False

    def stop(signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    if args.dry_run:
        for _pid, payload in sim.stream(max_events=args.max_events):
            print(payload, flush=True)
            if stopping:
                break
        return 0

    metrics.start_metrics_server(METRICS_PORT)
    producer = build_producer(args.bootstrap)
    stats = DeliveryStats()
    log.info("started", bootstrap=args.bootstrap, topic=args.topic, seed=args.seed,
        scenario=args.scenario, patient=args.patient, malformed_rate=args.malformed_rate)

    for n, (pid, payload) in enumerate(sim.stream(max_events=args.max_events), start=1):
        while True:
            try:
                producer.produce(args.topic, key=pid, value=payload, on_delivery=stats.callback)
                break
            except BufferError:
                # Local queue full (broker slow or down): serve callbacks, then try again.
                producer.poll(1)
        producer.poll(0)  # serve delivery callbacks without blocking
        if n % 100 == 0:
            log.info("progress", sent=stats.sent, errors=stats.errors)
        if stopping:
            break

    remaining = producer.flush(15)
    log.info("stopped", sent=stats.sent, errors=stats.errors, undelivered=remaining,
        partitions=stats.partition_of)
    return 0 if remaining == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
