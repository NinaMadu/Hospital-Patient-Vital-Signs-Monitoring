"""The demo's vital-sign feed: Member A's simulator, switchable from the UI.

During the demo this replaces the `vital-producer` service. It runs the same VitalSimulator
and the same Kafka producer settings (simulators/vital_producer/main.py); the only addition
is that a scenario can be started, changed or stopped while the ward keeps streaming.

A switch restarts the simulator with the new scenario starting a few seconds later
(`lead_s`). The seed stays the same, so every run of a scenario produces the same values and
the same alerts, as in Member A's command-line demo.
"""
from __future__ import annotations

import json
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field

from simulators.vital_producer import main as vp

# Scenarios this checkout's simulator knows (main has spike, hr_spike, spo2_drop, outage).
SUPPORTED = tuple(getattr(vp, "SCENARIOS", ("none", "spike")))
NUMERIC_FIELDS = ("heart_rate", "spo2", "systolic_bp", "diastolic_bp", "temperature")


class _Stopped(Exception):
    """Raised inside the simulator's sleep to end the stream at once (even mid-outage)."""


@dataclass
class ScenarioRun:
    scenario: str
    patient: str | None
    requested_at: float           # unix time of the button press
    starts_at: float              # unix time the scenario begins in the feed
    ends_at: float
    # Filled in by the server while the demo runs: how fast the pipeline reacted.
    first_alert_after_s: float | None = None
    concerning_after_s: float | None = None
    stale_after_s: float | None = None

    def phase(self, now: float) -> str:
        if now < self.starts_at:
            return "starting"
        return "active" if now < self.ends_at else "finished"


@dataclass
class FeedSettings:
    scenario: str = "none"
    patient: str = "P007"
    duration_s: float = 180.0
    malformed_rate: float = 0.0
    paused: bool = False


@dataclass
class _Counters:
    sent: int = 0
    errors: int = 0
    malformed: int = 0
    last_ack_at: float | None = None
    per_partition: dict = field(default_factory=dict)


def looks_malformed(payload: str) -> bool:
    """True for the simulator's deliberately broken readings (they go to vitals-dlq)."""
    try:
        event = json.loads(payload)
    except ValueError:
        return True
    if not isinstance(event, dict):
        return True
    for name in NUMERIC_FIELDS:
        value = event.get(name)
        if not isinstance(value, (int, float)):
            return True
    return not 50 <= event["spo2"] <= 100


class ScenarioProducer:
    """Streams the ward to Kafka on a background thread; settings change on the fly."""

    def __init__(self, topic: str, producer_factory=None, simulator_cls=None,
                 lead_s: float = 3.0, clock=time.time):
        self.topic = topic
        self._producer_factory = producer_factory
        self._simulator_cls = simulator_cls or vp.VitalSimulator
        self.lead_s = lead_s
        self.clock = clock
        self.settings = FeedSettings()
        self.run: ScenarioRun | None = None
        self.counters = _Counters()
        self.recent = deque(maxlen=40)          # newest readings, for the live feed panel
        self.last_error: str | None = None
        self._producer = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._stats = vp.DeliveryStats()       # also updates the ward_producer_* metrics

    # ------------------------------------------------------------ controls --

    def start(self) -> None:
        """Start (or restart) the feed with the current settings."""
        with self._lock:
            self._halt()
            if self.settings.paused:
                return
            self._stop = threading.Event()
            self._thread = threading.Thread(target=self._loop, args=(self._stop,),
                                            name="demo-feed", daemon=True)
            self._thread.start()

    def set_scenario(self, scenario: str, patient: str = "P007", duration_s: float = 180.0) -> None:
        if scenario not in SUPPORTED:
            raise ValueError(f"scenario {scenario!r} is not in this simulator: {SUPPORTED}")
        now = self.clock()
        self.settings.scenario, self.settings.patient = scenario, patient
        self.settings.duration_s, self.settings.paused = float(duration_s), False
        if scenario == "none":
            self.run = None
        else:
            starts = now + self.lead_s
            self.run = ScenarioRun(scenario=scenario,
                                   patient=None if scenario == "outage" else patient,
                                   requested_at=now, starts_at=starts,
                                   ends_at=starts + float(duration_s))
        self.start()

    def set_malformed_rate(self, rate: float) -> None:
        if not 0.0 <= rate <= 0.5:
            raise ValueError("malformed rate must be between 0 and 0.5")
        self.settings.malformed_rate = rate
        self.start()          # a running scenario keeps its timeline (see _loop)

    def pause(self) -> None:
        with self._lock:
            self.settings.paused = True
            self._halt()

    def resume(self) -> None:
        self.settings.paused = False
        self.start()

    def close(self) -> None:
        with self._lock:
            self._halt()
        if self._producer is not None:
            self._producer.flush(5)

    # ------------------------------------------------------------- status --

    def status(self) -> dict:
        now = self.clock()
        running = self._thread is not None and self._thread.is_alive()
        run = None
        if self.run:
            run = asdict(self.run) | {
                "phase": self.run.phase(now),
                "seconds_to_start": max(self.run.starts_at - now, 0.0),
                "seconds_left": max(self.run.ends_at - now, 0.0),
            }
        c = self.counters
        return {
            "running": running,
            "paused": self.settings.paused,
            "settings": asdict(self.settings),
            "supported_scenarios": list(SUPPORTED),
            "scenario": run,
            "sent": c.sent,
            "errors": c.errors,
            "malformed_sent": c.malformed,
            "per_partition": dict(sorted(c.per_partition.items())),
            "seconds_since_last_ack": None if c.last_ack_at is None else now - c.last_ack_at,
            "last_error": self.last_error,
            "recent": list(self.recent)[::-1][:15],
        }

    # ------------------------------------------------------------ internals --

    def _halt(self) -> None:
        if self._thread is not None:
            self._stop.set()
            self._thread.join(timeout=10)
            self._thread = None

    def _kafka(self):
        if self._producer is None:
            self._producer = (self._producer_factory or vp.build_producer)()
        return self._producer

    def _loop(self, stop: threading.Event) -> None:
        s, run, now = self.settings, self.run, self.clock()
        scenario_on = run is not None and run.phase(now) != "finished"
        # The scenario keeps its wall-clock window across restarts: it starts lead_s after
        # the button press, or at once when the feed restarts in the middle of it.
        lead = max(run.starts_at - now, 0.0) if scenario_on else 0.0
        sim = self._simulator_cls(
            malformed_rate=s.malformed_rate,
            scenario=s.scenario if scenario_on else "none",
            scenario_patient=s.patient,
            scenario_start_s=lead,
            scenario_duration_s=(run.ends_at - now - lead) if scenario_on else s.duration_s,
        )

        def sleep(seconds: float) -> None:
            if stop.wait(seconds):
                raise _Stopped

        try:
            producer = self._kafka()
            for pid, payload in sim.stream(sleep=sleep):
                if stop.is_set():
                    break
                self._produce(producer, pid, payload)
            # stream() never ends on its own; reaching here means stop was set.
        except _Stopped:
            pass
        except Exception as exc:  # noqa: BLE001 - shown in the UI, feed can be restarted
            self.last_error = f"{type(exc).__name__}: {exc}"
            vp.log.error("demo_feed_failed", error=self.last_error)

    def _produce(self, producer, pid: str, payload: str) -> None:
        bad = looks_malformed(payload)

        def delivered(err, msg):
            self._stats.callback(err, msg)
            if err is not None:
                self.counters.errors += 1
                self.last_error = str(err)
                return
            c = self.counters
            c.sent += 1
            c.last_ack_at = self.clock()
            part = msg.partition()
            c.per_partition[part] = c.per_partition.get(part, 0) + 1
            if bad:
                c.malformed += 1
            self.recent.append({"at": c.last_ack_at, "key": pid, "partition": part,
                                "malformed": bad, "payload": payload[:220]})

        while True:
            try:
                producer.produce(self.topic, key=pid, value=payload, on_delivery=delivered)
                break
            except BufferError:
                producer.poll(1)       # local queue full: serve callbacks, then retry
        producer.poll(0)
