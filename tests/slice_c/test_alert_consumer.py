"""api/alert_consumer.py: parsing, de-duplication, retry and commit-after-insert (no Kafka/DB)."""
import json
from datetime import timezone

import pytest

from api import alert_consumer as ac


def alert(alert_id="e1:SPO2_LOW", **overrides) -> dict:
    a = {
        "alert_id": alert_id, "patient_id": "P007", "bed_id": "BED-07",
        "alert_type": "THRESHOLD", "rule": "SPO2_LOW", "severity": "WARNING",
        "metric": "spo2", "metric_value": 88.0, "threshold": 92.0,
        "event_time": "2026-09-28T08:00:05.000Z", "window_start": None, "window_end": None,
        "abnormal_count": 1, "reading_count": 1, "sim_day": 96,
        "message": "spo2 88.0 below limit 92.0", "detected_at": "2026-09-28T08:00:12.345Z",
    }
    a.update(overrides)
    return a


class Msg:
    def __init__(self, value, err=None, partition=0, offset=0):
        self._value, self._err, self._p, self._o = value, err, partition, offset

    def value(self):
        return self._value

    def error(self):
        return self._err

    def partition(self):
        return self._p

    def offset(self):
        return self._o


def msg(**kw):
    return Msg(json.dumps(alert(**kw)).encode())


# ---------------------------------------------------------------- parsing --

def test_parse_valid_alert_to_row_in_column_order():
    row = dict(zip(ac.COLUMNS, ac.parse_alert(json.dumps(alert()))))
    assert row["alert_id"] == "e1:SPO2_LOW" and row["severity"] == "WARNING"
    assert row["event_time"].tzinfo == timezone.utc and row["event_time"].second == 5
    assert row["metric_value"] == 88.0 and row["sim_day"] == 96
    assert row["window_start"] is None


@pytest.mark.parametrize("raw, reason", [
    (None, "empty"),
    (b"{not json", "not JSON"),
    (b"[1, 2]", "not a JSON object"),
    (json.dumps(alert(alert_id=None)).encode(), "missing fields: alert_id"),
    (json.dumps(alert(event_time="yesterday")).encode(), "bad value"),
    (json.dumps(alert(metric_value="high")).encode(), "bad value"),
])
def test_invalid_alerts_are_rejected(raw, reason):
    with pytest.raises(ac.InvalidAlert, match=reason):
        ac.parse_alert(raw)


def test_decode_batch_drops_bad_and_duplicate_messages():
    batch = [msg(), msg(), Msg(b"garbage"), Msg(None, err="broker hiccup"),
             msg(alert_id="P007:SUSTAINED_SPO2_LOW:1790000000", severity="CRITICAL")]
    rows = ac.decode_batch(batch)
    assert [r[0] for r in rows] == ["e1:SPO2_LOW", "P007:SUSTAINED_SPO2_LOW:1790000000"]


# ------------------------------------------------------ retry and commits --

class FakeStore:
    def __init__(self, failures=0):
        self.failures = failures
        self.inserted: list[str] = []

    def insert(self, rows):
        if self.failures:
            self.failures -= 1
            raise ConnectionError("postgres is down")
        new = [r for r in rows if r[0] not in self.inserted]     # ON CONFLICT DO NOTHING
        self.inserted += [r[0] for r in new]
        return [(r[0], r[5], r[3]) for r in new]


def test_store_retries_with_backoff_until_db_is_back():
    store, sleeps = FakeStore(failures=2), []
    rows = ac.decode_batch([msg()])
    assert ac.store_with_retry(store, rows, should_stop=lambda: False, sleep=sleeps.append)
    assert sleeps == [2, 4] and store.inserted == ["e1:SPO2_LOW"]


def test_store_gives_up_only_when_stopping():
    assert not ac.store_with_retry(FakeStore(failures=99), ac.decode_batch([msg()]),
                                   should_stop=lambda: True, sleep=lambda s: None)


class FakeConsumer:
    def __init__(self, batches):
        self.batches = list(batches)
        self.commits = 0

    def consume(self, num_messages, timeout):
        return self.batches.pop(0) if self.batches else []

    def commit(self, asynchronous):
        self.commits += 1


def stop_when_empty(consumer):
    return lambda: not consumer.batches


def test_offsets_are_committed_only_after_insert():
    consumer = FakeConsumer([[msg()], [msg(alert_id="e2:SPO2_LOW")]])
    store = FakeStore()
    ac.run(consumer, store, stop_when_empty(consumer))
    assert store.inserted == ["e1:SPO2_LOW", "e2:SPO2_LOW"] and consumer.commits == 2


def test_no_commit_when_db_stays_down_and_we_stop():
    consumer = FakeConsumer([[msg()]])
    ac.run(consumer, FakeStore(failures=99), stop_when_empty(consumer))
    assert consumer.commits == 0          # the alert stays in Kafka and is read again later


def test_redelivered_alert_is_stored_once():
    consumer = FakeConsumer([[msg()], [msg()]])        # e.g. re-read after a crash
    store = FakeStore()
    ac.run(consumer, store, stop_when_empty(consumer))
    assert store.inserted == ["e1:SPO2_LOW"]


def test_batch_of_only_bad_messages_is_committed():
    consumer = FakeConsumer([[Msg(b"garbage")]])
    ac.run(consumer, FakeStore(), stop_when_empty(consumer))
    assert consumer.commits == 1          # skipped, so a bad message cannot block the partition
