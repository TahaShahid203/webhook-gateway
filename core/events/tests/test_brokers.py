import base64
import json
from unittest import mock

import pytest

from events import brokers, controls
from events.models import Event, Source

pytestmark = pytest.mark.django_db
T0 = 1_800_000_000


@pytest.fixture(autouse=True)
def keep_test_connection(monkeypatch):
    # the worker recycles stale connections between messages; inside a test that would close the test transaction
    monkeypatch.setattr(brokers, "close_old_connections", lambda: None)


@pytest.fixture
def src():
    return Source.objects.create(slug="s1", scheme="stripe", secret="s", event_id_json_key="id")


def test_redis_payload_roundtrip():
    body = b'{"id":"e1"}'
    payload = json.dumps({"headers": {"source": "s1", "received_at": T0, "quarantined": False},
                          "body": base64.b64encode(body).decode()})
    b, h = brokers.decode_redis_payload(payload)
    assert b == body and h["source"] == "s1"


def test_handle_stores_the_event(src):
    assert brokers.handle(b'{"id":"e1"}', {"source": "s1", "received_at": T0, "quarantined": False}) == "ok"
    assert Event.objects.filter(provider_event_id="e1").exists()


def test_handle_asks_for_a_retry_when_the_database_is_down(src):
    from django.db import OperationalError
    with mock.patch.object(brokers, "ingest_message", side_effect=OperationalError("down")):
        assert brokers.handle(b"{}", {"source": "s1", "received_at": T0}) == "retry"


def test_handle_marks_a_poison_message_dead(src):
    with mock.patch.object(brokers, "ingest_message", side_effect=ValueError("bad")):
        assert brokers.handle(b"{}", {"source": "s1", "received_at": T0}) == "dead"


def test_requeue_orphans_moves_unfinished_messages_back():
    class FakeRedis:
        def __init__(self):
            self.processing, self.queue = ["a", "b"], []

        def lmove(self, src, dst, wherefrom, whereto):
            if not self.processing:
                return None
            item = self.processing.pop(0)
            self.queue.append(item)
            return item
    r = FakeRedis()
    assert brokers.requeue_orphans(r) == 2 and r.queue == ["a", "b"] and not r.processing


def test_ingest_pause_switch():
    assert controls.ingest_paused() is False
    controls.put("ingest_paused", "1")
    assert controls.ingest_paused() is True
    controls.put("ingest_paused", "0")
    assert controls.ingest_paused() is False


def test_pause_flag_is_read_at_most_once_a_second(monkeypatch):
    calls = []
    monkeypatch.setattr(brokers.controls, "ingest_paused", lambda: calls.append(1) or False)
    clock = {"t": 1000.0}
    monkeypatch.setattr(brokers.time, "monotonic", lambda: clock["t"])
    brokers._pause.update(t=-10.0, v=False)
    for _ in range(50):
        brokers.paused_now()
    assert len(calls) == 1                       # 50 messages, one database read
    clock["t"] += 1.1
    brokers.paused_now()
    assert len(calls) == 2


def test_pause_flag_keeps_the_last_value_when_the_database_is_down(monkeypatch):
    from django.db import OperationalError

    def boom():
        raise OperationalError("down")
    monkeypatch.setattr(brokers.controls, "ingest_paused", boom)
    clock = {"t": 5000.0}
    monkeypatch.setattr(brokers.time, "monotonic", lambda: clock["t"])
    brokers._pause.update(t=-10.0, v=True)
    assert brokers.paused_now() is True          # the operator's "paused" must survive a database blip
