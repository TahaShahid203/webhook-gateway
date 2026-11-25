"""Regression: the playground feed must never skip a delivery attempt that was still in flight when it polled."""
import json
from datetime import timedelta

import pytest
from django.test import RequestFactory

from events import delivery
from events.ingest import ingest_message
from events.models import Consumer, Delivery, DeliveryAttempt, Source, Subscription
from playground import views

pytestmark = pytest.mark.django_db
T0 = 1_800_000_000


@pytest.fixture(autouse=True)
def playground_on(settings, monkeypatch):
    settings.PLAYGROUND = True
    monkeypatch.setattr(views, "_queue_depths", lambda: {"rabbitmq": 0, "redis": 0})


def attempts_for(deliveries_):
    return [DeliveryAttempt.objects.create(delivery=d, number=1, started_at=delivery.now()) for d in deliveries_]


def poll(cursor):
    r = RequestFactory().get("/play/api/poll", {"edge": 0, "attempt": cursor, "event_ts": "2020-01-01T00:00:00+00:00", "dup_ts": "2020-01-01T00:00:00+00:00"})
    return json.loads(views.poll(r).content)


def make_deliveries(n):
    src = Source.objects.create(slug="s", scheme="stripe", secret="x", event_id_json_key="id")
    Subscription.objects.create(source=src, consumer=Consumer.objects.create(name="c-demo", url="http://x.invalid/", secret="k"))
    for i in range(n):
        ingest_message(json.dumps({"id": f"e{i}"}).encode(), {"source": "s", "received_at": T0, "quarantined": False})
    return list(Delivery.objects.order_by("id"))


def test_an_attempt_still_in_flight_is_not_skipped():
    ds = make_deliveries(3)
    a1, a2, a3 = attempts_for(ds)
    now = delivery.now()
    DeliveryAttempt.objects.filter(pk__in=[a1.pk, a3.pk]).update(finished_at=now, status_code=200)   # a2 is still in flight
    first = poll(0)
    assert [a["id"] for a in first["attempts"]] == [a1.pk]                      # a3 waits behind the open a2
    assert first["cursors"]["attempt"] == a1.pk                                  # the cursor did NOT jump past a2
    DeliveryAttempt.objects.filter(pk=a2.pk).update(finished_at=now, status_code=200)               # a2 finishes
    second = poll(first["cursors"]["attempt"])
    assert [a["id"] for a in second["attempts"]] == [a2.pk, a3.pk]              # nothing was lost


def test_an_attempt_abandoned_by_a_dead_worker_does_not_block_the_feed_forever():
    ds = make_deliveries(2)
    a1, a2 = attempts_for(ds)
    DeliveryAttempt.objects.filter(pk=a1.pk).update(started_at=delivery.now() - timedelta(seconds=60))   # worker died
    DeliveryAttempt.objects.filter(pk=a2.pk).update(finished_at=delivery.now(), status_code=200)
    got = poll(0)
    assert [a["id"] for a in got["attempts"]] == [a1.pk, a2.pk] and got["attempts"][0]["done"] is False
