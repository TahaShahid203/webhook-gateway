import json
from datetime import timedelta

import pytest

from events import delivery, delivery_policy as policy
from events.ingest import ingest_message
from events.models import Alert, Consumer, Delivery, DeliveryAttempt, Event, Source, Subscription

T0 = 1_800_000_000
pytestmark = pytest.mark.django_db


@pytest.fixture
def src():
    return Source.objects.create(slug="shop-a", scheme="shopify", secret="s",
                                 event_id_header="x-shopify-webhook-id", event_type_header="x-shopify-topic")


@pytest.fixture
def cons():
    return Consumer.objects.create(name="erp", url="http://consumer.invalid/hook", secret="k", timeout_seconds=3)


def sub(src, cons, **kw):
    return Subscription.objects.create(source=src, consumer=cons, **kw)


def make_event(n, topic="orders/create", order="o1", ts=T0):
    body = json.dumps({"order": order, "n": n}).encode()
    msg = {"source": "shop-a", "received_at": ts, "quarantined": False,
           "x-shopify-webhook-id": f"w{n}", "x-shopify-topic": topic}
    return ingest_message(body, msg).event


def run(delivery_id, status, retry_after=None, error=""):
    """claim, then finish with a canned HTTP result (no network)."""
    cl = delivery.claim(delivery_id)
    assert cl is not None, "expected the delivery to be claimable"
    return cl, delivery.finish(cl, status, error, "", retry_after)


def test_outbox_rows_are_created_with_the_event(src, cons):
    sub(src, cons)
    e = make_event(1)
    d = Delivery.objects.get(event=e)
    assert d.consumer == cons and d.status == "pending" and d.attempts == 0


def test_event_type_pattern_filters_subscriptions(src, cons):
    sub(src, cons, event_type_pattern="refunds/*")
    make_event(1, topic="orders/create")
    assert Delivery.objects.count() == 0
    make_event(2, topic="refunds/create")
    assert Delivery.objects.count() == 1


def test_quarantined_events_are_never_delivered(src, cons):
    sub(src, cons)
    body = b'{"x":1}'
    r = ingest_message(body, {"source": "shop-a", "received_at": T0, "quarantined": True})
    assert r.event.status == "quarantined" and Delivery.objects.count() == 0


def test_success_marks_delivered_and_logs_the_attempt(src, cons):
    sub(src, cons)
    d = Delivery.objects.get(event=make_event(1))
    cl, outcome = run(d.id, 200)
    d.refresh_from_db()
    assert outcome == "delivered" and d.status == "delivered" and d.delivered_at and d.attempts == 1
    a = DeliveryAttempt.objects.get(delivery=d)
    assert a.number == 1 and a.status_code == 200 and a.finished_at


def test_500_schedules_a_retry_with_growing_delay(src, cons):
    sub(src, cons)
    d = Delivery.objects.get(event=make_event(1))
    _, outcome = run(d.id, 500)
    d.refresh_from_db()
    assert outcome == "retry" and d.status == "pending" and d.attempts == 1
    gap = (d.next_attempt_at - delivery.now()).total_seconds()
    assert policy.BASE_DELAY * 0.9 <= gap <= policy.BASE_DELAY * 1.3


def test_not_claimable_before_next_attempt_time(src, cons):
    sub(src, cons)
    d = Delivery.objects.get(event=make_event(1))
    run(d.id, 503)
    assert delivery.claim(d.id) is None


def test_400_fails_at_once_without_retry(src, cons):
    sub(src, cons)
    d = Delivery.objects.get(event=make_event(1))
    _, outcome = run(d.id, 400)
    d.refresh_from_db()
    assert outcome == "failed" and d.status == "failed" and d.attempts == 1
    assert Alert.objects.filter(kind="delivery_failed").exists()


def test_retry_after_is_honoured(src, cons):
    sub(src, cons)
    d = Delivery.objects.get(event=make_event(1))
    run(d.id, 429, retry_after="600")
    d.refresh_from_db()
    assert (d.next_attempt_at - delivery.now()).total_seconds() > 500 * policy.TIME_SCALE


def test_retries_exhausted_becomes_dead_and_alerts(src, cons, monkeypatch):
    monkeypatch.setattr(policy, "MAX_ATTEMPTS", 3)
    sub(src, cons)
    d = Delivery.objects.get(event=make_event(1))
    for _ in range(3):
        Delivery.objects.filter(pk=d.id).update(next_attempt_at=delivery.now())
        _, outcome = run(d.id, 503)
    d.refresh_from_db()
    assert outcome == "dead" and d.status == "dead" and d.attempts == 3
    assert Alert.objects.filter(kind="delivery_dead").exists()


def test_breaker_opens_after_five_failures_then_blocks_new_deliveries(src, cons):
    sub(src, cons)
    ids = [Delivery.objects.get(event=make_event(i)).id for i in range(1, 8)]
    for i in ids[:5]:
        run(i, 503)
    cons.refresh_from_db()
    assert cons.breaker_state == "open" and Alert.objects.filter(kind="consumer_paused").exists()
    assert delivery.claim(ids[5]) is None            # paused: nothing is sent, nothing dies


def test_in_flight_when_breaker_opens_is_stopped_not_counted(src, cons):
    sub(src, cons)
    ids = [Delivery.objects.get(event=make_event(i)).id for i in range(1, 8)]
    straggler = delivery.claim(ids[6])               # in flight before the breaker trips
    for i in ids[:5]:
        run(i, 503)
    outcome = delivery.finish(straggler, 503, "", "", None)
    d = Delivery.objects.get(pk=ids[6])
    assert outcome == "stopped" and d.status == "pending" and d.attempts == 0   # no attempt burned
    assert d.next_attempt_at > delivery.now()


def test_breaker_probe_after_cooldown_closes_on_success(src, cons):
    sub(src, cons)
    ids = [Delivery.objects.get(event=make_event(i)).id for i in range(1, 8)]
    for i in ids[:5]:
        run(i, 503)
    Consumer.objects.filter(pk=cons.pk).update(breaker_opened_at=delivery.now().timestamp() - 10_000)
    _, outcome = run(ids[5], 200)                    # the single probe
    cons.refresh_from_db()
    assert outcome == "delivered" and cons.breaker_state == "closed" and cons.breaker_failures == 0


def test_per_consumer_in_flight_cap(src, cons):
    Consumer.objects.filter(pk=cons.pk).update(max_in_flight=2)
    sub(src, cons)
    ids = [Delivery.objects.get(event=make_event(i, order=f"o{i}")).id for i in range(1, 4)]
    assert delivery.claim(ids[0]) and delivery.claim(ids[1])
    assert delivery.claim(ids[2]) is None            # cap reached: this consumer cannot hog the workers


def test_ordered_key_goes_one_at_a_time_in_arrival_order(src, cons):
    sub(src, cons, ordered=True, ordering_key_json_key="order")
    a, b, other = (Delivery.objects.get(event=make_event(n, order=o)).id
                   for n, o in ((1, "o1"), (2, "o1"), (3, "o2")))
    assert delivery.claim(b) is None                 # b waits for a
    assert delivery.claim(other) is not None         # a different key is not blocked
    cl = delivery.claim(a)
    assert cl and delivery.claim(b) is None          # a in flight: b still waits
    delivery.finish(cl, 200, "", "", None)
    assert delivery.claim(b) is not None


def test_dead_event_blocks_its_key_until_a_human_acts(src, cons, monkeypatch):
    monkeypatch.setattr(policy, "MAX_ATTEMPTS", 1)
    sub(src, cons, ordered=True, ordering_key_json_key="order")
    a, b = (Delivery.objects.get(event=make_event(n, order="o1")).id for n in (1, 2))
    run(a, 503)
    assert Delivery.objects.get(pk=a).status == "dead"
    assert delivery.claim(b) is None                 # blocked behind the dead event
    Delivery.objects.filter(pk=a).update(status="delivered")      # a human replayed or skipped it
    assert delivery.claim(b) is not None


def test_unordered_subscription_does_not_block(src, cons):
    sub(src, cons)
    a, b = (Delivery.objects.get(event=make_event(n, order="o1")).id for n in (1, 2))
    assert delivery.claim(b) is not None and delivery.claim(a) is not None


def test_new_consumer_breaker_cooldown_comes_from_the_policy(monkeypatch):
    # regression: the model once stored a hard-coded 60.0, so scaled-down test stacks waited a real minute
    monkeypatch.setattr(policy, "BREAKER_COOLDOWN", 1.2)
    c = Consumer.objects.create(name="fresh", url="http://x.invalid/", secret="k")
    assert c.breaker_cooldown == 1.2
