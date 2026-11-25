import json
from datetime import timedelta

import pytest

from events import alerts, delivery, metrics, replay, retention
from events import delivery_policy as policy
from events.ingest import ingest_message
from events.models import (Alert, AuditLog, Consumer, Delivery, DeliveryAttempt, Event, Source, Subscription)

T0 = 1_800_000_000
pytestmark = pytest.mark.django_db


@pytest.fixture
def src():
    return Source.objects.create(slug="shop-a", scheme="shopify", secret="s", event_id_header="x-shopify-webhook-id")


@pytest.fixture
def cons():
    return Consumer.objects.create(name="erp", url="http://consumer.invalid/", secret="k")


def make(src, cons, n, order="o1", ordered=False):
    if not Subscription.objects.filter(source=src, consumer=cons).exists():
        Subscription.objects.create(source=src, consumer=cons, ordered=ordered,
                                    ordering_key_json_key="order" if ordered else "")
    body = json.dumps({"order": order, "n": n}).encode()
    e = ingest_message(body, {"source": "shop-a", "received_at": T0, "quarantined": False,
                              "x-shopify-webhook-id": f"w{n}"}).event
    return Delivery.objects.get(event=e, consumer=cons)


def finish_with(d, status):
    cl = delivery.claim(d.id)
    assert cl
    return cl, delivery.finish(cl, status, "", "", None)


def kill(d, monkeypatch):
    """Drive a delivery to dead in one attempt."""
    monkeypatch.setattr(policy, "MAX_ATTEMPTS", 1)
    finish_with(d, 503)
    d.refresh_from_db()
    assert d.status == "dead"


# ---- replay -------------------------------------------------------------------------------------------------
def test_replay_a_dead_delivery_resets_it_and_audits_who(src, cons, monkeypatch):
    d = make(src, cons, 1)
    kill(d, monkeypatch)
    monkeypatch.setattr(policy, "MAX_ATTEMPTS", 14)
    r = replay.replay_delivery(d.id, actor="taha")
    assert r.status == "pending" and r.attempts == 0 and r.replayed and r.replay_count == 1
    a = AuditLog.objects.get(action="replay")
    assert a.actor == "taha" and a.old_value == "dead" and a.new_value == "pending" and a.object_id == str(d.id)


def test_replay_keeps_the_same_event_and_flags_the_attempt(src, cons, monkeypatch):
    d = make(src, cons, 1)
    kill(d, monkeypatch)
    replay.replay_delivery(d.id, actor="taha")
    cl = delivery.claim(d.id)
    assert cl.replayed is True and cl.event_id == str(d.event_id)       # SAME event id, replayed flag set
    assert cl.attempt_no == 1                                            # budget starts fresh
    assert [a.replay_round for a in DeliveryAttempt.objects.filter(delivery=d).order_by("id")] == [0, 1]


def test_replay_of_a_delivered_event_is_allowed(src, cons):
    d = make(src, cons, 1)
    finish_with(d, 200)
    assert replay.replay_delivery(d.id, actor="taha").status == "pending"


def test_cannot_replay_pending_or_in_flight(src, cons):
    d = make(src, cons, 1)
    with pytest.raises(replay.ReplayRefused):
        replay.replay_delivery(d.id, actor="taha")
    delivery.claim(d.id)
    with pytest.raises(replay.ReplayRefused):
        replay.replay_delivery(d.id, actor="taha")


def test_cannot_replay_after_retention_purged_the_body(src, cons):
    d = make(src, cons, 1)
    finish_with(d, 200)
    Event.objects.filter(pk=d.event_id).update(raw_purged_at=delivery.now())
    with pytest.raises(replay.ReplayRefused):
        replay.replay_delivery(d.id, actor="taha")


# ---- skip and ordered keys ----------------------------------------------------------------------------------
def test_skip_releases_an_ordered_key(src, cons, monkeypatch):
    a = make(src, cons, 1, ordered=True)
    b = make(src, cons, 2, ordered=True)
    kill(a, monkeypatch)
    assert delivery.claim(b.id) is None                       # held behind the dead event
    replay.skip_delivery(a.id, actor="taha", reason="bad payload")
    assert Delivery.objects.get(pk=a.id).status == "skipped"
    assert delivery.claim(b.id) is not None                   # released
    assert AuditLog.objects.filter(action="skip", actor="taha").count() == 1


def test_replaying_the_dead_head_of_a_key_holds_successors_until_done(src, cons, monkeypatch):
    a = make(src, cons, 1, ordered=True)
    b = make(src, cons, 2, ordered=True)
    kill(a, monkeypatch)
    replay.replay_delivery(a.id, actor="taha")
    assert delivery.claim(b.id) is None                       # a is pending again, so b still waits
    cl = delivery.claim(a.id)
    delivery.finish(cl, 200, "", "", None)
    assert delivery.claim(b.id) is not None


def test_cannot_skip_a_delivered_one(src, cons):
    d = make(src, cons, 1)
    finish_with(d, 200)
    with pytest.raises(replay.ReplayRefused):
        replay.skip_delivery(d.id, actor="taha")


# ---- bulk replay: dry run, filters, rate limit --------------------------------------------------------------
def test_bulk_dry_run_counts_and_changes_nothing(src, cons, monkeypatch):
    ds = [make(src, cons, i, order=f"o{i}") for i in range(1, 4)]
    for d in ds:
        kill(d, monkeypatch)
    plan = replay.plan_bulk_replay(consumer="erp", status="dead", rate_per_second=2)
    assert plan.count == 3 and plan.duration_seconds == 1.5
    assert Delivery.objects.filter(status="dead").count() == 3 and not AuditLog.objects.exists()


def test_bulk_replay_spreads_deliveries_at_the_rate(src, cons, monkeypatch):
    ds = [make(src, cons, i, order=f"o{i}") for i in range(1, 5)]
    for d in ds:
        kill(d, monkeypatch)
    plan = replay.plan_bulk_replay(consumer="erp", status="dead", rate_per_second=2)
    assert replay.execute_bulk_replay(plan, actor="taha", filters_text="consumer=erp") == 4
    times = [d.next_attempt_at for d in Delivery.objects.order_by("id")]
    gaps = [(b - a).total_seconds() for a, b in zip(times, times[1:])]
    assert all(abs(g - 0.5) < 0.01 for g in gaps)             # 2 per second
    assert AuditLog.objects.filter(action="bulk_replay", actor="taha").count() == 1
    assert AuditLog.objects.filter(action="replay").count() == 4


def test_bulk_filters_by_consumer_and_status(src, cons, monkeypatch):
    other = Consumer.objects.create(name="crm", url="http://x.invalid/", secret="k")
    Subscription.objects.create(source=src, consumer=other)      # before ingest: outbox rows exist per subscription
    a = make(src, cons, 1)
    b = Delivery.objects.get(event=a.event, consumer=other)
    kill(a, monkeypatch)
    finish_with(b, 200)
    assert replay.plan_bulk_replay(consumer="erp", status="dead").count == 1
    assert replay.plan_bulk_replay(consumer="crm", status="dead").count == 0
    assert replay.plan_bulk_replay(status="delivered").count == 1


# ---- metrics --------------------------------------------------------------------------------------------------
def test_metrics_snapshot_and_prometheus_text(src, cons, monkeypatch):
    ok, bad, queued = (make(src, cons, i, order=f"o{i}") for i in (1, 2, 3))
    finish_with(ok, 200)
    kill(bad, monkeypatch)
    snap = metrics.snapshot()
    assert snap["events_total"] == 3 and snap["queue_depth"] == 1
    assert snap["dead_by_consumer"]["erp"] == 1 and snap["success_rate_1h"]["erp"] == 0.5
    assert snap["delivery_p95_latency_seconds_15m"] is not None and snap["breaker"]["erp"] == "closed"
    text = metrics.prometheus_text(snap)
    assert 'gateway_deliveries{consumer="erp",status="delivered"} 1' in text
    assert 'gateway_dead_deliveries{consumer="erp"} 1' in text and "gateway_queue_depth 1" in text
    assert text.count("# TYPE gateway_deliveries ") == 1


def test_metrics_http_endpoints(client, src, cons):
    make(src, cons, 1)
    r = client.get("/metrics")
    assert r.status_code == 200 and b"gateway_queue_depth 1" in r.content
    assert client.get("/stats.json").json()["queue_depth"] == 1


# ---- alerts -----------------------------------------------------------------------------------------------------
def test_dead_count_alert_opens_once_and_resolves(src, cons, monkeypatch):
    ds = [make(src, cons, i, order=f"o{i}") for i in range(1, 4)]
    for d in ds:
        kill(d, monkeypatch)
    alerts.check_thresholds(dead_threshold=2, queue_age_seconds=10_000)
    alerts.check_thresholds(dead_threshold=2, queue_age_seconds=10_000)        # no duplicate
    assert Alert.objects.filter(kind="dead_count_high", resolved=False).count() == 1
    Delivery.objects.filter(pk__in=[d.pk for d in ds[:2]]).update(status="skipped")
    alerts.check_thresholds(dead_threshold=2, queue_age_seconds=10_000)
    assert Alert.objects.filter(kind="dead_count_high", resolved=False).count() == 0


def test_queue_age_alert(src, cons):
    d = make(src, cons, 1)
    Delivery.objects.filter(pk=d.pk).update(next_attempt_at=delivery.now() - timedelta(seconds=600))
    alerts.check_thresholds(dead_threshold=100, queue_age_seconds=300)
    assert Alert.objects.filter(kind="queue_age_high", resolved=False).count() == 1
    Delivery.objects.filter(pk=d.pk).update(status="delivered")
    alerts.check_thresholds(dead_threshold=100, queue_age_seconds=300)
    assert Alert.objects.filter(kind="queue_age_high", resolved=False).count() == 0


def test_alerts_are_sent_once_and_retried_on_failure():
    Alert.objects.create(kind="event_conflict", detail={"x": 1})
    seen = []
    assert alerts.notify_unsent("http://hook", sender=lambda u, p: seen.append(p) or False) == 0   # webhook down
    assert Alert.objects.filter(notified_at__isnull=True).count() == 1                       # retried next pass
    assert alerts.notify_unsent("http://hook", sender=lambda u, p: seen.append(p) or True) == 1
    assert alerts.notify_unsent("http://hook", sender=lambda u, p: seen.append(p) or True) == 0   # not twice
    assert seen[0]["kind"] == "event_conflict" and "[gateway]" in seen[0]["text"]


def test_no_webhook_configured_sends_nothing():
    Alert.objects.create(kind="x")
    assert alerts.notify_unsent("") == 0 and Alert.objects.filter(notified_at__isnull=True).count() == 1


# ---- retention --------------------------------------------------------------------------------------------------
def age(event, days):
    Event.objects.filter(pk=event.pk).update(created_at=delivery.now() - timedelta(days=days))


def test_purge_deletes_old_bodies_but_keeps_metadata(src, cons):
    d = make(src, cons, 1)
    finish_with(d, 200)
    age(d.event, 40)
    assert retention.purge_raw_bodies(30, actor="cron") == 1
    e = Event.objects.get(pk=d.event_id)
    assert bytes(e.raw_body) == b"" and e.raw_purged_at and e.provider_event_id == "w1" and e.body_sha256
    assert DeliveryAttempt.objects.filter(delivery=d).exists()                    # attempt log kept
    assert AuditLog.objects.filter(action="purge_raw_bodies", actor="cron").exists()


def test_purge_leaves_young_events(src, cons):
    d = make(src, cons, 1)
    finish_with(d, 200)
    age(d.event, 5)
    assert retention.purge_raw_bodies(30) == 0


def test_purge_never_touches_events_a_human_still_needs(src, cons, monkeypatch):
    pending, dead, failed = (make(src, cons, i, order=f"o{i}") for i in (1, 2, 3))
    kill(dead, monkeypatch)
    finish_with(failed, 400)
    for d in (pending, dead, failed):
        age(d.event, 90)
    assert retention.purge_raw_bodies(30) == 0
    assert all(bytes(Event.objects.get(pk=d.event_id).raw_body) for d in (pending, dead, failed))


def test_purge_dry_run_changes_nothing(src, cons):
    d = make(src, cons, 1)
    finish_with(d, 200)
    age(d.event, 40)
    assert retention.purge_raw_bodies(30, dry_run=True) == 1
    assert bytes(Event.objects.get(pk=d.event_id).raw_body) != b""
