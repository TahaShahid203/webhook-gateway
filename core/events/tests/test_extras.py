import json
from datetime import datetime, timedelta, timezone

import pytest

from events import alerts, corrections, delivery, explain, sources_export
from events.ingest import ingest_message
from events.models import (Alert, AuditLog, Consumer, Delivery, Event, EventCorrection, Source, Subscription)

T0 = 1_800_000_000
pytestmark = pytest.mark.django_db


@pytest.fixture
def src():
    return Source.objects.create(slug="s1", scheme="stripe", secret="s", event_id_json_key="id")


@pytest.fixture
def cons():
    return Consumer.objects.create(name="erp", url="http://x.invalid/", secret="k")


def ev(src, n, body=None, ts=T0):
    b = body or json.dumps({"id": f"e{n}", "amount": 100, "email": "typo@exmaple.com"}).encode()
    return ingest_message(b, {"source": src.slug, "received_at": ts, "quarantined": False}).event


# ---- corrections: raw layer never edited ------------------------------------------------------------------------
def test_no_correction_sends_the_original_bytes_untouched(src):
    e = ev(src, 1, body=b'{"id":"e1",   "n":1}')                      # odd spacing must survive
    body, corrected = corrections.effective_body(e)
    assert body == b'{"id":"e1",   "n":1}' and corrected is False


def test_correction_is_merged_at_delivery_and_raw_stays_immutable(src, cons):
    Subscription.objects.create(source=src, consumer=cons)
    e = ev(src, 1)
    raw_before = bytes(e.raw_body)
    corrections.add_correction(e, {"email": "fixed@example.com"}, actor="taha", reason="typo")
    e.refresh_from_db()
    assert bytes(e.raw_body) == raw_before                              # the raw body is untouched
    cl = delivery.claim(Delivery.objects.get(event=e).id)
    sent = json.loads(cl.body)
    assert sent["email"] == "fixed@example.com" and sent["amount"] == 100 and cl.corrected is True
    assert AuditLog.objects.filter(action="correct_event", actor="taha").exists()


def test_corrections_apply_in_order(src):
    e = ev(src, 1)
    corrections.add_correction(e, {"amount": 1}, "a")
    corrections.add_correction(e, {"amount": 2, "x": True}, "a")
    body, corrected = corrections.effective_body(e)
    assert json.loads(body) == {"id": "e1", "amount": 2, "email": "typo@exmaple.com", "x": True}


def test_cannot_correct_a_purged_or_non_object_body(src):
    e = ev(src, 1, body=b"[1,2]")
    with pytest.raises(ValueError):
        corrections.add_correction(e, {"a": 1}, "a")
    e2 = ev(src, 2)
    Event.objects.filter(pk=e2.pk).update(raw_purged_at=delivery.now())
    e2.refresh_from_db()
    with pytest.raises(ValueError):
        corrections.add_correction(e2, {"a": 1}, "a")


# ---- explain the event ----------------------------------------------------------------------------------------
def test_explain_event_tells_the_whole_story(src, cons, monkeypatch):
    from events import delivery_policy as policy
    Subscription.objects.create(source=src, consumer=cons)
    e = ev(src, 1)
    ev(src, 1)                                                          # a duplicate
    d = Delivery.objects.get(event=e)
    cl = delivery.claim(d.id)
    delivery.finish(cl, 503, "", "", None)
    text = "\n".join(explain.explain_event(Event.objects.get(pk=e.pk)))
    assert "received" in text and "signature valid" in text and "seen again 1x" in text
    assert "queued for consumer 'erp'" in text and "attempt 1" in text and "answered 503" in text
    assert "waiting to retry" in text


# ---- silent source alert ------------------------------------------------------------------------------------------
def test_source_silent_alert_uses_the_manual_override(src):
    for i in range(3):
        ev(src, i, ts=T0 + i)
    Source.objects.filter(pk=src.pk).update(stale_after_seconds=60)
    now = datetime.fromtimestamp(T0 + 2 + 30, tz=timezone.utc)
    alerts.check_silent_sources(now)
    assert not Alert.objects.filter(kind="source_silent").exists()      # 30 s quiet, limit 60
    alerts.check_silent_sources(now + timedelta(seconds=120))
    assert Alert.objects.filter(kind="source_silent", resolved=False).count() == 1
    alerts.check_silent_sources(now + timedelta(seconds=130))           # not duplicated
    assert Alert.objects.filter(kind="source_silent").count() == 1
    ev(src, 99, ts=T0 + 200)                                            # it speaks again
    alerts.check_silent_sources(datetime.fromtimestamp(T0 + 205, tz=timezone.utc))
    assert Alert.objects.filter(kind="source_silent", resolved=False).count() == 0


def test_learned_threshold_is_per_hour_of_day(src):
    # busy hour (an event every 2 s) vs quiet night hour (one every 600 s): different "normal"
    base = datetime(2026, 11, 20, 14, 0, 0, tzinfo=timezone.utc)                        # 14:00 busy
    night = datetime(2026, 11, 21, 3, 0, 0, tzinfo=timezone.utc)                        # 03:00 quiet
    for i in range(20):
        ev(src, f"d{i}", ts=int((base + timedelta(seconds=2 * i)).timestamp()))
    for i in range(20):
        ev(src, f"n{i}", ts=int((night + timedelta(seconds=600 * i)).timestamp()))
    busy_limit = alerts.learned_gap_threshold(src, base.replace(hour=14, minute=30))
    quiet_limit = alerts.learned_gap_threshold(src, night.replace(hour=3, minute=30))
    assert busy_limit < 100 and quiet_limit > 1000


def test_not_enough_history_means_no_alert(src):
    ev(src, 1)
    assert alerts.learned_gap_threshold(src, datetime.now(timezone.utc)) is None


# ---- sources export to the edge ---------------------------------------------------------------------------
def test_saving_a_source_rewrites_the_edge_file(settings, tmp_path):
    settings.EDGE_SOURCES_FILE = str(tmp_path / "sources.json")
    Source.objects.create(slug="a", scheme="stripe", secret="new", previous_secret="old", rate_per_second=5, rate_burst=10)
    data = json.load(open(settings.EDGE_SOURCES_FILE))["sources"]["a"]
    assert data["secret"] == "new" and data["previous_secret"] == "old" and data["rate_per_second"] == 5
    Source.objects.filter(slug="a").first().delete()
    assert json.load(open(settings.EDGE_SOURCES_FILE))["sources"] == {}


def test_inactive_sources_are_not_exported(settings, tmp_path):
    settings.EDGE_SOURCES_FILE = str(tmp_path / "sources.json")
    Source.objects.create(slug="off", scheme="stripe", secret="x", active=False)
    assert "off" not in json.load(open(settings.EDGE_SOURCES_FILE))["sources"]
