import json
from datetime import timedelta

import pytest

from events.ingest import CONFLICT, DUPLICATE, STORED, UNKNOWN_SOURCE, ingest_message
from events.models import Alert, DedupKey, Event, EventConflict, Source

T0 = 1_800_000_000
pytestmark = pytest.mark.django_db


@pytest.fixture
def stripe_src():
    return Source.objects.create(slug="stripe-a", scheme="stripe", secret="s", event_id_json_key="id")


@pytest.fixture
def shop_src():
    return Source.objects.create(slug="shop-a", scheme="shopify", secret="s",
                                 event_id_header="x-shopify-webhook-id")


def msg(source, ts=T0, quarantined=False, **extra):
    return {"source": source, "received_at": ts, "quarantined": quarantined, **extra}


def body(**kw):
    return json.dumps({"id": "evt_1", "n": 1, **kw}).encode()


def test_new_event_is_stored_with_raw_body_and_id_key(stripe_src):
    r = ingest_message(body(), msg("stripe-a"))
    assert r.outcome == STORED
    assert bytes(r.event.raw_body) == body() and r.event.provider_event_id == "evt_1"
    assert DedupKey.objects.get().kind == "id"


def test_same_id_same_body_is_dropped_and_counted(stripe_src):
    first = ingest_message(body(), msg("stripe-a"))
    again = ingest_message(body(), msg("stripe-a", ts=T0 + 5))
    assert again.outcome == DUPLICATE and Event.objects.count() == 1
    first.event.refresh_from_db()
    assert first.event.duplicate_count == 1 and first.event.last_seen_at is not None


def test_id_dedup_matches_forever(stripe_src):
    ingest_message(body(), msg("stripe-a"))
    late = ingest_message(body(), msg("stripe-a", ts=T0 + 30 * 24 * 3600))
    assert late.outcome == DUPLICATE and Event.objects.count() == 1


def test_same_id_different_body_is_a_conflict_with_alert(stripe_src):
    ingest_message(body(), msg("stripe-a"))
    r = ingest_message(body(n=2), msg("stripe-a", ts=T0 + 1))
    assert r.outcome == CONFLICT and Event.objects.count() == 1
    assert EventConflict.objects.count() == 1
    assert Alert.objects.filter(kind="event_conflict").count() == 1


def test_no_id_falls_back_to_body_hash(shop_src):
    ingest_message(b'{"order":1}', msg("shop-a"))               # no webhook-id header
    r = ingest_message(b'{"order":1}', msg("shop-a", ts=T0 + 60))
    assert r.outcome == DUPLICATE and DedupKey.objects.get().kind == "hash"


def test_identical_body_after_24h_is_a_new_event(shop_src):
    ingest_message(b'{"order":1}', msg("shop-a"))
    r = ingest_message(b'{"order":1}', msg("shop-a", ts=T0 + 24 * 3600 + 1))
    assert r.outcome == STORED and Event.objects.count() == 2
    assert DedupKey.objects.count() == 2                         # keys are kept forever


def test_identical_body_just_inside_24h_is_a_duplicate(shop_src):
    ingest_message(b'{"order":1}', msg("shop-a"))
    r = ingest_message(b'{"order":1}', msg("shop-a", ts=T0 + 24 * 3600))
    assert r.outcome == DUPLICATE


def test_header_id_is_used_when_present(shop_src):
    r = ingest_message(b'{"order":1}', msg("shop-a", **{"x-shopify-webhook-id": "w-9"}))
    assert r.event.provider_event_id == "w-9" and DedupKey.objects.get().kind == "id"


def test_quarantined_event_is_stored_flagged_and_alerted(stripe_src):
    r = ingest_message(body(), msg("stripe-a", quarantined=True))
    assert r.outcome == STORED and r.event.status == Event.Status.QUARANTINED
    assert Alert.objects.filter(kind="event_quarantined").count() == 1
    # a provider retry of the same quarantined event does not raise a second alert
    ingest_message(body(), msg("stripe-a", ts=T0 + 5, quarantined=True))
    assert Alert.objects.filter(kind="event_quarantined").count() == 1


def test_unknown_source_alerts_and_stores_nothing(db):
    r = ingest_message(b"{}", msg("ghost"))
    assert r.outcome == UNKNOWN_SOURCE and Event.objects.count() == 0
    assert Alert.objects.filter(kind="unknown_source").count() == 1


def test_sources_dedup_independently(stripe_src):
    Source.objects.create(slug="stripe-b", scheme="stripe", secret="s", event_id_json_key="id")
    ingest_message(body(), msg("stripe-a"))
    r = ingest_message(body(), msg("stripe-b"))
    assert r.outcome == STORED and Event.objects.count() == 2


def test_millisecond_receive_time_is_used_when_the_edge_sends_it(stripe_src):
    r = ingest_message(body(), {**msg("stripe-a"), "received_ms": T0 * 1000 + 250})
    assert r.event.received_at.timestamp() == T0 + 0.25
