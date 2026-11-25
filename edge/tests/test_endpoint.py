import json

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.publisher import PublishError
from app.rejection_log import RejectionLog
from app.sources import SourceStore
from tests.test_signatures import SECRET, shopify_header, stripe_header

NOW = 1_800_000_000
BODY = b'{"id":"evt_1","type":"order.created"}'


class FakePublisher:
    def __init__(self, fail=False):
        self.fail = fail
        self.sent = []

    async def publish(self, body, headers, queue="ingest"):
        if self.fail:
            raise PublishError("broker down")
        self.sent.append((queue, body, headers))


@pytest.fixture
def env(tmp_path):
    f = tmp_path / "sources.json"
    f.write_text(json.dumps({"sources": {
        "stripe-a": {"scheme": "stripe", "secret": SECRET},
        "shop-a": {"scheme": "shopify", "secret": SECRET}}}))
    pub = FakePublisher()
    rej = RejectionLog(str(tmp_path / "rej.log"), cap_bytes=10_000)
    app = create_app(SourceStore(str(f)), pub, rej, now=lambda: NOW)
    return TestClient(app), pub, rej


JSON = {"content-type": "application/json"}


def post(client, slug, body=BODY, **headers):
    return client.post(f"/in/{slug}", content=body, headers={**JSON, **headers})


def test_valid_event_is_queued_with_raw_body_and_200(env):
    client, pub, _ = env
    r = post(client, "stripe-a", **{"stripe-signature": stripe_header(NOW)})
    assert r.status_code == 200
    queue, body, headers = pub.sent[0]
    assert queue == "ingest" and body == BODY          # raw bytes untouched
    assert headers["source"] == "stripe-a" and headers["received_at"] == NOW
    assert headers["received_ms"] == NOW * 1000                          # millisecond receive time travels with it
    assert headers["quarantined"] is False


def test_unknown_source_404_nothing_queued(env):
    client, pub, _ = env
    assert post(client, "nope").status_code == 404 and not pub.sent


def test_tampered_body_401_nothing_queued_and_logged(env):
    client, pub, rej = env
    r = post(client, "stripe-a", body=BODY + b" ", **{"stripe-signature": stripe_header(NOW)})
    assert r.status_code == 401 and not pub.sent and rej.total == 1


def test_stale_but_valid_is_queued_as_quarantined(env):
    client, pub, _ = env
    r = post(client, "stripe-a", **{"stripe-signature": stripe_header(NOW - 3600)})
    assert r.status_code == 200
    assert pub.sent[0][2]["quarantined"] is True


def test_shopify_valid(env):
    client, pub, _ = env
    r = post(client, "shop-a", **{"x-shopify-hmac-sha256": shopify_header(),
                                  "x-shopify-webhook-id": "w-1"})
    assert r.status_code == 200 and pub.sent[0][2]["x-shopify-webhook-id"] == "w-1"


def test_wrong_content_type_415(env):
    client, pub, _ = env
    r = client.post("/in/stripe-a", content=BODY, headers={"content-type": "text/plain",
                    "stripe-signature": stripe_header(NOW)})
    assert r.status_code == 415 and not pub.sent


def test_too_large_413(env, monkeypatch):
    import app.main as m
    monkeypatch.setattr(m, "MAX_BODY_BYTES", 10)
    client, pub, _ = env
    assert post(client, "stripe-a", **{"stripe-signature": stripe_header(NOW)}).status_code == 413


def test_broker_down_returns_503_never_200(env):
    client, pub, _ = env
    pub.fail = True
    r = post(client, "stripe-a", **{"stripe-signature": stripe_header(NOW)})
    assert r.status_code == 503


# ---- rotation, rate limit, activity feed, broker choice -------------------------------------------------------
def make_env(tmp_path, cfg, publishers=None, **kw):
    f = tmp_path / "sources.json"
    f.write_text(json.dumps({"sources": cfg}))
    pub = FakePublisher()
    rej = RejectionLog(str(tmp_path / "rej.log"), cap_bytes=10_000)
    clock = {"t": NOW}
    app = create_app(SourceStore(str(f)), pub, rej, now=lambda: clock["t"], publishers=publishers, **kw)
    return TestClient(app), pub, clock, f


def test_both_secrets_work_during_rotation(tmp_path):
    client, pub, _, _ = make_env(tmp_path, {"s": {"scheme": "stripe", "secret": "new", "previous_secret": "old"}})
    assert post(client, "s", **{"stripe-signature": stripe_header(NOW, secret="old")}).status_code == 200
    assert post(client, "s", **{"stripe-signature": stripe_header(NOW, secret="new")}).status_code == 200
    assert post(client, "s", **{"stripe-signature": stripe_header(NOW, secret="third")}).status_code == 401


def test_rotation_reaches_the_edge_without_a_restart(tmp_path):
    client, pub, _, f = make_env(tmp_path, {"s": {"scheme": "stripe", "secret": "old"}})
    assert post(client, "s", **{"stripe-signature": stripe_header(NOW, secret="new")}).status_code == 401
    import os, time as _t
    f.write_text(json.dumps({"sources": {"s": {"scheme": "stripe", "secret": "new", "previous_secret": "old"}}}))
    os.utime(f, (_t.time() + 5, _t.time() + 5))                        # a new mtime, as a real rewrite has
    assert post(client, "s", **{"stripe-signature": stripe_header(NOW, secret="new")}).status_code == 200
    assert post(client, "s", **{"stripe-signature": stripe_header(NOW, secret="old")}).status_code == 200


def test_rate_limit_returns_429_with_retry_after_then_recovers(tmp_path):
    client, pub, clock, _ = make_env(tmp_path, {"s": {"scheme": "stripe", "secret": SECRET,
                                                      "rate_per_second": 2, "rate_burst": 2}})
    h = lambda: {"stripe-signature": stripe_header(NOW)}
    codes = [post(client, "s", **h()).status_code for _ in range(4)]
    assert codes == [200, 200, 429, 429]
    r = post(client, "s", **h())
    assert r.status_code == 429 and r.headers["retry-after"] == "1"
    clock["t"] += 1.0                                                   # a second later: two tokens again
    assert [post(client, "s", **h()).status_code for _ in range(3)] == [200, 200, 429]
    assert len(pub.sent) == 4                                           # rate-limited requests are not queued


def test_no_rate_limit_configured_means_unlimited(tmp_path):
    client, pub, _, _ = make_env(tmp_path, {"s": {"scheme": "stripe", "secret": SECRET}})
    assert all(post(client, "s", **{"stripe-signature": stripe_header(NOW)}).status_code == 200 for _ in range(50))


def test_recent_feed_reports_what_happened(tmp_path):
    client, pub, _, _ = make_env(tmp_path, {"s": {"scheme": "stripe", "secret": SECRET}})
    post(client, "s", **{"stripe-signature": stripe_header(NOW)})
    post(client, "s", **{"stripe-signature": "t=1,v1=00"})
    post(client, "nope")
    feed = client.get("/_recent").json()
    assert [i["outcome"] for i in feed["items"]] == ["accepted", "rejected", "unknown_source"]
    assert feed["counters"] == {"accepted": 1, "rejected": 1, "unknown_source": 1}
    assert [i["outcome"] for i in client.get(f"/_recent?since={feed['items'][1]['seq']}").json()["items"]] == ["unknown_source"]


def test_broker_choice_only_honoured_when_override_is_allowed(tmp_path):
    a, b = FakePublisher(), FakePublisher()
    cfg = {"s": {"scheme": "stripe", "secret": SECRET}}
    client, _, _, _ = make_env(tmp_path, cfg, publishers={"rabbitmq": a, "redis": b}, allow_override=False)
    post(client, "s", **{"stripe-signature": stripe_header(NOW), "x-gateway-broker": "redis"})
    assert len(a.sent) == 1 and not b.sent                              # ignored: not allowed
    a2, b2 = FakePublisher(), FakePublisher()
    client, _, _, _ = make_env(tmp_path, cfg, publishers={"rabbitmq": a2, "redis": b2}, allow_override=True)
    post(client, "s", **{"stripe-signature": stripe_header(NOW), "x-gateway-broker": "redis"})
    post(client, "s", **{"stripe-signature": stripe_header(NOW)})
    assert len(b2.sent) == 1 and len(a2.sent) == 1
    assert [i["broker"] for i in client.get("/_recent").json()["items"]] == ["redis", "rabbitmq"]


def test_redis_down_returns_503(tmp_path):
    class Down(FakePublisher):
        pass
    d = Down(fail=True)
    client, _, _, _ = make_env(tmp_path, {"s": {"scheme": "stripe", "secret": SECRET}},
                               publishers={"rabbitmq": FakePublisher(), "redis": d}, allow_override=True)
    r = post(client, "s", **{"stripe-signature": stripe_header(NOW), "x-gateway-broker": "redis"})
    assert r.status_code == 503


def test_recent_feed_echoes_the_dev_tag(tmp_path):
    client, pub, _, _ = make_env(tmp_path, {"s": {"scheme": "stripe", "secret": SECRET}})
    post(client, "s", **{"stripe-signature": stripe_header(NOW), "x-play-tag": "ui-1"})
    post(client, "s", **{"stripe-signature": "t=1,v1=00", "x-play-tag": "ui-2"})
    post(client, "s", **{"stripe-signature": stripe_header(NOW)})
    assert [i["tag"] for i in client.get("/_recent").json()["items"]] == ["ui-1", "ui-2", None]
