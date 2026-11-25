"""End-to-end against the running stack: real edge, real RabbitMQ, real worker, real Postgres."""
import base64
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.request

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "gateway.settings")
django.setup()
from events.models import Alert, Event  # noqa: E402

EDGE = os.environ.get("EDGE_URL", "http://edge:8000")
CFG = json.load(open(os.environ.get("SOURCES_FILE", "/dev/sources.json")))["sources"]


def post(slug, body: bytes, headers: dict):
    req = urllib.request.Request(f"{EDGE}/in/{slug}", data=body, method="POST",
                                 headers={"content-type": "application/json", **headers})
    try:
        return urllib.request.urlopen(req, timeout=5).status
    except urllib.error.HTTPError as e:
        return e.code


def stripe_headers(body, ts, secret=CFG["stripe-a"]["secret"]):
    sig = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return {"stripe-signature": f"t={ts},v1={sig}"}


def shopify_headers(body, wid):
    mac = base64.b64encode(hmac.new(CFG["shop-a"]["secret"].encode(), body, hashlib.sha256).digest()).decode()
    return {"x-shopify-hmac-sha256": mac, "x-shopify-webhook-id": wid}


def wait_for(cond, what, timeout=15):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return
        time.sleep(0.3)
    print(f"FAIL: timed out waiting for {what}")
    sys.exit(1)


def check(cond, what):
    print(("ok   " if cond else "FAIL ") + what)
    if not cond:
        sys.exit(1)


for _ in range(60):
    try:
        urllib.request.urlopen(f"{EDGE}/healthz", timeout=2)
        break
    except Exception:
        time.sleep(1)

run = str(int(time.time()))
now = int(time.time())

# 1. valid Stripe-style event: 200, stored once
b1 = json.dumps({"id": f"evt_{run}_1", "type": "order.created"}).encode()
# a 503 while the stack is still starting is correct edge behaviour; retry until it is up
for _ in range(30):
    status = post("stripe-a", b1, stripe_headers(b1, now))
    if status != 503:
        break
    time.sleep(1)
check(status == 200, "valid event -> 200")
wait_for(lambda: Event.objects.filter(provider_event_id=f"evt_{run}_1").exists(), "event in Postgres")
check(bytes(Event.objects.get(provider_event_id=f"evt_{run}_1").raw_body) == b1, "raw body stored byte-for-byte")

# 2. same event again: still one row, duplicate counted
check(post("stripe-a", b1, stripe_headers(b1, now)) == 200, "duplicate -> 200")
wait_for(lambda: Event.objects.get(provider_event_id=f"evt_{run}_1").duplicate_count == 1, "duplicate counted")
check(Event.objects.filter(provider_event_id=f"evt_{run}_1").count() == 1, "one stored row for duplicate")

# 3. tampered body: 401, nothing stored
b3 = json.dumps({"id": f"evt_{run}_3"}).encode()
check(post("stripe-a", b3 + b" ", stripe_headers(b3, now)) == 401, "tampered body -> 401")
time.sleep(1)
check(not Event.objects.filter(provider_event_id=f"evt_{run}_3").exists(), "tampered event not stored")

# 4. stale timestamp, valid signature: accepted, quarantined, alert
b4 = json.dumps({"id": f"evt_{run}_4"}).encode()
check(post("stripe-a", b4, stripe_headers(b4, now - 3600)) == 200, "stale-but-valid -> 200")
wait_for(lambda: Event.objects.filter(provider_event_id=f"evt_{run}_4", status="quarantined").exists(), "quarantined")
check(Alert.objects.filter(kind="event_quarantined").exists(), "quarantine alert raised")

# 5. Shopify-style with webhook-id header
b5 = json.dumps({"order": run}).encode()
check(post("shop-a", b5, shopify_headers(b5, f"w-{run}")) == 200, "shopify event -> 200")
wait_for(lambda: Event.objects.filter(provider_event_id=f"w-{run}").exists(), "shopify event in Postgres")

# 6. unknown source
check(post("ghost", b1, {}) == 404, "unknown source -> 404")

print("E2E PASSED")
