import base64
import hashlib
import hmac

from app.signatures import verify_shopify, verify_stripe

SECRET = "whsec_test"
BODY = b'{"id":"evt_1","type":"order.created"}'
NOW = 1_800_000_000


def stripe_header(ts: int, body: bytes = BODY, secret: str = SECRET) -> str:
    sig = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return f"t={ts},v1={sig}"


def shopify_header(body: bytes = BODY, secret: str = SECRET) -> str:
    return base64.b64encode(hmac.new(secret.encode(), body, hashlib.sha256).digest()).decode()


def test_stripe_valid():
    v = verify_stripe(SECRET, stripe_header(NOW), BODY, NOW)
    assert v.valid and not v.stale and v.signed_ts == NOW


def test_stripe_tampered_body():
    v = verify_stripe(SECRET, stripe_header(NOW), BODY + b" ", NOW)
    assert not v.valid


def test_stripe_wrong_secret():
    assert not verify_stripe("other", stripe_header(NOW), BODY, NOW).valid


def test_stripe_missing_or_garbage_header():
    assert not verify_stripe(SECRET, None, BODY, NOW).valid
    assert not verify_stripe(SECRET, "nonsense", BODY, NOW).valid
    assert not verify_stripe(SECRET, "t=abc,v1=00", BODY, NOW).valid


def test_stripe_stale_but_valid_is_flagged_not_invalid():
    old = NOW - 301
    v = verify_stripe(SECRET, stripe_header(old), BODY, NOW)
    assert v.valid and v.stale


def test_stripe_edge_of_window_is_fresh():
    assert not verify_stripe(SECRET, stripe_header(NOW - 300), BODY, NOW).stale


def test_stripe_window_uses_received_at_not_now():
    # event signed long ago but received (queued) in time: stays fresh even if we process it later
    received = NOW
    v = verify_stripe(SECRET, stripe_header(NOW - 10), BODY, received)
    assert not v.stale


def test_stripe_second_signature_accepted():
    good = stripe_header(NOW)
    header = good + ",v1=" + "0" * 64
    assert verify_stripe(SECRET, header, BODY, NOW).valid


def test_shopify_valid_and_tampered():
    assert verify_shopify(SECRET, shopify_header(), BODY).valid
    assert not verify_shopify(SECRET, shopify_header(), BODY + b"x").valid
    assert not verify_shopify(SECRET, None, BODY).valid
