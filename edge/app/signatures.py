"""Signature checks on the RAW request body. No parsing happens before verification."""
import base64
import hashlib
import hmac
from dataclasses import dataclass

REPLAY_WINDOW_SECONDS = 300


@dataclass(frozen=True)
class Verdict:
    valid: bool
    # timestamp signed by the provider (None for schemes without one)
    signed_ts: int | None = None
    # valid signature but older than the replay window -> caller quarantines
    stale: bool = False


INVALID = Verdict(valid=False)


def _hmac_sha256(secret: bytes, msg: bytes) -> bytes:
    return hmac.new(secret, msg, hashlib.sha256).digest()


def verify_stripe(secret: str, header: str | None, body: bytes, received_at: int,
                  window: int = REPLAY_WINDOW_SECONDS) -> Verdict:
    """`Stripe-Signature: t=<unix>,v1=<hex>[,v1=<hex>]`, HMAC over b"<t>." + body."""
    if not header:
        return INVALID
    ts = None
    sigs: list[str] = []
    for part in header.split(","):
        key, _, value = part.strip().partition("=")
        if key == "t":
            ts = value
        elif key == "v1":
            sigs.append(value)
    if ts is None or not ts.isdigit() or not sigs:
        return INVALID
    expected = _hmac_sha256(secret.encode(), ts.encode() + b"." + body).hex()
    if not any(hmac.compare_digest(expected, s) for s in sigs):
        return INVALID
    signed_ts = int(ts)
    # the window compares the signed timestamp with when WE received it, not with "now"
    return Verdict(valid=True, signed_ts=signed_ts, stale=abs(received_at - signed_ts) > window)


def verify_shopify(secret: str, header: str | None, body: bytes, received_at: int = 0,
                   window: int = 0) -> Verdict:
    """`X-Shopify-Hmac-Sha256: <base64>`, HMAC over the body. No timestamp, so no window."""
    if not header:
        return INVALID
    expected = base64.b64encode(_hmac_sha256(secret.encode(), body)).decode()
    return Verdict(valid=hmac.compare_digest(expected, header.strip()))


SCHEMES = {
    "stripe": ("stripe-signature", verify_stripe),
    "shopify": ("x-shopify-hmac-sha256", verify_shopify),
}
