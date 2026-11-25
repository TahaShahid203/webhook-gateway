"""Raw layer is immutable. A delivery sends raw + corrections: the raw JSON body with each correction's patch
merged over its top-level keys (in order), re-serialised. With no corrections the ORIGINAL BYTES go out untouched."""
import json

from .models import Event


def effective_body(event: Event) -> tuple[bytes, bool]:
    corrections = list(event.corrections.order_by("id").values_list("patch", flat=True))
    raw = bytes(event.raw_body)
    if not corrections:
        return raw, False
    body = json.loads(raw)
    for patch in corrections:
        body.update(patch)
    return json.dumps(body, separators=(",", ":"), sort_keys=True).encode(), True


def add_correction(event: Event, patch: dict, actor: str, reason: str = ""):
    from .audit import audit
    from .models import EventCorrection
    if not isinstance(patch, dict):
        raise ValueError("a correction must be a JSON object")
    if event.raw_purged_at:
        raise ValueError("raw body was purged by retention; nothing left to correct")
    if not isinstance(json.loads(bytes(event.raw_body)), dict):
        raise ValueError("only JSON-object bodies can be corrected")
    c = EventCorrection.objects.create(event=event, patch=patch, reason=reason, actor=actor)
    audit(actor, "correct_event", "event", event.id, "", json.dumps(patch)[:190], reason=reason)
    return c
