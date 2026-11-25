"""Ingest: turn one queued message from the edge into a stored Event, applying the dedup rules.

Rules (docs/DECISIONS.md):
- key = provider event id if the provider sends one, else sha256(raw body)
- id keys match forever; hash keys only match if the earlier copy arrived within 24 h
- same id + same body: drop, bump duplicate_count
- same id + different body: EventConflict + Alert
- stale-but-valid-signature (quarantined) events are stored, flagged and alerted, never delivered
"""
import fnmatch
import hashlib
import json
import zlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from django.conf import settings
from django.db import connection, transaction

from .models import Alert, DedupKey, Delivery, Event, EventConflict, Source, Subscription

STORED, DUPLICATE, CONFLICT, UNKNOWN_SOURCE = "stored", "duplicate", "conflict", "unknown_source"


@dataclass
class IngestResult:
    outcome: str
    event: Event | None = None


def _provider_event_id(source: Source, body: bytes, headers: dict) -> str:
    if source.event_id_header:
        value = headers.get(source.event_id_header.lower())
        if value:
            return str(value)
    if source.event_id_json_key:
        try:
            value = json.loads(body).get(source.event_id_json_key)
        except (ValueError, AttributeError):
            value = None
        if value:
            return str(value)
    return ""


def _json_value(body: bytes, key: str) -> str:
    try:
        value = json.loads(body).get(key)
    except (ValueError, AttributeError):
        return ""
    return str(value) if value not in (None, "") else ""


def _event_type(source: Source, body: bytes, headers: dict) -> str:
    if source.event_type_header and headers.get(source.event_type_header.lower()):
        return str(headers[source.event_type_header.lower()])
    if source.event_type_json_key:
        return _json_value(body, source.event_type_json_key)
    return ""


def _create_deliveries(event: Event, body: bytes) -> None:
    """Transactional outbox: the delivery rows commit together with the event, or not at all."""
    for sub in Subscription.objects.filter(source=event.source, active=True, consumer__active=True):
        if not fnmatch.fnmatch(event.event_type or "", sub.event_type_pattern) and sub.event_type_pattern != "*":
            continue
        key = _json_value(body, sub.ordering_key_json_key) if sub.ordered and sub.ordering_key_json_key else ""
        Delivery.objects.create(event=event, consumer=sub.consumer, next_attempt_at=datetime.now(timezone.utc),
                                ordering_key=key)


def _lock(source_id: int, key: str) -> None:
    """Serialize concurrent ingests of the same key (two workers, same event, same instant)."""
    lock_id = zlib.crc32(f"{source_id}:{key}".encode())
    with connection.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(%s)", [lock_id])


def ingest_message(body: bytes, headers: dict) -> IngestResult:
    headers = {k.lower(): v for k, v in headers.items()}
    try:
        source = Source.objects.get(slug=headers.get("source"), active=True)
    except Source.DoesNotExist:
        Alert.objects.create(kind="unknown_source", detail={"source": headers.get("source")})
        return IngestResult(UNKNOWN_SOURCE)

    # the edge sends milliseconds too; whole seconds alone made every latency we measured wrong by a random 0-1 s
    ms = headers.get("received_ms")
    received_at = (datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc) if ms
                   else datetime.fromtimestamp(int(headers["received_at"]), tz=timezone.utc))
    sha = hashlib.sha256(body).hexdigest()
    provider_id = _provider_event_id(source, body, headers)
    kind, key = ("id", provider_id) if provider_id else ("hash", sha)
    window = timedelta(seconds=settings.HASH_DEDUP_WINDOW_SECONDS)

    with transaction.atomic():
        _lock(source.id, key)

        existing = (DedupKey.objects.select_related("event")
                    .filter(source=source, kind=kind, key=key).order_by("-first_seen_at").first())
        if existing and kind == "hash" and received_at - existing.first_seen_at > window:
            existing = None   # an identical body more than 24 h later is a new event

        if existing:
            original = existing.event
            if original.body_sha256 == sha:
                original.duplicate_count += 1
                original.last_seen_at = received_at
                original.save(update_fields=["duplicate_count", "last_seen_at"])
                return IngestResult(DUPLICATE, original)
            EventConflict.objects.create(original=original, raw_body=body, body_sha256=sha,
                                         received_at=received_at)
            Alert.objects.create(kind="event_conflict", detail={
                "source": source.slug, "provider_event_id": provider_id, "event": str(original.id)})
            return IngestResult(CONFLICT, original)

        quarantined = str(headers.get("quarantined")).lower() == "true"
        event = Event.objects.create(
            source=source, provider_event_id=provider_id, body_sha256=sha, raw_body=body,
            event_type=_event_type(source, body, headers),
            headers={k: v for k, v in headers.items() if k not in ("source", "received_at", "received_ms", "quarantined")},
            received_at=received_at,
            status=Event.Status.QUARANTINED if quarantined else Event.Status.RECEIVED)
        DedupKey.objects.create(source=source, kind=kind, key=key, event=event, first_seen_at=received_at)
        if quarantined:
            Alert.objects.create(kind="event_quarantined", detail={
                "source": source.slug, "event": str(event.id), "reason": "stale timestamp, valid signature"})
        else:
            _create_deliveries(event, body)       # quarantined events are never delivered
        return IngestResult(STORED, event)
