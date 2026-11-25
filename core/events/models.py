import uuid

from django.db import models
from django.db.models import Q


class Source(models.Model):
    """One provider connection (e.g. one client's Shopify store)."""
    slug = models.SlugField(unique=True)
    scheme = models.CharField(max_length=20, choices=[("stripe", "stripe"), ("shopify", "shopify")])
    secret = models.CharField(max_length=200)
    # secret rotation: while this is set, a signature made with EITHER secret is accepted (no lost events)
    previous_secret = models.CharField(max_length=200, blank=True)
    # silent-source alert: override the learned "normal gap" (seconds); blank = learn it from history
    stale_after_seconds = models.PositiveIntegerField(null=True, blank=True)
    # edge rate limit for this source (requests/second and burst); blank = unlimited
    rate_per_second = models.FloatField(null=True, blank=True)
    rate_burst = models.PositiveIntegerField(null=True, blank=True)
    # where the provider puts its own event id: a header name, or a top-level JSON key
    event_id_header = models.CharField(max_length=100, blank=True)
    event_id_json_key = models.CharField(max_length=100, blank=True)
    # where the provider puts the event type (Stripe: JSON "type"; Shopify: header x-shopify-topic)
    event_type_header = models.CharField(max_length=100, blank=True)
    event_type_json_key = models.CharField(max_length=100, blank=True)
    active = models.BooleanField(default=True)

    def __str__(self):
        return self.slug


class Event(models.Model):
    class Status(models.TextChoices):
        RECEIVED = "received"
        QUARANTINED = "quarantined"   # valid signature, stale timestamp: never delivered

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)  # our stable event id
    source = models.ForeignKey(Source, on_delete=models.PROTECT)
    provider_event_id = models.CharField(max_length=200, blank=True)
    event_type = models.CharField(max_length=200, blank=True)
    body_sha256 = models.CharField(max_length=64, db_index=True)
    raw_body = models.BinaryField()          # immutable: the exact bytes the provider sent
    raw_purged_at = models.DateTimeField(null=True, blank=True)   # retention: body deleted, metadata kept
    headers = models.JSONField(default=dict)
    received_at = models.DateTimeField()     # stamped by the edge
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.RECEIVED)
    duplicate_count = models.PositiveIntegerField(default=0)
    last_seen_at = models.DateTimeField(null=True)
    created_at = models.DateTimeField(auto_now_add=True)


class DedupKey(models.Model):
    """Never deleted. 'id' keys are unique forever; 'hash' keys only match within the window."""
    source = models.ForeignKey(Source, on_delete=models.PROTECT)
    kind = models.CharField(max_length=4, choices=[("id", "id"), ("hash", "hash")])
    key = models.CharField(max_length=200)
    event = models.ForeignKey(Event, on_delete=models.PROTECT)
    first_seen_at = models.DateTimeField()

    class Meta:
        constraints = [models.UniqueConstraint(fields=["source", "key"], condition=Q(kind="id"),
                                               name="uniq_source_provider_event_id")]
        indexes = [models.Index(fields=["source", "kind", "key"])]


class EventConflict(models.Model):
    """Same provider event id arrived again with a DIFFERENT body."""
    original = models.ForeignKey(Event, on_delete=models.PROTECT, related_name="conflicts")
    raw_body = models.BinaryField()
    body_sha256 = models.CharField(max_length=64)
    received_at = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)


class Alert(models.Model):
    """Something a human must look at."""
    kind = models.CharField(max_length=40)
    detail = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)
    resolved = models.BooleanField(default=False)
    notified_at = models.DateTimeField(null=True, blank=True)   # sent to the alert webhook


def default_breaker_cooldown() -> float:
    """The policy owns this number; a hard-coded copy here silently drifted from it (and from the scaled test value)."""
    from . import delivery_policy
    return delivery_policy.BREAKER_COOLDOWN


class Consumer(models.Model):
    """Somewhere we deliver events to. Circuit-breaker state lives here so every worker agrees."""
    name = models.SlugField(unique=True)
    url = models.URLField()
    secret = models.CharField(max_length=200)           # signs what we send (X-Gateway-Signature)
    timeout_seconds = models.PositiveIntegerField(default=10)
    max_in_flight = models.PositiveIntegerField(default=4)   # isolation: a slow consumer cannot hog workers
    active = models.BooleanField(default=True)
    breaker_state = models.CharField(max_length=10, default="closed")
    breaker_failures = models.PositiveIntegerField(default=0)
    breaker_opened_at = models.FloatField(default=0.0)
    breaker_cooldown = models.FloatField(default=default_breaker_cooldown)

    def __str__(self):
        return self.name


class Subscription(models.Model):
    """source + event type pattern -> consumer."""
    source = models.ForeignKey(Source, on_delete=models.CASCADE)
    consumer = models.ForeignKey(Consumer, on_delete=models.CASCADE)
    event_type_pattern = models.CharField(max_length=200, default="*")   # fnmatch, e.g. "orders/*"
    # opt-in per-key ordering (e.g. one Shopify order id): events of one key go out one at a time, in order
    ordered = models.BooleanField(default=False)
    ordering_key_json_key = models.CharField(max_length=100, blank=True)  # top-level JSON key holding the key
    active = models.BooleanField(default=True)


class Delivery(models.Model):
    """One row per (event, consumer): the transactional outbox. Written in the same transaction as the event."""
    class Status(models.TextChoices):
        PENDING = "pending"
        IN_FLIGHT = "in_flight"
        DELIVERED = "delivered"
        FAILED = "failed"        # the consumer said 4xx: its bug, no retry
        DEAD = "dead"            # retries exhausted; kept forever, replayable
        SKIPPED = "skipped"      # a human decided this one will not be delivered (releases an ordered key)

    event = models.ForeignKey(Event, on_delete=models.PROTECT, related_name="deliveries")
    consumer = models.ForeignKey(Consumer, on_delete=models.PROTECT)
    status = models.CharField(max_length=10, choices=Status.choices, default=Status.PENDING, db_index=True)
    attempts = models.PositiveIntegerField(default=0)
    next_attempt_at = models.DateTimeField(db_index=True)
    enqueued_at = models.DateTimeField(null=True)
    in_flight_since = models.DateTimeField(null=True)
    ordering_key = models.CharField(max_length=200, blank=True, db_index=True)
    replayed = models.BooleanField(default=False)
    replay_count = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    delivered_at = models.DateTimeField(null=True)

    class Meta:
        constraints = [models.UniqueConstraint(fields=["event", "consumer"], name="uniq_event_consumer")]


class DeliveryAttempt(models.Model):
    """Written when an attempt STARTS, completed when it ends. A crash leaves status_code null: still visible."""
    delivery = models.ForeignKey(Delivery, on_delete=models.CASCADE, related_name="attempt_log")
    replay_round = models.PositiveIntegerField(default=0)   # 0 = first life, 1 = after the first replay ...
    number = models.PositiveIntegerField()
    started_at = models.DateTimeField()
    finished_at = models.DateTimeField(null=True)
    status_code = models.IntegerField(null=True)
    error = models.CharField(max_length=300, blank=True)
    response_body = models.TextField(blank=True)         # truncated


class AuditLog(models.Model):
    """Every manual action: who, when, what, old value, new value. Never edited, never deleted."""
    actor = models.CharField(max_length=150)
    action = models.CharField(max_length=40)          # replay, skip, bulk_replay, purge_raw_bodies ...
    object_type = models.CharField(max_length=40, blank=True)
    object_id = models.CharField(max_length=64, blank=True)
    old_value = models.CharField(max_length=200, blank=True)
    new_value = models.CharField(max_length=200, blank=True)
    detail = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)


class EventCorrection(models.Model):
    """A human fix to a held event. The raw body is NEVER edited: delivery reads raw + corrections.
    `patch` is a JSON object merged over the raw body's top-level keys, in the order corrections were made."""
    event = models.ForeignKey(Event, on_delete=models.PROTECT, related_name="corrections")
    patch = models.JSONField()
    reason = models.CharField(max_length=300, blank=True)
    actor = models.CharField(max_length=150)
    created_at = models.DateTimeField(auto_now_add=True)


class Control(models.Model):
    """Small operator switches shared by every process (e.g. ingest_paused). Changes are audited by the caller."""
    key = models.CharField(max_length=60, primary_key=True)
    value = models.CharField(max_length=200, blank=True)
    updated_at = models.DateTimeField(auto_now=True)
