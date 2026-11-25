"""Explain-the-event: one plain-words story of an event's whole life, from the database alone."""
from .models import Alert, AuditLog, Delivery, DeliveryAttempt, Event


def explain_event(event: Event) -> list[str]:
    t = lambda x: x.strftime("%H:%M:%S") if x else "?"
    lines = [f"received {t(event.received_at)} from source '{event.source.slug}'"
             + (f" ({event.event_type})" if event.event_type else "")]
    if event.status == Event.Status.QUARANTINED:
        lines.append("signature valid, but the timestamp was outside the replay window: held (quarantined), "
                     "never delivered, a human was alerted")
    else:
        lines.append("signature valid")
    if event.provider_event_id:
        lines.append(f"provider event id {event.provider_event_id}: not seen before, stored")
    else:
        lines.append("no provider event id: identified by a hash of the body")
    if event.duplicate_count:
        lines.append(f"seen again {event.duplicate_count}x (last {t(event.last_seen_at)}): repeats were dropped, "
                     "nothing was delivered twice because of them")
    for c in event.conflicts.all():
        lines.append(f"{t(c.received_at)}: the same id arrived with a DIFFERENT body: kept in conflicts, alert raised")
    for corr in event.corrections.order_by("id"):
        lines.append(f"{t(corr.created_at)}: {corr.actor} corrected {sorted(corr.patch)} ({corr.reason or 'no reason'}); "
                     "the raw body was not changed")
    for d in event.deliveries.select_related("consumer").order_by("id"):
        lines.append(f"queued for consumer '{d.consumer.name}'" + (" (ordered by key " + d.ordering_key + ")"
                                                                  if d.ordering_key else ""))
        for a in DeliveryAttempt.objects.filter(delivery=d).order_by("id"):
            tag = f"replay #{a.replay_round} " if a.replay_round else ""
            if a.finished_at is None:
                res = "no result recorded (the worker died or is still waiting)"
            elif a.status_code is None:
                res = f"no answer ({a.error[:60]})"
            else:
                res = f"answered {a.status_code}"
            lines.append(f"  {tag}attempt {a.number} at {t(a.started_at)}: {res}")
        if d.status == Delivery.Status.PENDING and d.attempts:
            lines.append(f"  waiting to retry at {t(d.next_attempt_at)}")
        lines.append(f"  -> {d.status}" + (f" at {t(d.delivered_at)}" if d.delivered_at else ""))
        for a in AuditLog.objects.filter(object_type="delivery", object_id=str(d.id)).order_by("id"):
            lines.append(f"  {t(a.created_at)}: {a.actor} did '{a.action}' ({a.old_value} -> {a.new_value})")
        for al in Alert.objects.filter(kind="consumer_paused", detail__consumer=d.consumer.name,
                                       created_at__gte=event.created_at):
            lines.append(f"  {t(al.created_at)}: consumer '{d.consumer.name}' was paused by the circuit breaker "
                         "(events wait, none are dropped)")
    return lines
