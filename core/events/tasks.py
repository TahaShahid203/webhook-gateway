from celery import shared_task

from . import delivery


@shared_task(name="events.deliver", acks_late=True, reject_on_worker_lost=True)
def deliver(delivery_id: int) -> str:
    claimed = delivery.claim(delivery_id)
    if claimed is None:
        return "skipped"          # not due, blocked by ordering/cap/breaker, or someone else has it
    status, error, body, retry_after = delivery.send(claimed)
    outcome = delivery.finish(claimed, status, error, body, retry_after)
    if outcome == "delivered":
        nxt = delivery.next_in_key(delivery_id)
        if nxt:
            deliver.delay(nxt)
    return outcome
