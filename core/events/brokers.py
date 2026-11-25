"""Ingest-side transports. Both give the same promise: a message leaves the queue only AFTER its event is committed
to Postgres (ack-after-commit), so a crash means redelivery and dedup absorbs the repeat.

RabbitMQ: durable queue + persistent messages + consumer acks.
Redis:    reliable-queue pattern. BLMOVE ingest -> ingest:processing, process, then LREM from processing.
          At startup anything left in processing (a worker died mid-message) goes back to the queue."""
import base64
import json
import logging
import time

import pika
import redis
from django.db import InterfaceError, OperationalError, close_old_connections

from . import controls
from .ingest import ingest_message

log = logging.getLogger("ingest")
INGEST, DEAD, ALERTS, PROCESSING = "ingest", "ingest.dead", "alerts", "ingest:processing"


def decode_redis_payload(payload: bytes | str) -> tuple[bytes, dict]:
    msg = json.loads(payload)
    return base64.b64decode(msg["body"]), msg["headers"]


_pause = {"t": -10.0, "v": False}


def paused_now() -> bool:
    """The operator's ingest switch, read from the database at most once a second (it used to be read on every pass,
    which cost a query per message). If the database is down, keep the last known value."""
    now = time.monotonic()
    if now - _pause["t"] >= 1.0:
        _pause["t"] = now
        try:
            close_old_connections()
            _pause["v"] = controls.ingest_paused()
        except (OperationalError, InterfaceError):
            pass
    return _pause["v"]


def handle(body: bytes, headers: dict) -> str:
    """Store one message. Returns 'ok' | 'retry' (database down: do not drop it) | 'dead' (poison message)."""
    close_old_connections()
    try:
        result = ingest_message(body, headers)
        log.info("ingested %s", result.outcome)
        return "ok"
    except (OperationalError, InterfaceError):
        log.warning("database unavailable, will retry")
        return "retry"
    except Exception:
        log.exception("poison message")
        return "dead"


# ---- RabbitMQ ---------------------------------------------------------------------------------------------------
def connect_rabbit(url: str, tries: int = 60):
    for _ in range(tries):
        try:
            return pika.BlockingConnection(pika.URLParameters(url))
        except pika.exceptions.AMQPConnectionError:
            time.sleep(1)
    raise RuntimeError("broker unreachable")


def run_rabbit(url: str) -> None:
    conn = connect_rabbit(url)
    ch = conn.channel()
    for q in (INGEST, DEAD, ALERTS):
        ch.queue_declare(queue=q, durable=True)
    ch.basic_qos(prefetch_count=10)

    def on_message(channel, method, props, body):
        outcome = handle(body, dict(props.headers or {}))
        if outcome == "ok":
            channel.basic_ack(method.delivery_tag)
        elif outcome == "retry":
            channel.basic_nack(method.delivery_tag, requeue=True)
            time.sleep(2)
        else:
            channel.basic_publish("", DEAD, body, pika.BasicProperties(headers=props.headers, delivery_mode=2))
            channel.basic_ack(method.delivery_tag)

    tag = None
    log.info("ingest worker ready (rabbitmq)")
    while True:
        paused = paused_now()
        if paused and tag is not None:
            ch.basic_cancel(tag)           # stop taking messages: they pile up in the queue, nothing is lost
            tag = None
            log.info("ingest paused")
        elif not paused and tag is None:
            tag = ch.basic_consume(INGEST, on_message)
            log.info("ingest consuming")
        # Short window on purpose: with time_limit=1 a delivered message waited for the END of the one-second window,
        # so received-to-stored latency was uniformly 0-1 s (the load test showed p50 0.5 s, p99 1.0 s).
        conn.process_data_events(time_limit=0.05)


# ---- Redis ------------------------------------------------------------------------------------------------------
def connect_redis(url: str, tries: int = 60):
    for _ in range(tries):
        try:
            r = redis.Redis.from_url(url)
            r.ping()
            return r
        except redis.exceptions.ConnectionError:
            time.sleep(1)
    raise RuntimeError("redis unreachable")


def requeue_orphans(r) -> int:
    n = 0
    while r.lmove(PROCESSING, INGEST, "LEFT", "RIGHT"):
        n += 1
    return n


def run_redis(url: str) -> None:
    r = connect_redis(url)
    n = requeue_orphans(r)
    log.info("ingest worker ready (redis); put %s unfinished messages back", n)
    was_paused = False
    while True:
        paused = paused_now()
        if paused != was_paused:
            log.info("ingest paused" if paused else "ingest consuming")
            was_paused = paused
        if paused:
            time.sleep(0.5)
            continue
        payload = r.blmove(INGEST, PROCESSING, 1, "RIGHT", "LEFT")
        if payload is None:
            continue
        body, headers = decode_redis_payload(payload)
        while True:
            outcome = handle(body, headers)
            if outcome != "retry":
                break
            time.sleep(2)                     # the message stays in `processing`, never dropped
        if outcome == "dead":
            r.lpush(DEAD, payload)
        r.lrem(PROCESSING, 1, payload)        # ack: only now does it leave the broker
