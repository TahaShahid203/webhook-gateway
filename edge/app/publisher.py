"""Publishing to the durable ingest queue. The edge only says 200 after the broker confirms."""
import asyncio
import base64
import json
import logging
from typing import Protocol

log = logging.getLogger("edge.publisher")

INGEST_QUEUE = "ingest"
ALERT_QUEUE = "alerts"


class PublishError(Exception):
    """The broker did not confirm the message. The caller must return 503, never 200."""


class Publisher(Protocol):
    async def publish(self, body: bytes, headers: dict, queue: str = INGEST_QUEUE) -> None: ...


class RabbitPublisher:
    def __init__(self, url: str, confirm_timeout: float = 2.0, connect_timeout: float = 8.0):
        self.url = url
        self.confirm_timeout = confirm_timeout
        self.connect_timeout = connect_timeout
        self._conn = None
        self._channel = None

    async def _ensure(self):
        import aio_pika
        if self._conn is None or self._conn.is_closed:
            self._conn = await aio_pika.connect_robust(self.url, timeout=self.connect_timeout)
            self._channel = await self._conn.channel(publisher_confirms=True)
            for q in (INGEST_QUEUE, ALERT_QUEUE):
                await self._channel.declare_queue(q, durable=True)
        return self._channel

    async def publish(self, body: bytes, headers: dict, queue: str = INGEST_QUEUE) -> None:
        import aio_pika
        try:
            # a cold connection (first request, or after a broker restart) gets a longer budget
            # than a publish on a warm channel
            channel = await asyncio.wait_for(self._ensure(), self.connect_timeout)
            message = aio_pika.Message(body, headers=headers,
                                       delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
            # with publisher_confirms=True this returns only after the broker acks (or raises)
            await asyncio.wait_for(
                channel.default_exchange.publish(message, routing_key=queue, mandatory=True),
                self.confirm_timeout)
        except Exception as exc:  # broker down, timeout, nack, unroutable
            log.warning("publish failed (%s): %r", type(exc).__name__, exc)
            self._conn = None
            raise PublishError(str(exc)) from exc


class RedisPublisher:
    """Same contract as RabbitPublisher over a Redis list: the payload is LPUSHed and the edge says 200 only after
    Redis replied. How durable that is depends on Redis's own persistence (appendfsync always / everysec / none),
    which is exactly what the kill -9 comparison measures."""

    def __init__(self, url: str, timeout: float = 2.0):
        self.url = url
        self.timeout = timeout
        self._client = None

    def _redis(self):
        import redis.asyncio as aioredis
        if self._client is None:
            self._client = aioredis.from_url(self.url, socket_timeout=self.timeout,
                                             socket_connect_timeout=self.timeout)
        return self._client

    async def publish(self, body: bytes, headers: dict, queue: str = INGEST_QUEUE) -> None:
        payload = json.dumps({"headers": headers, "body": base64.b64encode(body).decode()})
        try:
            await asyncio.wait_for(self._redis().lpush(queue, payload), self.timeout + 1)
        except Exception as exc:
            log.warning("redis publish failed (%s): %r", type(exc).__name__, exc)
            self._client = None
            raise PublishError(str(exc)) from exc
