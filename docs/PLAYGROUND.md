# Playground and crash demo

Back to the [README](../README.md).

**Try it:** run `./demo.sh`, then open <http://localhost:8080/play/>. It is a 3D neon city you can orbit with the mouse, and
every part of it is a real part of the gateway: three launch pads fire real signed Stripe-style and Shopify-style webhooks
(and a fake-webhook pad fires forged ones) at the real edge. A scanner ring checks each signature, a glass tube holds the
durable queue (it visibly fills when you switch the ingest worker off), a gear turbine does the dedup, a tower of discs
grows by one for every event stored in Postgres, delivery drones fly to two consumer skyscrapers (their windows go dark
when the consumer is down, and a force-field dome closes when the circuit breaker trips), and failed deliveries end up in a
graveyard you can replay from. **Ride along** puts a chase camera on a webhook for its whole journey, and a trace panel
ticks off the real steps with real numbers. The animation is slowed down so a viewer can follow it (a note on screen shows
the real edge time, usually a few milliseconds); there is a faster pace setting. Nothing on that page is mocked.

**Crash the queue:** pick a transport (RabbitMQ, Redis with an append-only file, or Redis with no persistence at all), switch the
ingest worker off, fire a burst so acknowledged webhooks pile up in the queue, then press *crash the queue*. That runs a real
`kill -9` on the broker's container and restarts it, and a banner reports what survived. RabbitMQ and Redis-with-AOF keep
every webhook; Redis with no persistence loses all of them, even though the edge had answered 200 for each. (This button mounts
the Docker socket, so it is only enabled by `demo.sh` via `docker-compose.chaos.yml` and must never be exposed publicly.)
Three.js (MIT) is vendored under `core/playground/static/playground/vendor/` so the demo works offline.
