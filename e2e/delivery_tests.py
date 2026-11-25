#!/usr/bin/env python3
"""Delivery scenarios (spec 5-9 plus ordering and the circuit breaker) against the real stack.

The stack must be started with scaled-down time so hours of backoff run in seconds:
  DELIVERY_TIME_SCALE=0.02 DELIVERY_MAX_ATTEMPTS=5 DELIVERY_STUCK_GRACE=2 docker compose up -d --build ...
Shards: `main` runs the non-destructive scenarios concurrently; `crash` kills a worker, so it needs a stack of
its own. `e2e/run_all.sh` starts the stacks and runs every shard in parallel.
From the repo root:  python3 e2e/delivery_tests.py main|crash
Stdlib only. Events go in through the real edge; results are read from Postgres and the fake consumer.
"""
import hashlib
import hmac
from http.cookiejar import CookieJar
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

# each shard runs against its own compose project and ports, so shards can run in parallel
PROJECT = os.environ.get("COMPOSE_PROJECT", "webhook-gateway")
EDGE = os.environ.get("EDGE", "http://localhost:8000")
CONSUMER = os.environ.get("CONSUMER", "http://localhost:9000")
WEB = os.environ.get("WEB", "http://localhost:8080")
DC = ("docker", "compose", "-p", PROJECT)
SECRET = "whsec_dev_stripe"
RUN = str(int(time.time()))
FAILED = []


def sh(*a, check=True):
    return subprocess.run(a, check=check, capture_output=True, text=True).stdout.strip()


def psql(sql):
    return sh(*DC, "exec", "-T", "db", "psql", "-U", "gateway", "-tA", "-F", "|", "-c", sql)


def http(method, url, data=None):
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"content-type": "application/json"})
    try:
        return urllib.request.urlopen(req, timeout=15).read()
    except urllib.error.HTTPError as e:
        return e.read()


def control(path, mode):
    http("POST", f"{CONSUMER}/_control", json.dumps({"path": path, "mode": mode}).encode())


def hits(path):
    return json.loads(http("GET", f"{CONSUMER}/_stats?path={path}"))


def send(slug, event_id, **extra):
    body = json.dumps({"id": event_id, "type": "t", **extra}).encode()
    ts = int(time.time())
    sig = hmac.new(SECRET.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    req = urllib.request.Request(f"{EDGE}/in/{slug}", data=body, method="POST", headers={
        "content-type": "application/json", "stripe-signature": f"t={ts},v1={sig}"})
    for _ in range(60):
        try:
            if urllib.request.urlopen(req, timeout=10).status == 200:
                return
        except urllib.error.HTTPError as e:
            if e.code != 503:
                raise
        time.sleep(1)
    raise RuntimeError("edge never accepted the event")


def delivery(event_id, consumer):
    row = psql("select d.status, d.attempts, extract(epoch from d.created_at), "
               "coalesce(extract(epoch from d.delivered_at), 0) from events_delivery d "
               "join events_event e on e.id = d.event_id join events_consumer c on c.id = d.consumer_id "
               f"where e.provider_event_id = '{event_id}' and c.name = '{consumer}'")
    if not row:
        return None
    st, att, created, delivered = row.split("|")
    return {"status": st, "attempts": int(att), "created": float(created), "delivered": float(delivered)}


def wait(cond, what, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        try:
            r = cond()
            if r:
                return r
        except Exception:
            pass
        time.sleep(0.3)
    raise TimeoutError(what)


def check(ok, text):
    print(("PASS " if ok else "FAIL ") + text, flush=True)
    if not ok:
        FAILED.append(text)


def scenario(fn):
    try:
        fn()
    except Exception as e:  # a crashed scenario is a failed scenario
        check(False, f"{fn.__name__}: crashed: {type(e).__name__}: {e}")


SHARDS = {}   # filled at the bottom


def s5_flaky():
    """500 three times then 200: three retries with growing delay, delivered, attempts logged."""
    control("/flaky", "flaky:3")
    eid = f"s5-{RUN}"
    send("s-flaky", eid)
    wait(lambda: (delivery(eid, "c-flaky") or {}).get("status") == "delivered", "s5 delivered")
    d, h = delivery(eid, "c-flaky"), hits("/flaky")
    ts = [x["at"] for x in h]
    gaps = [b - a for a, b in zip(ts, ts[1:])]
    logged = int(psql("select count(*) from events_deliveryattempt a join events_delivery d on d.id = a.delivery_id "
                      f"join events_event e on e.id = d.event_id where e.provider_event_id = '{eid}'"))
    check(d["attempts"] == 4 and len(h) == 4 and all(x["sig_ok"] for x in h)
          and gaps[0] < gaps[1] < gaps[2] and logged == 4,
          f"5: 500 x3 then 200 -> delivered after {d['attempts']} attempts, gaps {[round(g, 2) for g in gaps]} "
          f"(growing), {logged} attempts logged, signatures valid")


def s6_reject():
    """400: failed at once, no retries."""
    control("/reject", "status:400")
    eid = f"s6-{RUN}"
    send("s-reject", eid)
    wait(lambda: (delivery(eid, "c-reject") or {}).get("status") == "failed", "s6 failed")
    time.sleep(3)   # a retry storm would show up here
    d, h = delivery(eid, "c-reject"), hits("/reject")
    check(d["attempts"] == 1 and len(h) == 1, f"6: 400 -> failed after {d['attempts']} attempt, consumer hit {len(h)}x")


def s7_dead():
    """Consumer down past the retry budget: dead, visible, nothing deleted."""
    control("/dead", "status:503")
    eid = f"s7-{RUN}"
    send("s-dead", eid)
    wait(lambda: (delivery(eid, "c-dead") or {}).get("status") == "dead", "s7 dead", timeout=90)
    d = delivery(eid, "c-dead")
    ev = int(psql(f"select count(*) from events_event where provider_event_id = '{eid}'"))
    al = int(psql("select count(*) from events_alert where kind = 'delivery_dead' and detail->>'consumer' = 'c-dead'"))
    check(d["attempts"] == 5 and ev == 1 and al >= 1,
          f"7: always-503 consumer -> dead after {d['attempts']} attempts, event kept ({ev}), alert raised")


def s8_isolation():
    """One consumer is dead/slow, the other healthy: the healthy one's latency is unaffected."""
    control("/slow", "slow:10")      # longer than its 3 s timeout: every attempt times out
    control("/healthy", "ok")
    ids = [f"s8-{RUN}-{i}" for i in range(40)]
    for i in ids:
        send("s-iso", i)
    wait(lambda: all((delivery(i, "c-healthy") or {}).get("status") == "delivered" for i in ids),
         "s8 healthy delivered", timeout=60)
    lat = sorted(delivery(i, "c-healthy")["delivered"] - delivery(i, "c-healthy")["created"] for i in ids)
    p95 = lat[int(len(lat) * 0.95) - 1]
    check(p95 < 3.0, f"8: dead/slow consumer next to a healthy one -> healthy p95 latency {p95:.2f}s "
                     f"(max {lat[-1]:.2f}s) over 40 events")


def s9_crash():
    """Worker killed mid-delivery: delivered again after restart, same event id, both attempts logged."""
    control("/crash", "slow:3")
    eid = f"s9-{RUN}"
    send("s-crash", eid)
    wait(lambda: hits("/crash"), "s9 first hit")
    sh(*DC, "kill", "-s", "KILL", "worker")
    control("/crash", "ok")
    sh(*DC, "start", "worker")
    wait(lambda: (delivery(eid, "c-crash") or {}).get("status") == "delivered", "s9 redelivered", timeout=90)
    h = hits("/crash")
    err = psql("select a.number, a.error from events_deliveryattempt a join events_delivery d on d.id = a.delivery_id "
               f"join events_event e on e.id = d.event_id where e.provider_event_id = '{eid}' order by a.number")
    ids = {x["event_id"] for x in h}
    check(len(h) >= 2 and len(ids) == 1 and "worker lost" in err and delivery(eid, "c-crash")["attempts"] >= 2,
          f"9: worker killed mid-delivery -> consumer saw {len(h)} hits with ONE event id, attempts "
          f"{[x['attempt'] for x in h]}; log: {err.replace(chr(10), ' ; ')}")


def s_order():
    """Ordered key: strictly in sequence, a stuck event blocks only its own key."""
    control("/order", "ok")
    first = f"o1a-{RUN}"
    control("/order", "flaky:2")      # every event fails twice before it succeeds (rule is per event id)
    send("s-order", first, order=f"o1-{RUN}")
    time.sleep(0.4)
    second, third, other = f"o1b-{RUN}", f"o1c-{RUN}", f"o2a-{RUN}"
    send("s-order", second, order=f"o1-{RUN}")
    send("s-order", third, order=f"o1-{RUN}")
    send("s-order", other, order=f"o2-{RUN}")
    for e in (first, second, third, other):
        wait(lambda e=e: (delivery(e, "c-order") or {}).get("status") == "delivered", f"order {e}", timeout=60)
    body_of = lambda x: json.loads(x["body"])["id"]
    h = hits("/order")
    seq = [body_of(x) for x in h if body_of(x) in (first, second, third)]
    t_other = min(x["at"] for x in h if body_of(x) == other)
    t_second = min(x["at"] for x in h if body_of(x) == second)
    # strictly sequential: every attempt of a, then every attempt of b, then c; never interleaved
    check(seq == [first] * 3 + [second] * 3 + [third] * 3 and t_other < t_second,
          f"order: key o1 attempts went a,a,a,b,b,b,c,c,c (never interleaved: {len(seq) == 9}), "
          f"and key o2 was NOT blocked (delivered {t_second - t_other:.1f}s before o1's b)")


def s_breaker():
    """Consumer failing: after 5 failures it is paused (nothing sent during the cooldown), events wait, and they
    resume once it recovers."""
    control("/breaker", "status:503")
    ids = [f"sb-{RUN}-{i}" for i in range(8)]
    for i in ids:
        send("s-breaker", i)
    wait(lambda: psql("select breaker_state from events_consumer where name = 'c-breaker'") == "open",
         "breaker open", timeout=30)
    opened_at, cooldown = [float(x) for x in psql(
        "select breaker_opened_at, breaker_cooldown from events_consumer where name = 'c-breaker'").split("|")]
    n_at_open = len(hits("/breaker"))
    time.sleep(max(0.0, opened_at + cooldown - time.time()) + 0.3)       # let the whole first cooldown pass
    # nothing may reach the consumer inside the cooldown window (a margin covers requests already on the wire)
    inside = [h for h in hits("/breaker") if opened_at + 0.15 < h["at"] < opened_at + cooldown - 0.15]
    quiet = not inside
    alert = int(psql("select count(*) from events_alert where kind = 'consumer_paused' "
                     "and detail->>'consumer' = 'c-breaker'")) >= 1
    control("/breaker", "ok")
    wait(lambda: all((delivery(i, "c-breaker") or {}).get("status") == "delivered" for i in ids),
         "breaker recovery", timeout=90)
    dead = int(psql("select count(*) from events_delivery d join events_consumer c on c.id = d.consumer_id "
                    "where c.name = 'c-breaker' and d.status = 'dead'"))
    check(quiet and alert and dead == 0,
          f"breaker: opened after repeated failures (consumer hit {n_at_open}x by then), silent for the whole "
          f"{cooldown:.1f}s cooldown: {quiet}, alert raised: {alert}, after recovery all 8 delivered, dead: {dead}")


def dc_exec(*cmd):
    return sh(*DC, "exec", "-T", "worker", "python", "manage.py", *cmd)


def s10_replay():
    """Spec 10: replay a dead event after the consumer is fixed: delivered once more, same event id, replayed flag,
    audit row says who. Also: the alert webhook fires for the dead event and resolves after the replay."""
    control("/replay", "status:503")
    eid = f"s10-{RUN}"
    send("s-replay", eid)
    wait(lambda: (delivery(eid, "c-replay") or {}).get("status") == "dead", "s10 dead", timeout=90)
    before = len(hits("/replay"))
    # the alert webhook received a dead_count_high message about this consumer
    wait(lambda: any(json.loads(h["body"]).get("kind") == "dead_count_high"
                     and json.loads(h["body"])["detail"].get("consumer") == "c-replay" for h in hits("/alerts")),
         "s10 dead alert delivered to the webhook", timeout=30)
    control("/replay", "ok")                                   # the consumer is fixed
    did = psql("select d.id from events_delivery d join events_event e on e.id = d.event_id "
               f"where e.provider_event_id = '{eid}'")
    out = dc_exec("replay_deliveries", "--actor", "taha", "--delivery", did)
    wait(lambda: (delivery(eid, "c-replay") or {}).get("status") == "delivered", "s10 replay delivered")
    h = hits("/replay")[before:]
    audit = psql("select actor, action, old_value, new_value from events_auditlog "
                 f"where action = 'replay' and object_id = '{did}'")
    event_ids = {x["event_id"] for x in hits("/replay")}
    log = psql("select string_agg(replay_round::text || ':' || number::text, ',' order by id) from "
               f"events_deliveryattempt where delivery_id = {did}")
    wait(lambda: psql("select count(*) from events_alert where kind = 'dead_count_high' and resolved = false "
                      "and detail->>'consumer' = 'c-replay'") == "0", "s10 alert resolved", timeout=30)
    check(len(h) == 1 and h[0]["replayed"] == "true" and len(event_ids) == 1
          and audit == "taha|replay|dead|pending" and log.startswith("0:1") and log.endswith("1:1"),
          f"10: dead event replayed after the consumer was fixed -> consumer got it once more "
          f"(replayed={h[0]['replayed'] if h else None}, same event id: {len(event_ids) == 1}), audit '{audit}', "
          f"attempt log (round:attempt) {log}, dead-count alert sent to webhook and resolved")
    dry = dc_exec("replay_deliveries", "--actor", "taha", "--consumer", "c-replay", "--status", "delivered")
    check("DRY RUN: 1 deliveries match" in dry and "nothing changed" in dry
          and delivery(eid, "c-replay")["status"] == "delivered",
          f"10b: bulk replay is a dry run by default -> '{dry.splitlines()[0]}', nothing changed")


def s_admin():
    """The admin shows an event with its raw body and deliveries, /metrics and /stats.json answer."""
    eid = f"adm-{RUN}"
    send("stripe-a", eid, marker=f"RAWBODY-{RUN}")
    wait(lambda: (delivery(eid, "c-demo") or {}).get("status") == "delivered", "admin event delivered")
    uuid = psql(f"select id from events_event where provider_event_id = '{eid}'")
    jar = CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    op.open(f"{WEB}/admin/login/").read()
    token = next(c.value for c in jar if c.name == "csrftoken")
    op.open(urllib.request.Request(f"{WEB}/admin/login/", method="POST", headers={"Referer": f"{WEB}/admin/login/"},
            data=urllib.parse.urlencode({"username": "admin", "password": "admin", "csrfmiddlewaretoken": token,
                                         "next": "/admin/"}).encode())).read()
    page = op.open(f"{WEB}/admin/events/event/{uuid}/change/").read().decode()
    lst = op.open(f"{WEB}/admin/events/delivery/?status__exact=delivered").read().decode()
    metrics_txt = urllib.request.urlopen(f"{WEB}/metrics").read().decode()
    stats = json.loads(urllib.request.urlopen(f"{WEB}/stats.json").read())
    check(f"RAWBODY-{RUN}" in page and "c-demo" in page and "delivered" in page
          and "Replay selected deliveries" in lst
          and "gateway_queue_depth" in metrics_txt and stats["events_total"] >= 1,
          "admin: event page shows the raw body and its deliveries, delivery list offers Replay, "
          "/metrics and /stats.json answer")


def s_graceful():
    """Spec D5: a worker stopped CLEANLY in the middle of deliveries (a normal deploy) loses nothing and does not
    send any event twice: in-flight deliveries finish, the rest wait and go out after the restart."""
    control("/grace", "slow:2")
    ids = [f"gr-{RUN}-{i}" for i in range(12)]
    for i in ids:
        send("s-grace", i)
    wait(lambda: len(hits("/grace")) >= 3, "graceful: deliveries under way")
    sh(*DC, "stop", "-t", "30", "worker")          # SIGTERM: Celery's warm shutdown finishes what it is doing
    mid = {i: (delivery(i, "c-grace") or {}).get("status") for i in ids}
    sh(*DC, "start", "worker")
    wait(lambda: all((delivery(i, "c-grace") or {}).get("status") == "delivered" for i in ids),
         "graceful: all delivered", timeout=90)
    per_event = {}
    for h in hits("/grace"):
        per_event[h["event_id"]] = per_event.get(h["event_id"], 0) + 1
    doubled = {e: n for e, n in per_event.items() if n > 1}
    check(len(per_event) == 12 and not doubled,
          f"D5: worker stopped cleanly mid-run (statuses at stop: {sorted(set(v for v in mid.values() if v))}) -> "
          f"all 12 delivered, {len(doubled)} delivered twice (want 0)")


SHARDS = {"main": [s5_flaky, s6_reject, s7_dead, s8_isolation, s_order, s_breaker, s10_replay, s_admin],
          "crash": [s9_crash, s_graceful]}


if __name__ == "__main__":
    shard = sys.argv[1] if len(sys.argv) > 1 else "main"
    http("POST", f"{CONSUMER}/_reset")
    wait(lambda: sh(*DC, "ps", "-q", "dispatcher"), "stack up")
    t0 = time.time()
    only = os.environ.get("ONLY")          # e.g. ONLY=s10_replay to run one scenario while debugging
    fns = [f for f in SHARDS[shard] if not only or f.__name__ == only]
    if shard == "crash":                  # these stop/kill the same worker, so they must not overlap
        for fn in fns:
            scenario(fn)
    else:
        threads = [threading.Thread(target=scenario, args=(fn,)) for fn in fns]
        [t.start() for t in threads]
        [t.join() for t in threads]
    print(f"[{shard}] {'ALL SCENARIOS PASSED' if not FAILED else str(len(FAILED)) + ' FAILED'} "
          f"({len(fns)} scenarios{'' if shard == 'crash' else ' in parallel'}, {time.time() - t0:.0f}s)")
    sys.exit(1 if FAILED else 0)
