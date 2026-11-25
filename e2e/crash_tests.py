#!/usr/bin/env python3
"""Crash tests (spec scenarios 13-15), driven from the host against the real compose stack.

Run from the repo root:  python3 e2e/crash_tests.py
Needs: docker compose, the stack's edge on localhost:8000. Stdlib only.
"""
import hashlib
import hmac
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

PROJECT = os.environ.get("COMPOSE_PROJECT", "webhook-gateway")
# which transport to break: rabbitmq (default) or redis. The edge picks it per request via X-Gateway-Broker.
KIND = os.environ.get("BROKER_KIND", "rabbitmq")
BROKER_SERVICE = "redis" if KIND == "redis" else "broker"
INGEST_SERVICE = "ingest-redis" if KIND == "redis" else "ingest"
FSYNC = os.environ.get("REDIS_APPENDFSYNC", "-")
AOF = os.environ.get("REDIS_APPENDONLY", "yes")          # "no" = Redis with persistence off (the contrast case)
LABEL = ("RabbitMQ (durable queue, persistent messages)" if KIND == "rabbitmq" else
         "Redis, persistence OFF" if AOF == "no" else f"Redis AOF appendfsync={FSYNC}")
EXPECT_LOSS = KIND == "redis" and AOF == "no"
EDGE = os.environ.get("EDGE", "http://localhost:8000")
SECRET = json.load(open("dev/sources.json"))["sources"]["stripe-a"]["secret"]
RUN = str(int(time.time()))


def sh(*args, check=True):
    return subprocess.run(args, check=check, capture_output=True, text=True).stdout.strip()


def compose(*args, check=True):
    return sh("docker", "compose", "-p", PROJECT, *args, check=check)


def psql(sql):
    return compose("exec", "-T", "db", "psql", "-U", "gateway", "-tA", "-c", sql)


def post(event_id):
    body = json.dumps({"id": event_id, "type": "crash.test"}).encode()
    ts = int(time.time())
    sig = hmac.new(SECRET.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    req = urllib.request.Request(f"{EDGE}/in/stripe-a", data=body, method="POST", headers={
        "content-type": "application/json", "stripe-signature": f"t={ts},v1={sig}", "x-gateway-broker": KIND})
    try:
        return urllib.request.urlopen(req, timeout=15).status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return 0


def count(prefix):
    return int(psql(f"select count(*) from events_event where provider_event_id like '{prefix}%'"))


def distinct(prefix):
    return int(psql(f"select count(distinct provider_event_id) from events_event "
                    f"where provider_event_id like '{prefix}%'"))


def wait_until(cond, what, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if cond():
                return
        except Exception:
            pass
        time.sleep(1)
    print(f"FAIL: timed out waiting for {what}")
    sys.exit(1)


def report(ok, text):
    print(("PASS " if ok else "FAIL ") + text, flush=True)
    return ok


def wait_stack_ready():
    wait_until(lambda: post(f"warm-{RUN}-{time.time()}") == 200, "stack accepting events")
    wait_until(lambda: int(psql("select count(*) from events_event")) >= 1, "worker storing events")
    print(f"[crash tests: {LABEL}]", flush=True)


def scenario_13(n=500):
    """Django DB and the ingest worker are down; edge keeps accepting; nothing is lost or doubled."""
    prefix = f"s13-{RUN}-"
    compose("stop", INGEST_SERVICE, "db")
    statuses = [post(f"{prefix}{i}") for i in range(n)]
    accepted = statuses.count(200)
    compose("start", "db")
    wait_until(lambda: psql("select 1") == "1", "db back")
    compose("start", INGEST_SERVICE)
    wait_until(lambda: count(prefix) >= accepted, f"{accepted} events stored", timeout=180)
    time.sleep(3)  # let any wrongly duplicated rows show up
    stored, uniq = count(prefix), distinct(prefix)
    ok = report(accepted == n and stored == n and uniq == n,
                f"13: sent {n}, edge accepted {accepted}, stored {stored}, distinct {uniq} "
                f"(core down the whole time)")
    return ok


def scenario_14():
    """Broker down: the edge must say 503 and never a false 200."""
    prefix = f"s14-{RUN}-"
    compose("stop", BROKER_SERVICE)
    down = [post(f"{prefix}d{i}") for i in range(10)]
    compose("start", BROKER_SERVICE)
    wait_until(lambda: post(f"{prefix}up") == 200, "edge recovers after broker restart", timeout=90)
    wait_until(lambda: count(prefix) >= 1, "recovered event stored")
    stored_down = int(psql(f"select count(*) from events_event where provider_event_id like '{prefix}d%'"))
    ok = report(all(s == 503 for s in down) and stored_down == 0,
                f"14: broker down -> statuses {sorted(set(down))} (want only 503), "
                f"events from the outage stored: {stored_down} (want 0), edge recovered")
    return ok


def scenario_15(total=600, kill_after=150):
    """kill -9 the broker mid-load. Provider-like senders retry on 503. Count acknowledged-but-lost."""
    prefix = f"s15-{RUN}-"
    acked, lock = set(), threading.Lock()
    sent = {"n": 0}

    def sender():
        for i in range(total):
            eid = f"{prefix}{i}"
            for _ in range(80):                # a provider retries a non-2xx (about 20 s of trying)
                if post(eid) == 200:
                    with lock:
                        acked.add(eid)
                    break
                time.sleep(0.25)
            sent["n"] = i + 1
            time.sleep(0.005)

    t = threading.Thread(target=sender, daemon=True)   # never keep the process alive if the test fails
    t.start()
    while sent["n"] < kill_after:
        time.sleep(0.05)
    container = compose("ps", "-q", BROKER_SERVICE)
    sh("docker", "kill", "-s", "KILL", container)
    killed_at = sent["n"]
    time.sleep(4)
    compose("start", BROKER_SERVICE)
    t.join()
    # with Redis everysec some acknowledged events are legitimately gone: wait for the queue to drain, not for all
    deadline = len(acked) if (KIND == "rabbitmq" or FSYNC == "always") else 0
    wait_until(lambda: count(prefix) >= deadline, "acknowledged events stored", timeout=240)
    if deadline == 0:
        previous = -1
        while previous != count(prefix):
            previous = count(prefix)
            time.sleep(4)
    time.sleep(3)
    stored_ids = set(psql(f"select provider_event_id from events_event "
                          f"where provider_event_id like '{prefix}%'").split())
    lost = sorted(acked - stored_ids)
    dupes = count(prefix) - distinct(prefix)
    line = (f"15 ({LABEL}): kill -9 at event ~{killed_at}; acknowledged {len(acked)}, "
            f"stored {len(stored_ids)}, acknowledged-but-lost {len(lost)}, duplicate rows {dupes}")
    if EXPECT_LOSS:
        ok = report(dupes == 0 and len(stored_ids) + len(lost) == len(acked), line + "  [loss is possible here]")
    else:
        ok = report(not lost and dupes == 0, line)
    return ok


def set_ingest_paused(paused):
    psql("insert into events_control(key, value, updated_at) values ('ingest_paused', "
         f"'{1 if paused else 0}', now()) on conflict (key) do update set value = excluded.value, updated_at = now()")


def scenario_15b(n=300):
    """kill -9 the broker while a FULL BACKLOG of acknowledged events exists only inside it. Ingest is paused, so
    nothing has reached Postgres: every acknowledged event is in the broker's hands when it dies."""
    prefix = f"s15b-{RUN}-"
    set_ingest_paused(True)
    time.sleep(3)                                            # the worker notices and stops consuming
    acked = [f"{prefix}{i}" for i in range(n) if post(f"{prefix}{i}") == 200]
    in_pg_before = count(prefix)
    container = compose("ps", "-q", BROKER_SERVICE)
    sh("docker", "kill", "-s", "KILL", container)
    time.sleep(3)
    compose("start", BROKER_SERVICE)
    wait_until(lambda: post(f"{prefix}probe") == 200, "broker back", timeout=90)
    set_ingest_paused(False)
    previous = -1
    time.sleep(4)
    while previous != count(prefix):                         # wait for the backlog to drain, however much survived
        previous = count(prefix)
        time.sleep(4)
    stored_ids = set(psql(f"select provider_event_id from events_event where provider_event_id like '{prefix}%'").split())
    lost = [e for e in acked if e not in stored_ids]
    dupes = count(prefix) - distinct(prefix)
    line = (f"15b ({LABEL}): {len(acked)} events acknowledged and sitting only in the broker "
            f"({in_pg_before} in Postgres) when it was kill -9'd; after restart stored {len(acked) - len(lost)}, "
            f"acknowledged-but-LOST {len(lost)}, duplicate rows {dupes}")
    if EXPECT_LOSS:
        return report(dupes == 0 and in_pg_before == 0 and len(acked) - len(lost) + len(lost) == len(acked),
                      line + "  [this is the contrast case]")
    return report(not lost and dupes == 0 and in_pg_before == 0, line)


if __name__ == "__main__":
    wait_stack_ready()
    results = [scenario_13(), scenario_14(), scenario_15(), scenario_15b()]
    print("ALL CRASH TESTS PASSED" if all(results) else "CRASH TESTS FAILED")
    sys.exit(0 if all(results) else 1)
