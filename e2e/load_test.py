#!/usr/bin/env python3
"""Load test (spec D4 / scenario 12): 50 events/second for 5 minutes through the real edge, with duplicates, bad
signatures and a consumer that fails 30% of the time. Prints (and saves) the numbers.

  DELIVERY_TIME_SCALE=0.05 docker compose up -d --build   # retries scaled down so the backlog drains in minutes
  python3 e2e/load_test.py [rate_per_second] [seconds]

Stdlib only. Events go through the edge exactly like a provider's; everything measured is read back from the
system (client-side edge latency, Postgres, the RabbitMQ management API).
"""
import hashlib
import hmac
import json
import os
import random
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

RATE = float(sys.argv[1]) if len(sys.argv) > 1 else 50.0
SECONDS = int(sys.argv[2]) if len(sys.argv) > 2 else 300
PROJECT = os.environ.get("COMPOSE_PROJECT", "webhook-gateway")
EDGE = os.environ.get("EDGE", "http://localhost:8000")
CONSUMER = os.environ.get("CONSUMER", "http://localhost:9000")
RABBIT = os.environ.get("RABBIT", "")          # management API, e.g. http://localhost:15672 (optional)
SECRET = "whsec_dev_stripe"
RUN = str(int(time.time()))
DUP_RATE, BAD_RATE, FAIL_PCT = 0.08, 0.04, 30


def psql(sql):
    return subprocess.run(["docker", "compose", "-p", PROJECT, "exec", "-T", "db", "psql", "-U", "gateway", "-tA",
                           "-F", "|", "-c", sql], capture_output=True, text=True).stdout.strip()


def post(slug, body, sig_secret=SECRET, tamper=False):
    ts = int(time.time())
    sig = hmac.new(sig_secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    send = body + b" " if tamper else body
    req = urllib.request.Request(f"{EDGE}/in/{slug}", data=send, method="POST", headers={
        "content-type": "application/json", "stripe-signature": f"t={ts},v1={sig}"})
    t0 = time.perf_counter()
    try:
        status = urllib.request.urlopen(req, timeout=20).status
    except urllib.error.HTTPError as e:
        status = e.code
    except Exception:
        status = 0
    return status, (time.perf_counter() - t0) * 1000


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))] if xs else float("nan")


lock = threading.Lock()
lat, codes = [], {}
sent = {"valid_unique": 0, "duplicates": 0, "bad": 0}
monitor = {"stop": False, "max_queue": 0, "max_age": 0.0, "samples": 0}


def make_body(eid):
    return json.dumps({"id": eid, "type": "load.test", "n": random.random()}).encode()


def job(kind, body):
    # a duplicate is the SAME bytes again (a provider retry). A different body under the same id is a conflict.
    if kind == "bad":
        status, ms = post("s-load", body, tamper=True)
    else:
        status, ms = post("s-load", body)
    with lock:
        lat.append(ms)
        codes[status] = codes.get(status, 0) + 1
    return body


def watch():
    """Sample queue depth and the age of the oldest due delivery once a second while the test runs."""
    while not monitor["stop"]:
        depth = psql("select count(*), coalesce(extract(epoch from now() - min(next_attempt_at)), 0) "
                     "from events_delivery where status = 'pending' and next_attempt_at <= now()")
        if depth:
            n, age = depth.split("|")
            monitor["max_queue"] = max(monitor["max_queue"], int(n))
            monitor["max_age"] = max(monitor["max_age"], float(age))
            monitor["samples"] += 1
        time.sleep(1)


def main():
    t_start = time.time()
    ingest_workers = subprocess.run(["docker", "compose", "-p", PROJECT, "ps", "-q", "ingest"],
                                    capture_output=True, text=True).stdout.split().__len__()
    req = urllib.request.Request(f"{CONSUMER}/_control", method="POST",
                                 data=json.dumps({"path": "/load", "mode": f"fail_pct:{FAIL_PCT}"}).encode())
    urllib.request.urlopen(req).read()
    threading.Thread(target=watch, daemon=True).start()

    total = int(RATE * SECONDS)
    pool = ThreadPoolExecutor(max_workers=64)
    futures, t0 = [], time.perf_counter()
    for i in range(total):
        delay = t0 + i / RATE - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
        r = random.random()
        body = make_body(f"load-{RUN}-{i}")
        if r < BAD_RATE:
            sent["bad"] += 1
            futures.append(pool.submit(job, "bad", body))
        else:
            sent["valid_unique"] += 1
            futures.append(pool.submit(job, "valid", body))
            if random.random() < DUP_RATE:           # the provider retries: the same event again right away
                sent["duplicates"] += 1
                futures.append(pool.submit(job, "valid", body))
        if i % int(RATE * 30) == 0:
            print(f"  ... {i}/{total} sent ({time.perf_counter() - t0:.0f}s)", flush=True)
    for f in futures:
        f.result()
    send_time = time.perf_counter() - t0
    print(f"sending finished in {send_time:.1f}s; waiting for the backlog to drain ...", flush=True)

    prefix = f"load-{RUN}-"
    deadline = time.time() + 900
    while time.time() < deadline:
        left = int(psql(f"select count(*) from events_delivery d join events_event e on e.id = d.event_id "
                        f"where e.provider_event_id like '{prefix}%' and d.status in ('pending','in_flight')") or 0)
        if left == 0:
            break
        time.sleep(3)
    drain_time = time.perf_counter() - t0
    monitor["stop"] = True

    q = lambda s: psql(s)
    stored = int(q(f"select count(*) from events_event where provider_event_id like '{prefix}%'"))
    distinct = int(q(f"select count(distinct provider_event_id) from events_event where provider_event_id like '{prefix}%'"))
    dup_absorbed = int(q(f"select coalesce(sum(duplicate_count),0) from events_event where provider_event_id like '{prefix}%'"))
    st = dict(r.split("|") for r in q(f"select d.status, count(*) from events_delivery d join events_event e on e.id = d.event_id "
                                      f"where e.provider_event_id like '{prefix}%' group by 1").splitlines())
    attempts = q(f"select count(*), count(*) filter (where a.status_code = 200), "
                 f"count(*) filter (where a.status_code >= 500) from events_deliveryattempt a "
                 f"join events_delivery d on d.id = a.delivery_id join events_event e on e.id = d.event_id "
                 f"where e.provider_event_id like '{prefix}%'").split("|")
    e2e = q(f"select percentile_cont(array[0.5,0.95,0.99]) within group (order by extract(epoch from d.delivered_at - e.received_at)), "
            f"max(extract(epoch from d.delivered_at - e.received_at)) "
            f"from events_delivery d join events_event e on e.id = d.event_id "
            f"where e.provider_event_id like '{prefix}%' and d.status = 'delivered'")
    p50, p95, p99 = [float(x) for x in e2e.split("|")[0].strip("{}").split(",")]
    e2e_max = float(e2e.split("|")[1])
    ack_p = q(f"select percentile_cont(array[0.5,0.95,0.99]) within group (order by extract(epoch from e.created_at - e.received_at)) "
              f"from events_event e where e.provider_event_id like '{prefix}%'")
    ing = [float(x) for x in ack_p.strip("{}").split(",")]
    paused = int(q(f"select count(*) from events_alert where kind = 'consumer_paused' and created_at > to_timestamp({t_start})"))
    conflicts = int(q(f"select count(*) from events_eventconflict where created_at > to_timestamp({t_start})"))

    ok_accept = codes.get(200, 0)
    report = f"""# Load test results

Run at {time.strftime('%Y-%m-%d %H:%M')}: {RATE:.0f} events/s for {SECONDS}s through the real edge, RabbitMQ transport,
consumer failing {FAIL_PCT}% of requests at random, {ingest_workers} ingest worker(s), retries time-scaled by DELIVERY_TIME_SCALE={os.environ.get('DELIVERY_TIME_SCALE', '?')}.

| What | Result |
|---|---|
| Requests sent | {total + sent['duplicates']} ({sent['valid_unique']} unique valid, {sent['duplicates']} duplicates, {sent['bad']} bad signatures) |
| Sustained send rate | {(total + sent['duplicates']) / send_time:.1f} requests/s over {send_time:.0f}s |
| Edge answers | {', '.join(f'{k}: {v}' for k, v in sorted(codes.items()))} |
| Edge response time (client side) | p50 {pct(lat, .5):.0f} ms, p95 {pct(lat, .95):.0f} ms, p99 {pct(lat, .99):.0f} ms, max {max(lat):.0f} ms |
| Events stored | {stored} (distinct ids: {distinct}); expected {sent['valid_unique']} |
| Duplicates absorbed | {dup_absorbed} of {sent['duplicates']} sent |
| Same id, different body (conflicts) | {conflicts} (expected 0: the test sends identical bytes) |
| Bad signatures stored | 0 (rejected with 401: {codes.get(401, 0)} of {sent['bad']}) |
| Received to stored (queue + ingest worker) | p50 {ing[0]*1000:.0f} ms, p95 {ing[1]*1000:.0f} ms, p99 {ing[2]*1000:.0f} ms |
| Deliveries | {', '.join(f'{k}: {v}' for k, v in sorted(st.items()))} |
| Delivery attempts | {attempts[0]} total, {attempts[1]} answered 200, {attempts[2]} answered 5xx (the injected failures) |
| Received to delivered (incl. retries) | p50 {p50:.1f}s, p95 {p95:.1f}s, p99 {p99:.1f}s, max {e2e_max:.1f}s |
| Peak backlog of due deliveries | {monitor['max_queue']} (oldest waited up to {monitor['max_age']:.1f}s) |
| Circuit breaker trips | {paused} |
| Time until fully drained | {drain_time:.0f}s from the start |
"""
    print(report)
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "load_results.md")
    open(path, "w").write(report)
    problems = []
    if stored != sent["valid_unique"] or distinct != stored:
        problems.append("stored count does not match unique valid events")
    if codes.get(401, 0) != sent["bad"]:
        problems.append("not every bad signature was rejected")
    if dup_absorbed != sent["duplicates"]:
        problems.append(f"duplicates absorbed {dup_absorbed} != sent {sent['duplicates']}")
    if conflicts:
        problems.append("unexpected conflicts")
    if st.get("dead", 0) or st.get("failed", 0):
        problems.append(f"some deliveries did not complete: {st}")
    print("LOAD TEST " + ("FAILED: " + "; ".join(problems) if problems else "PASSED"))
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
