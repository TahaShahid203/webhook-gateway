"""A controllable fake consumer, used by the end-to-end delivery tests.

POST /<anything>        the webhook endpoint; behaviour depends on the mode set for that path
POST /_control          {"path": "/x", "mode": "ok" | "status:503" | "slow:3" | "flaky:2" | "retry_after:5"}
GET  /_stats?path=/x    every hit on that path: event id, attempt, signature check, status we answered
POST /_reset            forget all hits and modes
Modes: flaky:N answers 500 for the first N hits of each event id, then 200; fail_pct:P fails P% at random.
"""
import hashlib
import hmac
import json
import os
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

SECRET = os.environ.get("CONSUMER_SECRET", "consumer-secret")
lock = threading.Lock()
hits, modes, seen = [], {}, {}


def decide(path, event_id):
    mode = modes.get(path, "ok")
    if mode == "ok":
        return 200, {}, 0
    kind, _, arg = mode.partition(":")
    if kind == "status":
        return int(arg), {}, 0
    if kind == "slow":
        return 200, {}, float(arg)
    if kind == "flaky":
        n = seen.get((path, event_id), 0)
        return (500 if n < int(arg) else 200), {}, 0
    if kind == "fail_pct":                     # fail this percentage of requests at random (the load test uses 30)
        return (500 if random.random() * 100 < float(arg) else 200), {}, 0
    if kind == "retry_after":
        return 429, {"Retry-After": arg}, 0
    return 200, {}, 0


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _json(self, code, obj):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/_stats":
            path = parse_qs(u.query).get("path", [""])[0]
            with lock:
                return self._json(200, [h for h in hits if not path or h["path"] == path])
        self._json(200, {"ok": True})

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n)
        if self.path == "/_control":
            c = json.loads(body)
            with lock:
                modes[c["path"]] = c["mode"]
            return self._json(200, {"ok": True})
        if self.path == "/_reset":
            with lock:
                hits.clear(), modes.clear(), seen.clear()
            return self._json(200, {"ok": True})

        event_id = self.headers.get("X-Gateway-Event-Id", "")
        sig = self.headers.get("X-Gateway-Signature", "")
        parts = dict(p.split("=", 1) for p in sig.split(",") if "=" in p)
        expect = hmac.new(SECRET.encode(), f"{parts.get('t', '')}.".encode() + body, hashlib.sha256).hexdigest()
        with lock:
            code, headers, delay = decide(self.path, event_id)
            seen[(self.path, event_id)] = seen.get((self.path, event_id), 0) + 1
            hit = {"path": self.path, "event_id": event_id, "attempt": self.headers.get("X-Gateway-Attempt"),
                   "replayed": self.headers.get("X-Gateway-Replayed"), "sig_ok": hmac.compare_digest(
                       parts.get("v1", ""), expect), "answered": code, "at": time.time(),
                   "body": body.decode(errors="replace")}
            hits.append(hit)
        if delay:
            time.sleep(delay)
        self.send_response(code)
        for k, v in headers.items():
            self.send_header(k, v)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"{}")


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 9000), Handler).serve_forever()
