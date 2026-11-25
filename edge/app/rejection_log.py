"""Log of rejected (bad signature) requests, with a disk cap.

Full logging until the file reaches `cap_bytes`. Then: one alert, a counter, and
a 1-in-`sample_every` sample. When someone deletes/truncates the file and it is under
the cap again, logging goes back to full.
"""
import json
import os
import time
from typing import Callable


class RejectionLog:
    def __init__(self, path: str, cap_bytes: int, sample_every: int = 100,
                 alert: Callable[[dict], None] | None = None):
        self.path = path
        self.cap_bytes = cap_bytes
        self.sample_every = sample_every
        self.alert = alert or (lambda info: None)
        self.degraded = False
        self.suppressed = 0     # rejections counted but not written, since entering degraded mode
        self.total = 0          # every rejection ever seen by this process

    def _size(self) -> int:
        try:
            return os.path.getsize(self.path)
        except FileNotFoundError:
            return 0

    def _write(self, record: dict) -> None:
        with open(self.path, "a") as f:
            f.write(json.dumps(record) + "\n")

    def record(self, source: str, reason: str, remote: str | None = None) -> None:
        self.total += 1
        entry = {"ts": int(time.time()), "source": source, "reason": reason, "remote": remote}
        size = self._size()

        if self.degraded and size < self.cap_bytes:
            # space was freed: back to logging everything
            self.degraded = False
            self.suppressed = 0

        if not self.degraded and size >= self.cap_bytes:
            self.degraded = True
            self.alert({"kind": "rejection_log_full", "path": self.path, "cap_bytes": self.cap_bytes})

        if not self.degraded:
            self._write(entry)
            return

        self.suppressed += 1
        if self.suppressed % self.sample_every == 1:
            self._write({**entry, "sampled": True, "suppressed_so_far": self.suppressed})
