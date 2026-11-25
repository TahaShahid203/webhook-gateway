"""Sources (one provider connection each), loaded from a JSON file so the edge needs no database.

The core writes this file whenever a Source changes (add, rotate a secret, change a rate limit). The edge re-reads it
when its mtime changes, so none of that needs a restart.
File format: {"sources": {"<slug>": {"scheme": "stripe"|"shopify", "secret": "...", "previous_secret": "",
              "rate_per_second": null, "rate_burst": null}}}
"""
import json
import math
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Source:
    slug: str
    scheme: str
    secrets: tuple          # newest first; two while a rotation is in progress
    rate_per_second: float | None = None
    rate_burst: int | None = None

    @property
    def secret(self) -> str:
        return self.secrets[0]


class SourceStore:
    def __init__(self, path: str):
        self.path = path
        self._mtime = -1.0
        self._sources: dict[str, Source] = {}

    def _reload_if_changed(self) -> None:
        try:
            mtime = os.path.getmtime(self.path)
        except FileNotFoundError:
            return                      # not written yet: keep what we have (an empty set answers 404)
        if mtime == self._mtime:
            return
        with open(self.path) as f:
            raw = json.load(f)["sources"]
        self._sources = {slug: Source(
            slug, cfg["scheme"],
            tuple(s for s in (cfg["secret"], cfg.get("previous_secret") or "") if s),
            cfg.get("rate_per_second"), cfg.get("rate_burst")) for slug, cfg in raw.items()}
        self._mtime = mtime

    def get(self, slug: str) -> Source | None:
        self._reload_if_changed()
        return self._sources.get(slug)


class TokenBucket:
    """Per-source rate limit at the edge. allow() -> True if a request may pass now."""

    def __init__(self, clock):
        self.clock = clock
        self.state: dict[str, tuple[float, float]] = {}      # slug -> (tokens, last)

    def allow(self, source: Source) -> bool:
        if not source.rate_per_second:
            return True
        burst = source.rate_burst or max(1, math.ceil(source.rate_per_second))
        now = self.clock()
        tokens, last = self.state.get(source.slug, (float(burst), now))
        tokens = min(float(burst), tokens + (now - last) * source.rate_per_second)
        if tokens < 1:
            self.state[source.slug] = (tokens, now)
            return False
        self.state[source.slug] = (tokens - 1, now)
        return True
