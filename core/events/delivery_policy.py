"""Pure delivery rules: backoff, which failures retry, Retry-After, circuit breaker.
No I/O and an injectable clock/rng, so every rule is unit-testable."""
import os
import random
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone

# Production values. The test stack scales time down (DELIVERY_TIME_SCALE) and may shorten the
# attempt budget (DELIVERY_MAX_ATTEMPTS) so the real code paths run in seconds instead of hours.
TIME_SCALE = float(os.getenv("DELIVERY_TIME_SCALE", "1"))
MAX_ATTEMPTS = int(os.getenv("DELIVERY_MAX_ATTEMPTS", "14"))
BASE_DELAY = 30 * TIME_SCALE            # seconds
MAX_DELAY = 3600 * TIME_SCALE
JITTER = 0.25              # +0..25%, never negative, so the outage budget is a floor
BREAKER_THRESHOLD = 5
BREAKER_COOLDOWN = 60 * TIME_SCALE
BREAKER_COOLDOWN_MAX = 600 * TIME_SCALE

RETRYABLE_STATUS = {408, 429}


def backoff_delay(attempt: int, rng=random.random) -> float:
    """Seconds to wait after failed attempt number `attempt` (1-based)."""
    base = min(BASE_DELAY * 2 ** (attempt - 1), MAX_DELAY)
    return base * (1 + JITTER * rng())


def is_retryable(status: int | None) -> bool:
    """status None = timeout / connection error."""
    if status is None:
        return True
    return status >= 500 or status in RETRYABLE_STATUS


def retry_after_seconds(value: str | None, now: datetime | None = None) -> float | None:
    """Parse a Retry-After header (seconds or HTTP-date); capped at MAX_DELAY."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return min(float(value), MAX_DELAY)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    now = now or datetime.now(timezone.utc)
    return min(max((when - now).total_seconds(), 0.0), MAX_DELAY)


def next_delay(attempt: int, retry_after: str | None = None, rng=random.random) -> float:
    """Wait before the next attempt: the longer of our backoff and the consumer's Retry-After."""
    ra = retry_after_seconds(retry_after)
    delay = backoff_delay(attempt, rng)
    return max(delay, ra) if ra is not None else delay


def outcome_after(attempt: int, status: int | None) -> str:
    """'delivered' | 'failed' (no retry) | 'retry' | 'dead' (retries exhausted)."""
    if status is not None and 200 <= status < 300:
        return "delivered"
    if not is_retryable(status):
        return "failed"
    return "dead" if attempt >= MAX_ATTEMPTS else "retry"


@dataclass
class CircuitBreaker:
    """closed -> (5 failures in a row) -> open -> (cooldown) -> half_open: one probe decides."""
    state: str = "closed"
    consecutive_failures: int = 0
    opened_at: float = 0.0
    cooldown: float = BREAKER_COOLDOWN

    def allow(self, now: float) -> bool:
        """May a delivery be sent now? While open, nothing is sent; after the cooldown one probe is."""
        if self.state == "closed":
            return True
        if self.state == "open" and now - self.opened_at >= self.cooldown:
            self.state = "half_open"
            return True            # exactly one probe
        return False               # open, or half_open with the probe already out

    def record(self, success: bool, now: float) -> None:
        if success:
            self.state, self.consecutive_failures, self.cooldown = "closed", 0, BREAKER_COOLDOWN
            return
        self.consecutive_failures += 1
        if self.state == "half_open":                       # probe failed: back off longer
            self.cooldown = min(self.cooldown * 2, BREAKER_COOLDOWN_MAX)
            self.state, self.opened_at = "open", now
        elif self.state == "closed" and self.consecutive_failures >= BREAKER_THRESHOLD:
            self.state, self.opened_at = "open", now
