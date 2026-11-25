from datetime import datetime, timedelta, timezone

from events import delivery_policy as p


def test_backoff_doubles_to_cap_and_never_shortens():
    no_jitter = lambda: 0.0
    waits = [p.backoff_delay(a, no_jitter) for a in range(1, 14)]
    assert waits[:8] == [30, 60, 120, 240, 480, 960, 1920, 3600]
    assert all(w == 3600 for w in waits[7:])
    assert p.backoff_delay(1, lambda: 1.0) == 30 * 1.25
    assert all(p.backoff_delay(a, lambda: 0.0) >= min(30 * 2 ** (a - 1), 3600) for a in range(1, 15))


def test_retry_budget_covers_a_six_hour_outage():
    # 14 attempts = 13 waits; with zero jitter (the minimum) the budget must still exceed 6 h
    total = sum(p.backoff_delay(a, lambda: 0.0) for a in range(1, p.MAX_ATTEMPTS))
    assert total > 6 * 3600


def test_retryable_statuses():
    for s in (None, 500, 502, 503, 504, 429, 408):
        assert p.is_retryable(s)
    for s in (400, 401, 403, 404, 410, 422):
        assert not p.is_retryable(s)


def test_outcomes():
    assert p.outcome_after(1, 200) == "delivered" and p.outcome_after(3, 204) == "delivered"
    assert p.outcome_after(1, 400) == "failed"          # consumer's bug: no retry storm
    assert p.outcome_after(1, 503) == "retry" and p.outcome_after(1, None) == "retry"
    assert p.outcome_after(p.MAX_ATTEMPTS, 503) == "dead"
    assert p.outcome_after(p.MAX_ATTEMPTS, 400) == "failed"


def test_retry_after_seconds_and_date_and_cap():
    assert p.retry_after_seconds("120") == 120
    assert p.retry_after_seconds("999999") == p.MAX_DELAY
    assert p.retry_after_seconds("garbage") is None and p.retry_after_seconds(None) is None
    now = datetime(2026, 11, 24, 12, 0, 0, tzinfo=timezone.utc)
    when = (now + timedelta(seconds=90)).strftime("%a, %d %b %Y %H:%M:%S GMT")
    assert abs(p.retry_after_seconds(when, now) - 90) < 1


def test_next_delay_honours_longer_retry_after():
    assert p.next_delay(1, "600", lambda: 0.0) == 600
    assert p.next_delay(1, "5", lambda: 0.0) == 30


def test_breaker_opens_after_five_failures_and_blocks():
    b = p.CircuitBreaker()
    for _ in range(4):
        b.record(False, 0)
    assert b.state == "closed" and b.allow(0)
    b.record(False, 0)
    assert b.state == "open" and not b.allow(10)


def test_breaker_success_resets_the_count():
    b = p.CircuitBreaker()
    for _ in range(4):
        b.record(False, 0)
    b.record(True, 0)
    for _ in range(4):
        b.record(False, 0)
    assert b.state == "closed"


def test_breaker_allows_one_probe_after_cooldown_then_closes_on_success():
    b = p.CircuitBreaker()
    for _ in range(5):
        b.record(False, 0)
    assert not b.allow(59)
    assert b.allow(60) and b.state == "half_open"
    assert not b.allow(61)                              # only one probe in flight
    b.record(True, 62)
    assert b.state == "closed" and b.allow(63)


def test_breaker_failed_probe_reopens_with_longer_cooldown():
    b = p.CircuitBreaker()
    for _ in range(5):
        b.record(False, 0)
    assert b.allow(60)
    b.record(False, 60)
    assert b.state == "open" and b.cooldown == 120
    assert not b.allow(179) and b.allow(180)
    for _ in range(10):                                 # cooldown is capped
        b.record(False, 200)
        b.allow(10_000)
    assert b.cooldown <= p.BREAKER_COOLDOWN_MAX
