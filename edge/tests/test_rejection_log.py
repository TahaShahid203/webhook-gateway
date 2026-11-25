import json

from app.rejection_log import RejectionLog


def lines(path):
    with open(path) as f:
        return [json.loads(x) for x in f]


def test_logs_everything_under_cap(tmp_path):
    p = tmp_path / "rej.log"
    log = RejectionLog(str(p), cap_bytes=10_000)
    for _ in range(5):
        log.record("shop-a", "bad_signature")
    assert len(lines(p)) == 5 and not log.degraded


def test_cap_alerts_once_then_counts_and_samples(tmp_path):
    p = tmp_path / "rej.log"
    alerts = []
    log = RejectionLog(str(p), cap_bytes=200, sample_every=10, alert=alerts.append)
    for _ in range(200):
        log.record("shop-a", "bad_signature")
    assert log.degraded
    assert len(alerts) == 1 and alerts[0]["kind"] == "rejection_log_full"
    assert log.total == 200
    # far fewer lines than rejections: full lines up to the cap, then ~1 in 10
    assert len(lines(p)) < 60
    assert any(l.get("sampled") for l in lines(p))


def test_freeing_space_resumes_full_logging_and_can_alert_again(tmp_path):
    p = tmp_path / "rej.log"
    alerts = []
    log = RejectionLog(str(p), cap_bytes=200, sample_every=10, alert=alerts.append)
    for _ in range(50):
        log.record("s", "bad_signature")
    assert log.degraded
    p.write_text("")                      # someone clears the logs
    log.record("s", "bad_signature")
    assert not log.degraded and log.suppressed == 0
    before = len(lines(p))
    log.record("s", "bad_signature")
    assert len(lines(p)) == before + 1    # logging everything again
    for _ in range(50):
        log.record("s", "bad_signature")
    assert len(alerts) == 2               # it filled again, so it alerts again
