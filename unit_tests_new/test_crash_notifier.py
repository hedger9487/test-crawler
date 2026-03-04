import urllib.error

from crawler.utils.crash_notifier import send_crash_notification


def test_crash_notifier_returns_false_without_webhook():
    ok = send_crash_notification(RuntimeError("boom"), webhook_url="")
    assert ok is False


def test_crash_notifier_success_path(monkeypatch):
    class _Resp:
        status = 204

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr("urllib.request.urlopen", lambda *_args, **_kwargs: _Resp())
    ok = send_crash_notification(RuntimeError("boom"), webhook_url="https://x.test")
    assert ok is True


def test_crash_notifier_handles_network_error(monkeypatch):
    def _raise(*_args, **_kwargs):
        raise urllib.error.URLError("down")

    monkeypatch.setattr("urllib.request.urlopen", _raise)
    ok = send_crash_notification(RuntimeError("boom"), webhook_url="https://x.test")
    assert ok is False
