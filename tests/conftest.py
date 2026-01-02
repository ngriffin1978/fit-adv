import pytest

@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """
    Prevent real sleeping during tests.
    Backoff logic is validated via assertions, not wall-clock time.
    """
    monkeypatch.setattr("fit_adv.io_whoop_api.time.sleep", lambda *_args, **_kwargs: None)

