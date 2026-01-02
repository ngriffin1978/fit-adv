from fit_adv.io_whoop_api import WHOOP_API_BASE, fetch_collection


def test_backoff_retries_429_then_succeeds(requests_mock, monkeypatch):
    url = f"{WHOOP_API_BASE}/cycle"

    sleeps = []

    def fake_sleep(x):
        sleeps.append(float(x))

    # IMPORTANT: patch the sleep used inside the module under test
    monkeypatch.setattr("fit_adv.io_whoop_api.time.sleep", fake_sleep)

    # First response: 429 with Retry-After, second: 200
    requests_mock.get(
        url,
        [
            {"status_code": 429, "headers": {"Retry-After": "1"}, "json": {"error": "rate"}},
            {"status_code": 200, "json": {"records": [{"id": 1}], "nextToken": ""}},
        ],
    )

    out = fetch_collection(access_token="AT", path="/cycle", since=None, limit=25)
    assert out == [{"id": 1}]
    assert sleeps == [1.0]


def test_backoff_retries_5xx_then_succeeds(requests_mock, monkeypatch):
    url = f"{WHOOP_API_BASE}/recovery"

    sleeps = []

    def fake_sleep(x):
        sleeps.append(float(x))

    # IMPORTANT: patch the sleep used inside the module under test
    monkeypatch.setattr("fit_adv.io_whoop_api.time.sleep", fake_sleep)

    requests_mock.get(
        url,
        [
            {"status_code": 500, "text": "nope"},
            {"status_code": 502, "text": "still nope"},
            {"status_code": 200, "json": {"records": [{"id": "ok"}], "nextToken": ""}},
        ],
    )

    out = fetch_collection(access_token="AT", path="/recovery", since=None, limit=25)
    assert out == [{"id": "ok"}]

    # Exponential backoff starts at 1.0 then 2.0 for the second 5xx
    assert sleeps[:2] == [1.0, 2.0]

