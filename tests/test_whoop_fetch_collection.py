import pytest

from fit_adv.io_whoop_api import WHOOP_API_BASE, WhoopApiError, fetch_collection


def test_fetch_collection_single_page(requests_mock):
    url = f"{WHOOP_API_BASE}/cycle"

    requests_mock.get(
        url,
        json={"records": [{"id": 1}, {"id": 2}], "nextToken": None},
        status_code=200,
    )

    out = fetch_collection(access_token="AT", path="/cycle", since="2026-01-01T00:00:00Z", limit=10)

    assert out == [{"id": 1}, {"id": 2}]

    req = requests_mock.last_request
    assert req.headers["Authorization"] == "Bearer AT"
    # query params are in req.qs dict
    assert req.qs["limit"] == ["10"]
    assert req.qs["start"][0].lower() == "2026-01-01t00:00:00z"
    assert "nextToken" not in req.qs


def test_fetch_collection_paginates_with_nextToken(requests_mock):
    url = f"{WHOOP_API_BASE}/recovery"

    # First page returns nextToken
    requests_mock.get(
        url,
        [
            {"json": {"records": [{"id": "a"}], "nextToken": "N1"}, "status_code": 200},
            {"json": {"records": [{"id": "b"}], "nextToken": ""}, "status_code": 200},
        ],
    )

    out = fetch_collection(access_token="AT", path="/recovery", since="2026-01-01T00:00:00Z", limit=25)
    assert out == [{"id": "a"}, {"id": "b"}]

    history = requests_mock.request_history
    assert len(history) == 2

    # First request: start + limit
    assert history[0].qs["limit"] == ["25"]
    assert history[0].qs["start"] == ["2026-01-01t00:00:00z"]
    assert "nextToken" not in history[0].qs

    # Second request: nextToken present
    assert history[1].qs["limit"] == ["25"]
    assert history[1].qs["start"] == ["2026-01-01t00:00:00z"]
    qs2 = history[1].qs
    sent = (qs2.get("nextToken") or qs2.get("nexttoken") or qs2.get("next_token"))
    assert sent is not None
    assert sent[0].lower() == "n1"




def test_fetch_collection_clamps_limit_to_25(requests_mock):
    url = f"{WHOOP_API_BASE}/activity/sleep"

    requests_mock.get(url, json={"records": [], "nextToken": ""}, status_code=200)

    fetch_collection(access_token="AT", path="/activity/sleep", since=None, limit=999)

    req = requests_mock.last_request
    assert req.qs["limit"] == ["25"]


def test_fetch_collection_non_200_raises(requests_mock):
    url = f"{WHOOP_API_BASE}/activity/workout"
    requests_mock.get(url, text="nope", status_code=401)

    with pytest.raises(WhoopApiError) as e:
        fetch_collection(access_token="AT", path="/activity/workout", since=None, limit=25)

    assert "GET /activity/workout failed" in str(e.value)

