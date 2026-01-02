import pytest
from urllib.parse import parse_qs

from fit_adv.io_whoop_api import WHOOP_TOKEN_URL, WhoopApiError, refresh_access_token


def test_refresh_access_token_success_no_rotation(requests_mock, monkeypatch):
    # Track whether persist was called
    called = {"persist": False}

    def fake_persist(new_rt, *args, **kwargs):
        called["persist"] = True

    monkeypatch.setattr("fit_adv.io_whoop_api._persist_refresh_token", fake_persist)

    requests_mock.post(
        WHOOP_TOKEN_URL,
        json={"access_token": "AT", "refresh_token": "RT_SAME", "expires_in": 3600},
        status_code=200,
    )

    tokens = refresh_access_token(
        client_id="cid",
        client_secret="csecret",
        refresh_token="RT_SAME",
        scope="offline read:cycles",
    )

    assert tokens.access_token == "AT"
    assert tokens.refresh_token == "RT_SAME"
    assert tokens.expires_in == 3600
    assert called["persist"] is False

    # Verify outgoing form fields
    req = requests_mock.last_request
    raw = req.body
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")

    qs = parse_qs(raw)

    assert qs["grant_type"] == ["refresh_token"]
    assert qs["client_id"] == ["cid"]
    assert qs["client_secret"] == ["csecret"]
    assert qs["refresh_token"] == ["RT_SAME"]
    assert qs["scope"] == ["offline read:cycles"]


def test_refresh_access_token_rotates_refresh_token(requests_mock, monkeypatch):
    persisted = {"value": None}

    def fake_persist(new_rt, *args, **kwargs):
        persisted["value"] = new_rt

    monkeypatch.setattr("fit_adv.io_whoop_api._persist_refresh_token", fake_persist)

    requests_mock.post(
        WHOOP_TOKEN_URL,
        json={"access_token": "AT2", "refresh_token": "RT_NEW", "expires_in": 3600},
        status_code=200,
    )

    tokens = refresh_access_token(
        client_id="cid",
        client_secret="csecret",
        refresh_token="RT_OLD",
        scope="offline",
    )

    assert tokens.access_token == "AT2"
    assert persisted["value"] == "RT_NEW"


def test_refresh_access_token_non_200_raises(requests_mock):
    requests_mock.post(WHOOP_TOKEN_URL, text="nope", status_code=400)

    with pytest.raises(WhoopApiError) as e:
        refresh_access_token(
            client_id="cid",
            client_secret="csecret",
            refresh_token="rt",
        )

    assert "Token refresh failed" in str(e.value)

