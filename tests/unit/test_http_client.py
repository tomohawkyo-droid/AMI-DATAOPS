"""Tests for ami.utils.http_client retry logic."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from pydantic import ValidationError

from ami.core.exceptions import StorageError
from ami.utils.http_client import (
    BACKOFF_MULTIPLIER,
    DEFAULT_MAX_RETRIES,
    INITIAL_RETRY_DELAY,
    MAX_RETRY_DELAY,
    RetryConfig,
    request_with_retry,
)

# ---------------------------------------------------------------
# Constants
# ---------------------------------------------------------------

_URL = "https://api.example.com/v1/items"
_METHOD = "GET"

_HTTP_OK = 200
_HTTP_RATE_LIMIT = 429
_HTTP_SERVER_ERROR = 500
_SINGLE_CALL = 1
_TWO_CALLS = 2
_CUSTOM_MAX_RETRIES = 5
_CUSTOM_INITIAL_DELAY = 1.0
_CUSTOM_MAX_DELAY = 30.0
_CUSTOM_BACKOFF = 3.0


# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------


def _mock_response(
    status: int = _HTTP_OK,
    reason: str = "OK",
    text: str = "",
) -> MagicMock:
    """Build a mock aiohttp.ClientResponse."""
    resp = MagicMock()
    resp.status = status
    resp.reason = reason
    resp.text = AsyncMock(return_value=text)
    resp.release = AsyncMock()
    return resp


def _mock_session(
    *responses: MagicMock,
    side_effects: (list[Exception | MagicMock] | None) = None,
) -> MagicMock:
    """Build a mock aiohttp.ClientSession.

    Accepts either a sequence of response objects or explicit
    side_effects (mix of exceptions and responses).
    """
    session = MagicMock(spec=aiohttp.ClientSession)
    if side_effects is not None:
        session.request = AsyncMock(side_effect=side_effects)
    else:
        session.request = AsyncMock(side_effect=list(responses))
    return session


# ===============================================================
# RetryConfig
# ===============================================================


class TestRetryConfig:
    """Verify RetryConfig defaults and immutability."""

    def test_defaults_match_module_constants(self) -> None:
        cfg = RetryConfig()
        assert cfg.max_retries == DEFAULT_MAX_RETRIES
        assert cfg.initial_delay == INITIAL_RETRY_DELAY
        assert cfg.max_delay == MAX_RETRY_DELAY
        assert cfg.backoff_multiplier == BACKOFF_MULTIPLIER

    def test_custom_values_override_defaults(self) -> None:
        cfg = RetryConfig(
            max_retries=_CUSTOM_MAX_RETRIES,
            initial_delay=_CUSTOM_INITIAL_DELAY,
            max_delay=_CUSTOM_MAX_DELAY,
            backoff_multiplier=_CUSTOM_BACKOFF,
        )
        assert cfg.max_retries == _CUSTOM_MAX_RETRIES
        assert cfg.initial_delay == _CUSTOM_INITIAL_DELAY
        assert cfg.max_delay == _CUSTOM_MAX_DELAY
        assert cfg.backoff_multiplier == _CUSTOM_BACKOFF

    def test_frozen_model_rejects_mutation(self) -> None:
        cfg = RetryConfig()
        with pytest.raises(ValidationError):
            cfg.max_retries = 10


# ===============================================================
# Successful request (no retry)
# ===============================================================


class TestRequestSuccess:
    """200 response returns immediately with no retry."""

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_returns_on_200(self, mock_sleep: AsyncMock) -> None:
        ok = _mock_response(_HTTP_OK, "OK")
        session = _mock_session(ok)

        result = await request_with_retry(session, _METHOD, _URL)

        assert result is ok
        assert session.request.call_count == _SINGLE_CALL
        mock_sleep.assert_not_awaited()


# ===============================================================
# Retry on 500
# ===============================================================


class TestRetryOn500:
    """Server error triggers retry; success on second try."""

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_retries_once_on_500(self, mock_sleep: AsyncMock) -> None:
        err = _mock_response(_HTTP_SERVER_ERROR, "Internal Server Error")
        ok = _mock_response(_HTTP_OK, "OK")
        session = _mock_session(err, ok)

        result = await request_with_retry(session, _METHOD, _URL)

        assert result is ok
        assert session.request.call_count == _TWO_CALLS
        err.release.assert_awaited_once()
        mock_sleep.assert_awaited_once()


# ===============================================================
# Retry on 429
# ===============================================================


class TestRetryOn429:
    """Rate-limit status triggers retry."""

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_retries_on_429(self, mock_sleep: AsyncMock) -> None:
        rate = _mock_response(_HTTP_RATE_LIMIT, "Too Many Requests")
        ok = _mock_response(_HTTP_OK, "OK")
        session = _mock_session(rate, ok)

        result = await request_with_retry(session, _METHOD, _URL)

        assert result is ok
        assert session.request.call_count == _TWO_CALLS
        rate.release.assert_awaited_once()
        mock_sleep.assert_awaited_once()


# ===============================================================
# Retry on TimeoutError
# ===============================================================


class TestRetryOnTimeout:
    """TimeoutError triggers retry; success on second try."""

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_retries_on_timeout(self, mock_sleep: AsyncMock) -> None:
        ok = _mock_response(_HTTP_OK, "OK")
        session = _mock_session(
            side_effects=[
                TimeoutError("timed out"),
                ok,
            ]
        )

        result = await request_with_retry(session, _METHOD, _URL)

        assert result is ok
        assert session.request.call_count == _TWO_CALLS
        mock_sleep.assert_awaited_once()


# ===============================================================
# Retry on aiohttp.ClientError
# ===============================================================


class TestRetryOnClientError:
    """aiohttp.ClientError triggers retry."""

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_retries_on_client_error(self, mock_sleep: AsyncMock) -> None:
        ok = _mock_response(_HTTP_OK, "OK")
        session = _mock_session(
            side_effects=[
                aiohttp.ClientError("conn reset"),
                ok,
            ]
        )

        result = await request_with_retry(session, _METHOD, _URL)

        assert result is ok
        assert session.request.call_count == _TWO_CALLS
        mock_sleep.assert_awaited_once()


# ===============================================================
# Max retries exhausted -- 500
# ===============================================================


class TestMaxRetriesExhausted500:
    """All attempts return 500; raises StorageError."""

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_raises_after_all_retries(self, mock_sleep: AsyncMock) -> None:
        cfg = RetryConfig(max_retries=3)
        responses = [
            _mock_response(
                _HTTP_SERVER_ERROR,
                "Internal Server Error",
                "bad",
            )
            for _ in range(cfg.max_retries)
        ]
        session = _mock_session(*responses)

        with pytest.raises(StorageError, match="HTTP"):
            await request_with_retry(
                session,
                _METHOD,
                _URL,
                retry_cfg=cfg,
            )

        assert session.request.call_count == cfg.max_retries


# ===============================================================
# Max retries exhausted -- TimeoutError
# ===============================================================


class TestMaxRetriesExhaustedTimeout:
    """All attempts raise TimeoutError; raises StorageError."""

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_raises_after_all_timeouts(self, mock_sleep: AsyncMock) -> None:
        cfg = RetryConfig(max_retries=3)
        errors = [TimeoutError("timeout") for _ in range(cfg.max_retries)]
        session = _mock_session(side_effects=errors)

        with pytest.raises(StorageError, match="failed after"):
            await request_with_retry(
                session,
                _METHOD,
                _URL,
                retry_cfg=cfg,
            )

        assert session.request.call_count == cfg.max_retries


# ===============================================================
# Non-retryable 200 returns immediately
# ===============================================================


class TestNonRetryableSuccess:
    """200 on first try returns the response object."""

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_200_returns_response(self, mock_sleep: AsyncMock) -> None:
        ok = _mock_response(_HTTP_OK, "OK", '{"data": 1}')
        session = _mock_session(ok)

        result = await request_with_retry(
            session,
            "POST",
            _URL,
            json={"key": "val"},
        )

        assert result.status == _HTTP_OK
        body = await result.text()
        assert body == '{"data": 1}'
        mock_sleep.assert_not_awaited()


# ===============================================================
# Backoff delay verification
# ===============================================================

_BACKOFF_MAX_RETRIES = 5
_BACKOFF_INIT_DELAY = 1.0
_BACKOFF_MAX_DELAY = 8.0
_BACKOFF_MULT = 2.0
_CAPPED_MAX_RETRIES = 4
_CAPPED_INIT_DELAY = 5.0


class TestBackoffDelay:
    """Verify exponential backoff doubles up to max_delay."""

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_delays_double_between_retries(self, mock_sleep: AsyncMock) -> None:
        cfg = RetryConfig(
            max_retries=_BACKOFF_MAX_RETRIES,
            initial_delay=_BACKOFF_INIT_DELAY,
            max_delay=_BACKOFF_MAX_DELAY,
            backoff_multiplier=_BACKOFF_MULT,
        )
        responses = [
            _mock_response(_HTTP_SERVER_ERROR, "Error") for _ in range(cfg.max_retries)
        ]
        session = _mock_session(*responses)

        with pytest.raises(StorageError):
            await request_with_retry(
                session,
                _METHOD,
                _URL,
                retry_cfg=cfg,
            )

        # Retries on attempts 0..3 (not the last).
        # Delays: 1.0, 2.0, 4.0, 8.0
        sleep_calls = [c.args[0] for c in mock_sleep.await_args_list]
        expected = [1.0, 2.0, 4.0, 8.0]
        assert sleep_calls == expected

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_delay_capped_at_max(self, mock_sleep: AsyncMock) -> None:
        cfg = RetryConfig(
            max_retries=_CAPPED_MAX_RETRIES,
            initial_delay=_CAPPED_INIT_DELAY,
            max_delay=_BACKOFF_MAX_DELAY,
            backoff_multiplier=_BACKOFF_MULT,
        )
        responses = [
            _mock_response(_HTTP_SERVER_ERROR, "Error") for _ in range(cfg.max_retries)
        ]
        session = _mock_session(*responses)

        with pytest.raises(StorageError):
            await request_with_retry(
                session,
                _METHOD,
                _URL,
                retry_cfg=cfg,
            )

        sleep_calls = [c.args[0] for c in mock_sleep.await_args_list]
        # 5.0, min(10.0, 8.0)=8.0, min(16.0, 8.0)=8.0
        expected = [5.0, 8.0, 8.0]
        assert sleep_calls == expected
