import asyncio

import aiohttp
import pytest

from crawler.fetcher.async_fetcher import AsyncFetcher


class _FakeContent:
    def __init__(self, data=b"", exc: Exception | None = None):
        self._data = data
        self._exc = exc
        self.read_called = False

    async def read(self, _limit: int):
        self.read_called = True
        if self._exc:
            raise self._exc
        return self._data


class _FakeResponse:
    def __init__(
        self,
        status: int = 200,
        headers: dict | None = None,
        body: bytes = b"",
        url: str = "https://example.com/final",
        read_exc: Exception | None = None,
    ):
        self.status = status
        self.headers = headers or {}
        self.url = url
        self.content = _FakeContent(body, read_exc)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def get_encoding(self):
        return "utf-8"


class _FakeSession:
    def __init__(self, response: _FakeResponse | None = None, exc: Exception | None = None):
        self._response = response
        self._exc = exc

    def get(self, *_args, **_kwargs):
        if self._exc:
            raise self._exc
        return self._response


@pytest.mark.asyncio
async def test_fetch_non_html_skips_body_read():
    fetcher = AsyncFetcher()
    resp = _FakeResponse(status=200, headers={"Content-Type": "image/png"}, body=b"binary")
    fetcher._session = _FakeSession(response=resp)

    result = await fetcher.fetch("https://example.com/img")
    assert result.status == 200
    assert result.html == ""
    assert resp.content.read_called is False


@pytest.mark.asyncio
async def test_fetch_html_reads_and_decodes_body():
    fetcher = AsyncFetcher()
    body = "<html><a href='/a'>A</a></html>".encode("utf-8")
    resp = _FakeResponse(
        status=200,
        headers={"Content-Type": "text/html; charset=utf-8"},
        body=body,
        url="https://example.com/final",
    )
    fetcher._session = _FakeSession(response=resp)

    result = await fetcher.fetch("https://example.com/start")
    assert result.status == 200
    assert "html" in result.html.lower()
    assert result.redirect_url == "https://example.com/final"
    assert resp.content.read_called is True


@pytest.mark.asyncio
async def test_fetch_read_error_returns_error_field():
    fetcher = AsyncFetcher()
    resp = _FakeResponse(
        status=200,
        headers={"Content-Type": "text/html"},
        read_exc=RuntimeError("boom"),
    )
    fetcher._session = _FakeSession(response=resp)

    result = await fetcher.fetch("https://example.com")
    assert result.status == 200
    assert "read error" in result.error


@pytest.mark.asyncio
async def test_fetch_timeout_returns_timeout_error():
    fetcher = AsyncFetcher()
    fetcher._session = _FakeSession(exc=asyncio.TimeoutError())

    result = await fetcher.fetch("https://timeout.test")
    assert result.status == 0
    assert result.error == "timeout"


@pytest.mark.asyncio
async def test_fetch_client_error_returns_error_message():
    fetcher = AsyncFetcher()
    fetcher._session = _FakeSession(exc=aiohttp.ClientError("network down"))

    result = await fetcher.fetch("https://err.test")
    assert result.status == 0
    assert "network down" in result.error
