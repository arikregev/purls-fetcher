"""Tests for ecosystem handlers with mocked HTTP responses."""

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from packageurl import PackageURL

from main import (
    _encode_go_module_path,
    fetch_cargo,
    fetch_golang,
    fetch_maven,
    fetch_npm,
    fetch_nuget,
    fetch_pypi,
    fetch_rubygems,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_session(response_data, status=200, text_data=None):
    """Create a mock aiohttp session that returns the given JSON or text."""
    mock_resp = AsyncMock()
    mock_resp.status = status
    mock_resp.json = AsyncMock(return_value=response_data)
    mock_resp.text = AsyncMock(return_value=text_data or "")
    mock_resp.headers = {}
    mock_resp.raise_for_status = MagicMock()

    if status >= 400:
        from aiohttp import ClientResponseError, RequestInfo
        from yarl import URL

        exc = ClientResponseError(
            request_info=RequestInfo(url=URL("http://test"), method="GET", headers={}, real_url=URL("http://test")),
            history=(),
            status=status,
        )
        mock_resp.raise_for_status.side_effect = exc

    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_resp)
    ctx.__aexit__ = AsyncMock(return_value=False)

    session = AsyncMock()
    session.get = MagicMock(return_value=ctx)
    return session


def _make_multi_session(responses):
    """Create a session that returns different responses for sequential calls."""
    session = AsyncMock()
    ctxs = []
    for resp_data, status, is_text in responses:
        mock_resp = AsyncMock()
        mock_resp.status = status
        mock_resp.json = AsyncMock(return_value=resp_data)
        mock_resp.text = AsyncMock(return_value=resp_data if is_text else "")
        mock_resp.headers = {}
        mock_resp.raise_for_status = MagicMock()
        if status >= 400:
            from aiohttp import ClientResponseError, RequestInfo
            from yarl import URL
            exc = ClientResponseError(
                request_info=RequestInfo(url=URL("http://test"), method="GET", headers={}, real_url=URL("http://test")),
                history=(),
                status=status,
            )
            mock_resp.raise_for_status.side_effect = exc

        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=mock_resp)
        ctx.__aexit__ = AsyncMock(return_value=False)
        ctxs.append(ctx)

    session.get = MagicMock(side_effect=ctxs)
    return session


# ---------------------------------------------------------------------------
# Maven
# ---------------------------------------------------------------------------


class TestMavenHandler:
    def test_normal_response(self):
        data = {
            "response": {
                "numFound": 2,
                "docs": [
                    {"v": "3.12.0", "timestamp": 1636502400000},
                    {"v": "3.13.0", "timestamp": 1700000000000},
                ],
            }
        }
        session = _make_session(data)
        purl = PackageURL.from_string("pkg:maven/org.apache.commons/commons-lang3@3.12.0")
        result = asyncio.run(fetch_maven(session, purl))
        assert "3.12.0" in result.versions
        assert "3.13.0" in result.versions
        assert not result.error

    def test_404(self):
        session = _make_session({}, status=404)
        purl = PackageURL.from_string("pkg:maven/com.example/nonexistent@1.0.0")
        result = asyncio.run(fetch_maven(session, purl))
        assert result.versions == {}

    def test_missing_namespace(self):
        purl = PackageURL(type="maven", name="artifact")
        session = _make_session({})
        result = asyncio.run(fetch_maven(session, purl))
        assert result.error


# ---------------------------------------------------------------------------
# PyPI
# ---------------------------------------------------------------------------


class TestPyPIHandler:
    def test_normal_response(self):
        data = {
            "releases": {
                "2.28.0": [{"upload_time_iso_8601": "2022-06-29T15:00:00.000000Z"}],
                "2.31.0": [{"upload_time_iso_8601": "2023-05-22T12:00:00.000000Z"}],
                "2.32.0": [],
            }
        }
        session = _make_session(data)
        purl = PackageURL.from_string("pkg:pypi/requests@2.28.0")
        result = asyncio.run(fetch_pypi(session, purl))
        assert "2.28.0" in result.versions
        assert "2.31.0" in result.versions
        assert "2.32.0" not in result.versions

    def test_404(self):
        session = _make_session({}, status=404)
        purl = PackageURL.from_string("pkg:pypi/nonexistent@1.0.0")
        result = asyncio.run(fetch_pypi(session, purl))
        assert result.versions == {}


# ---------------------------------------------------------------------------
# NPM
# ---------------------------------------------------------------------------


class TestNPMHandler:
    def test_normal_response(self):
        data = {
            "time": {
                "created": "2010-12-29T00:00:00.000Z",
                "modified": "2024-01-01T00:00:00.000Z",
                "4.18.2": "2022-10-08T14:00:00.000Z",
                "4.19.0": "2024-03-25T12:00:00.000Z",
            }
        }
        session = _make_session(data)
        purl = PackageURL.from_string("pkg:npm/express@4.18.2")
        result = asyncio.run(fetch_npm(session, purl))
        assert "4.18.2" in result.versions
        assert "4.19.0" in result.versions
        assert "created" not in result.versions
        assert "modified" not in result.versions

    def test_scoped_package(self):
        data = {"time": {"18.0.0": "2024-04-01T00:00:00.000Z"}}
        session = _make_session(data)
        purl = PackageURL.from_string("pkg:npm/%40angular/core@18.0.0")
        result = asyncio.run(fetch_npm(session, purl))
        assert "18.0.0" in result.versions


# ---------------------------------------------------------------------------
# Go
# ---------------------------------------------------------------------------


class TestGoHandler:
    def test_module_path_encoding(self):
        assert _encode_go_module_path("github.com/Azure/go-autorest") == (
            "github.com/!azure/go-autorest"
        )

    def test_normal_response(self):
        list_text = "v1.8.0\nv1.9.0\n"
        info_1 = {"Version": "v1.8.0", "Time": "2022-02-01T00:00:00Z"}
        info_2 = {"Version": "v1.9.0", "Time": "2023-05-01T00:00:00Z"}

        responses = [
            (list_text, 200, True),
            (info_1, 200, False),
            (info_2, 200, False),
        ]
        session = _make_multi_session(responses)
        purl = PackageURL.from_string("pkg:golang/github.com/sirupsen/logrus@v1.9.0")
        result = asyncio.run(fetch_golang(session, purl))
        assert "v1.8.0" in result.versions
        assert "v1.9.0" in result.versions

    def test_404(self):
        session = _make_session({}, status=404, text_data="")
        purl = PackageURL.from_string("pkg:golang/example.com/nonexistent@v1.0.0")
        result = asyncio.run(fetch_golang(session, purl))
        assert result.versions == {}


# ---------------------------------------------------------------------------
# NuGet
# ---------------------------------------------------------------------------


class TestNuGetHandler:
    def test_inline_items(self):
        data = {
            "items": [
                {
                    "items": [
                        {
                            "catalogEntry": {
                                "version": "13.0.1",
                                "published": "2021-03-09T00:00:00+00:00",
                                "listed": True,
                            }
                        },
                        {
                            "catalogEntry": {
                                "version": "13.0.3",
                                "published": "2023-03-08T00:00:00+00:00",
                                "listed": True,
                            }
                        },
                    ]
                }
            ]
        }
        session = _make_session(data)
        purl = PackageURL.from_string("pkg:nuget/Newtonsoft.Json@13.0.1")
        result = asyncio.run(fetch_nuget(session, purl))
        assert "13.0.1" in result.versions
        assert "13.0.3" in result.versions

    def test_unlisted_skipped(self):
        data = {
            "items": [
                {
                    "items": [
                        {
                            "catalogEntry": {
                                "version": "1.0.0",
                                "published": "1900-01-01T00:00:00+00:00",
                                "listed": False,
                            }
                        },
                        {
                            "catalogEntry": {
                                "version": "2.0.0",
                                "published": "2024-01-01T00:00:00+00:00",
                                "listed": True,
                            }
                        },
                    ]
                }
            ]
        }
        session = _make_session(data)
        purl = PackageURL.from_string("pkg:nuget/SomePackage@1.0.0")
        result = asyncio.run(fetch_nuget(session, purl))
        assert "1.0.0" not in result.versions
        assert "2.0.0" in result.versions


# ---------------------------------------------------------------------------
# Cargo
# ---------------------------------------------------------------------------


class TestCargoHandler:
    def test_normal_response(self):
        data = {
            "versions": [
                {"num": "1.0.160", "created_at": "2023-04-01T00:00:00Z", "yanked": False},
                {"num": "1.0.170", "created_at": "2023-09-01T00:00:00Z", "yanked": False},
                {"num": "1.0.155", "created_at": "2023-02-01T00:00:00Z", "yanked": True},
            ]
        }
        session = _make_session(data)
        purl = PackageURL.from_string("pkg:cargo/serde@1.0.160")
        result = asyncio.run(fetch_cargo(session, purl))
        assert "1.0.160" in result.versions
        assert "1.0.170" in result.versions
        assert "1.0.155" not in result.versions  # yanked


# ---------------------------------------------------------------------------
# RubyGems
# ---------------------------------------------------------------------------


class TestRubyGemsHandler:
    def test_normal_response(self):
        data = [
            {"number": "7.0.4", "created_at": "2022-09-09T00:00:00Z", "prerelease": False},
            {"number": "7.1.0", "created_at": "2023-10-05T00:00:00Z", "prerelease": False},
            {"number": "7.2.0.beta1", "created_at": "2024-02-01T00:00:00Z", "prerelease": True},
        ]
        session = _make_session(data)
        purl = PackageURL.from_string("pkg:gem/rails@7.0.4")
        result = asyncio.run(fetch_rubygems(session, purl))
        assert "7.0.4" in result.versions
        assert "7.1.0" in result.versions
        assert "7.2.0.beta1" in result.versions  # included in metadata; filtering at enrichment

    def test_404(self):
        session = _make_session([], status=404)
        purl = PackageURL.from_string("pkg:gem/nonexistent@1.0.0")
        result = asyncio.run(fetch_rubygems(session, purl))
        assert result.versions == {}
