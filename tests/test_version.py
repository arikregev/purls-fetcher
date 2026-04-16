"""Tests for version parsing, latest-version logic, aging calculations, and counting."""

from datetime import date, datetime, timezone
from unittest.mock import patch

import pytest
import semver

from main import (
    NOT_FOUND,
    calculate_aging,
    calculate_latest_aging,
    calculate_latest_aging_months,
    count_newer_versions,
    determine_latest_stable,
    get_earliest_version,
    parse_version_flexible,
)


# ---------------------------------------------------------------------------
# parse_version_flexible
# ---------------------------------------------------------------------------


class TestParseVersionFlexible:
    def test_standard_semver(self):
        v = parse_version_flexible("1.2.3")
        assert v == semver.Version.parse("1.2.3")

    def test_v_prefix(self):
        v = parse_version_flexible("v1.9.0")
        assert v == semver.Version.parse("1.9.0")

    def test_capital_v_prefix(self):
        v = parse_version_flexible("V2.0.0")
        assert v == semver.Version.parse("2.0.0")

    def test_two_segment(self):
        v = parse_version_flexible("1.2")
        assert v is not None
        assert v.major == 1
        assert v.minor == 2

    def test_single_segment(self):
        v = parse_version_flexible("5")
        assert v is not None
        assert v.major == 5

    def test_prerelease(self):
        v = parse_version_flexible("1.0.0-alpha.1")
        assert v is not None
        assert v.prerelease == "alpha.1"

    def test_unparseable(self):
        assert parse_version_flexible("not-a-version") is None

    def test_empty_string(self):
        assert parse_version_flexible("") is None

    def test_whitespace(self):
        v = parse_version_flexible("  1.0.0  ")
        assert v == semver.Version.parse("1.0.0")


# ---------------------------------------------------------------------------
# determine_latest_stable
# ---------------------------------------------------------------------------

_dt = lambda y, m, d: datetime(y, m, d, tzinfo=timezone.utc)


class TestDetermineLatestStable:
    def test_all_stable(self):
        versions = {
            "1.0.0": _dt(2023, 1, 1),
            "1.1.0": _dt(2023, 6, 1),
            "2.0.0": _dt(2024, 1, 1),
        }
        ver, dt = determine_latest_stable(versions, "1.0.0")
        assert ver == "2.0.0"
        assert dt == _dt(2024, 1, 1)

    def test_mixed_stable_prerelease_current_stable(self):
        versions = {
            "1.0.0": _dt(2023, 1, 1),
            "2.0.0": _dt(2024, 1, 1),
            "3.0.0-beta.1": _dt(2024, 6, 1),
        }
        ver, dt = determine_latest_stable(versions, "1.0.0")
        assert ver == "2.0.0"

    def test_current_is_prerelease_includes_prereleases(self):
        versions = {
            "1.0.0": _dt(2023, 1, 1),
            "2.0.0": _dt(2024, 1, 1),
            "3.0.0-beta.1": _dt(2024, 6, 1),
        }
        ver, dt = determine_latest_stable(versions, "1.0.0-alpha.1")
        assert ver == "3.0.0-beta.1"

    def test_empty_versions(self):
        ver, dt = determine_latest_stable({}, "1.0.0")
        assert ver == NOT_FOUND
        assert dt is None

    def test_all_unparseable(self):
        versions = {
            "foo": _dt(2023, 1, 1),
            "bar": _dt(2024, 1, 1),
        }
        ver, dt = determine_latest_stable(versions, "1.0.0")
        assert ver == NOT_FOUND

    def test_current_unparseable_defaults_stable(self):
        versions = {
            "1.0.0": _dt(2023, 1, 1),
            "2.0.0-rc1": _dt(2024, 1, 1),
        }
        ver, dt = determine_latest_stable(versions, "some-custom-tag")
        assert ver == "1.0.0"


# ---------------------------------------------------------------------------
# get_earliest_version
# ---------------------------------------------------------------------------


class TestGetEarliestVersion:
    def test_normal(self):
        versions = {
            "1.0.0": _dt(2023, 1, 1),
            "0.1.0": _dt(2020, 6, 15),
            "2.0.0": _dt(2024, 1, 1),
        }
        result = get_earliest_version(versions)
        assert result == ("0.1.0", _dt(2020, 6, 15))

    def test_single_version(self):
        versions = {"1.0.0": _dt(2023, 1, 1)}
        result = get_earliest_version(versions)
        assert result == ("1.0.0", _dt(2023, 1, 1))

    def test_empty(self):
        assert get_earliest_version({}) is None


# ---------------------------------------------------------------------------
# calculate_aging
# ---------------------------------------------------------------------------


class TestCalculateAging:
    def test_both_dates(self):
        latest = _dt(2024, 6, 1)
        deployed = _dt(2024, 1, 1)
        result = calculate_aging(latest, deployed, None)
        assert result == "152"

    def test_deployed_missing_fallback(self):
        latest = _dt(2024, 6, 1)
        earliest = _dt(2020, 1, 1)
        result = calculate_aging(latest, None, earliest)
        assert int(result) > 0

    def test_all_missing(self):
        assert calculate_aging(None, None, None) == NOT_FOUND

    def test_negative_clamped(self):
        latest = _dt(2023, 1, 1)
        deployed = _dt(2024, 1, 1)
        result = calculate_aging(latest, deployed, None)
        assert result == "0"


# ---------------------------------------------------------------------------
# calculate_latest_aging
# ---------------------------------------------------------------------------


class TestCalculateLatestAging:
    def test_date_present(self):
        past = _dt(2024, 1, 1)
        result = calculate_latest_aging(past)
        assert int(result) > 0

    def test_none(self):
        assert calculate_latest_aging(None) == NOT_FOUND

    def test_negative_clamped(self):
        future = _dt(2099, 1, 1)
        result = calculate_latest_aging(future)
        assert result == "0"


# ---------------------------------------------------------------------------
# calculate_latest_aging_months
# ---------------------------------------------------------------------------


class TestCalculateLatestAgingMonths:
    @patch("main.date")
    def test_exact_months(self, mock_date):
        mock_date.today.return_value = date(2024, 7, 15)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        latest = _dt(2024, 1, 15)
        result = calculate_latest_aging_months(latest)
        assert result == "6"

    @patch("main.date")
    def test_day_not_reached(self, mock_date):
        mock_date.today.return_value = date(2024, 7, 10)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        latest = _dt(2024, 1, 15)
        result = calculate_latest_aging_months(latest)
        assert result == "5"

    def test_none(self):
        assert calculate_latest_aging_months(None) == NOT_FOUND


# ---------------------------------------------------------------------------
# count_newer_versions
# ---------------------------------------------------------------------------


class TestCountNewerVersions:
    def test_basic(self):
        versions = {
            "1.0.0": _dt(2023, 1, 1),
            "1.1.0": _dt(2023, 6, 1),
            "2.0.0": _dt(2024, 1, 1),
            "2.1.0": _dt(2024, 6, 1),
        }
        assert count_newer_versions(versions, "1.0.0") == "3"

    def test_already_latest(self):
        versions = {
            "1.0.0": _dt(2023, 1, 1),
            "2.0.0": _dt(2024, 1, 1),
        }
        assert count_newer_versions(versions, "2.0.0") == "0"

    def test_excludes_prerelease_when_stable(self):
        versions = {
            "1.0.0": _dt(2023, 1, 1),
            "2.0.0": _dt(2024, 1, 1),
            "3.0.0-beta.1": _dt(2024, 6, 1),
        }
        assert count_newer_versions(versions, "1.0.0") == "1"

    def test_unparseable_current(self):
        versions = {"1.0.0": _dt(2023, 1, 1)}
        assert count_newer_versions(versions, "not-a-version") == NOT_FOUND

    def test_empty(self):
        assert count_newer_versions({}, "1.0.0") == "0"
