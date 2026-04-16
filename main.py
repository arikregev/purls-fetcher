#!/usr/bin/env python3
"""PURL Metadata Fetcher — enriches a CSV of software dependencies with version metadata."""

import argparse
import asyncio
import csv
import json
import logging
import random
import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from urllib.parse import quote

import aiohttp
import semver
from packageurl import PackageURL

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NOT_FOUND = "NOT_FOUND"
ERROR = "ERROR"
MAX_RETRIES = 3
BASE_BACKOFF = 1.0

INPUT_COLUMNS = ["CSI_ID", "COMPONENT", "BUILD_ID", "PURL", "DEPLOYED_VERSION"]
ENRICHED_COLUMNS = [
    "PUBLISHED_AT",
    "LATEST_VERSION",
    "LATEST_VERSION_PUBLISHED_AT",
    "EARLIEST_REGISTRY_VERSION",
    "EARLIEST_VERSION_PUBLISHED_AT",
    "AGING",
    "LATEST_AGING",
    "LATEST_AGING_MONTHS",
    "NEWER_VERSIONS_COUNT",
]

logger = logging.getLogger("purls-fetcher")

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class PackageMetadata:
    versions: dict[str, datetime] = field(default_factory=dict)
    error: bool = False


# ---------------------------------------------------------------------------
# Version utilities
# ---------------------------------------------------------------------------


def parse_version_flexible(version_string: str) -> semver.Version | None:
    v = version_string.strip()
    if v.startswith(("v", "V")):
        v = v[1:]
    try:
        return semver.Version.parse(v)
    except ValueError:
        pass
    try:
        return semver.Version.parse(v, optional_minor_and_patch=True)
    except (ValueError, TypeError):
        pass
    return None


def determine_latest_stable(
    versions: dict[str, datetime], current_version: str
) -> tuple[str, datetime | None]:
    current_parsed = parse_version_flexible(current_version)
    include_prerelease = (
        current_parsed is not None and current_parsed.prerelease is not None
    )

    candidates: list[tuple[str, semver.Version, datetime]] = []
    for ver_str, pub_date in versions.items():
        parsed = parse_version_flexible(ver_str)
        if parsed is None:
            continue
        if not include_prerelease and parsed.prerelease is not None:
            continue
        candidates.append((ver_str, parsed, pub_date))

    if not candidates:
        return (NOT_FOUND, None)

    candidates.sort(key=lambda x: x[1])
    best = candidates[-1]
    return (best[0], best[2])


def get_earliest_version(
    versions: dict[str, datetime],
) -> tuple[str, datetime] | None:
    if not versions:
        return None
    earliest_ver = min(versions, key=lambda v: versions[v])
    return (earliest_ver, versions[earliest_ver])


def _clamp_aging(value: int, purl_str: str, column: str) -> int:
    if value < 0:
        logger.warning(
            "Negative %s (%d) for %s — clamping to 0", column, value, purl_str
        )
        return 0
    return value


def calculate_aging(
    latest_date: datetime | None,
    deployed_date: datetime | None,
    earliest_date: datetime | None,
    purl_str: str = "",
) -> str:
    if deployed_date and latest_date:
        days = (latest_date - deployed_date).days
        return str(_clamp_aging(days, purl_str, "AGING"))
    if latest_date and earliest_date:
        days = (latest_date - earliest_date).days
        return str(_clamp_aging(days, purl_str, "AGING"))
    return NOT_FOUND


def calculate_latest_aging(latest_date: datetime | None, purl_str: str = "") -> str:
    if latest_date is None:
        return NOT_FOUND
    days = (date.today() - latest_date.date()).days
    return str(_clamp_aging(days, purl_str, "LATEST_AGING"))


def calculate_latest_aging_months(
    latest_date: datetime | None, purl_str: str = ""
) -> str:
    if latest_date is None:
        return NOT_FOUND
    today = date.today()
    d = latest_date.date()
    months = (today.year - d.year) * 12 + (today.month - d.month)
    if today.day < d.day:
        months -= 1
    months = _clamp_aging(months, purl_str, "LATEST_AGING_MONTHS")
    return str(months)


def count_newer_versions(
    versions: dict[str, datetime], current_version: str
) -> str:
    current_parsed = parse_version_flexible(current_version)
    if current_parsed is None:
        return NOT_FOUND

    include_prerelease = current_parsed.prerelease is not None
    count = 0
    for ver_str in versions:
        parsed = parse_version_flexible(ver_str)
        if parsed is None:
            continue
        if not include_prerelease and parsed.prerelease is not None:
            continue
        if parsed > current_parsed:
            count += 1
    return str(count)


# ---------------------------------------------------------------------------
# HTTP utilities
# ---------------------------------------------------------------------------

RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


async def _request_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict | None = None,
    parse_json: bool = True,
) -> dict | str:
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    return (await resp.json()) if parse_json else (await resp.text())
                if resp.status in RETRYABLE_STATUSES:
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after and attempt < MAX_RETRIES:
                        delay = float(retry_after)
                    else:
                        delay = BASE_BACKOFF * (2**attempt) + random.random()
                    logger.debug(
                        "HTTP %d from %s — retrying in %.1fs", resp.status, url, delay
                    )
                    await asyncio.sleep(delay)
                    continue
                resp.raise_for_status()
        except aiohttp.ClientResponseError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                delay = BASE_BACKOFF * (2**attempt) + random.random()
                logger.debug(
                    "Connection error for %s — retrying in %.1fs: %s",
                    url,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)
            else:
                raise
    raise last_exc  # type: ignore[misc]


async def fetch_json(
    session: aiohttp.ClientSession, url: str, headers: dict | None = None
) -> dict:
    return await _request_with_retry(session, url, headers, parse_json=True)  # type: ignore[return-value]


async def fetch_text(
    session: aiohttp.ClientSession, url: str, headers: dict | None = None
) -> str:
    return await _request_with_retry(session, url, headers, parse_json=False)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------


class PackageCache:
    def __init__(self) -> None:
        self._store: dict[str, PackageMetadata] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()

    @staticmethod
    def make_key(purl: PackageURL) -> str:
        return PackageURL(
            type=purl.type, namespace=purl.namespace, name=purl.name
        ).to_string()

    async def get_or_fetch(
        self,
        purl: PackageURL,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        handler_fn,
    ) -> PackageMetadata:
        key = self.make_key(purl)

        if key in self._store:
            logger.debug("Cache hit: %s", key)
            return self._store[key]

        async with self._global_lock:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
            lock = self._locks[key]

        async with lock:
            if key in self._store:
                logger.debug("Cache hit (after lock): %s", key)
                return self._store[key]

            async with semaphore:
                metadata = await handler_fn(session, purl)
                self._store[key] = metadata
                return metadata


# ---------------------------------------------------------------------------
# Ecosystem handlers
# ---------------------------------------------------------------------------

ECOSYSTEM_HANDLERS: dict[str, object] = {}


def _register_handler(purl_type: str):
    def decorator(fn):
        ECOSYSTEM_HANDLERS[purl_type] = fn
        return fn
    return decorator


def _parse_iso(s: str) -> datetime:
    s = s.replace("Z", "+00:00")
    return datetime.fromisoformat(s)


# --- Maven ---

@_register_handler("maven")
async def fetch_maven(
    session: aiohttp.ClientSession, purl: PackageURL
) -> PackageMetadata:
    group_id = purl.namespace
    artifact_id = purl.name
    if not group_id:
        logger.warning("Maven PURL missing namespace (groupId): %s", purl.to_string())
        return PackageMetadata(error=True)

    versions: dict[str, datetime] = {}
    start = 0
    rows = 200
    while True:
        url = (
            f"https://search.maven.org/solrsearch/select"
            f"?q=g:{quote(group_id)}+AND+a:{quote(artifact_id)}"
            f"&core=gav&rows={rows}&start={start}&wt=json"
        )
        try:
            data = await fetch_json(session, url)
        except aiohttp.ClientResponseError as exc:
            if exc.status == 404:
                return PackageMetadata()
            logger.error("Maven API error for %s: %s", purl.to_string(), exc)
            return PackageMetadata(error=True)
        except Exception as exc:
            logger.error("Maven fetch failed for %s: %s", purl.to_string(), exc)
            return PackageMetadata(error=True)

        response = data.get("response", {})
        docs = response.get("docs", [])
        for doc in docs:
            v = doc.get("v")
            ts = doc.get("timestamp")
            if v and ts:
                versions[v] = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)

        num_found = response.get("numFound", 0)
        start += rows
        if start >= num_found:
            break

    return PackageMetadata(versions=versions)


# --- PyPI ---

@_register_handler("pypi")
async def fetch_pypi(
    session: aiohttp.ClientSession, purl: PackageURL
) -> PackageMetadata:
    url = f"https://pypi.org/pypi/{quote(purl.name, safe='')}/json"
    try:
        data = await fetch_json(session, url)
    except aiohttp.ClientResponseError as exc:
        if exc.status == 404:
            return PackageMetadata()
        logger.error("PyPI API error for %s: %s", purl.to_string(), exc)
        return PackageMetadata(error=True)
    except Exception as exc:
        logger.error("PyPI fetch failed for %s: %s", purl.to_string(), exc)
        return PackageMetadata(error=True)

    versions: dict[str, datetime] = {}
    releases = data.get("releases", {})
    for ver_str, dists in releases.items():
        if not dists:
            continue
        upload_time = dists[0].get("upload_time_iso_8601")
        if upload_time:
            versions[ver_str] = _parse_iso(upload_time)

    return PackageMetadata(versions=versions)


# --- NPM ---

@_register_handler("npm")
async def fetch_npm(
    session: aiohttp.ClientSession, purl: PackageURL
) -> PackageMetadata:
    if purl.namespace:
        pkg_name = f"{purl.namespace}%2F{purl.name}"
    else:
        pkg_name = purl.name
    url = f"https://registry.npmjs.org/{pkg_name}"
    try:
        data = await fetch_json(session, url)
    except aiohttp.ClientResponseError as exc:
        if exc.status == 404:
            return PackageMetadata()
        logger.error("NPM API error for %s: %s", purl.to_string(), exc)
        return PackageMetadata(error=True)
    except Exception as exc:
        logger.error("NPM fetch failed for %s: %s", purl.to_string(), exc)
        return PackageMetadata(error=True)

    versions: dict[str, datetime] = {}
    time_dict = data.get("time", {})
    skip_keys = {"modified", "created"}
    for ver_str, iso_date in time_dict.items():
        if ver_str in skip_keys:
            continue
        try:
            versions[ver_str] = _parse_iso(iso_date)
        except (ValueError, TypeError):
            pass

    return PackageMetadata(versions=versions)


# --- Go ---

def _encode_go_module_path(path: str) -> str:
    return re.sub(r"[A-Z]", lambda m: "!" + m.group(0).lower(), path)


@_register_handler("golang")
async def fetch_golang(
    session: aiohttp.ClientSession, purl: PackageURL
) -> PackageMetadata:
    if purl.namespace:
        module_path = f"{purl.namespace}/{purl.name}"
    else:
        module_path = purl.name
    encoded = _encode_go_module_path(module_path)

    list_url = f"https://proxy.golang.org/{encoded}/@v/list"
    try:
        text = await fetch_text(session, list_url)
    except aiohttp.ClientResponseError as exc:
        if exc.status in (404, 410):
            return PackageMetadata()
        logger.error("Go proxy error for %s: %s", purl.to_string(), exc)
        return PackageMetadata(error=True)
    except Exception as exc:
        logger.error("Go proxy fetch failed for %s: %s", purl.to_string(), exc)
        return PackageMetadata(error=True)

    version_list = [v.strip() for v in text.strip().split("\n") if v.strip()]
    if not version_list:
        return PackageMetadata()

    async def get_version_info(ver: str) -> tuple[str, datetime] | None:
        info_url = f"https://proxy.golang.org/{encoded}/@v/{ver}.info"
        try:
            info = await fetch_json(session, info_url)
            return (info["Version"], _parse_iso(info["Time"]))
        except Exception as exc:
            logger.debug("Go version info failed for %s@%s: %s", module_path, ver, exc)
            return None

    results = await asyncio.gather(
        *(get_version_info(v) for v in version_list), return_exceptions=True
    )

    versions: dict[str, datetime] = {}
    for r in results:
        if isinstance(r, tuple) and r is not None:
            versions[r[0]] = r[1]

    return PackageMetadata(versions=versions)


# --- NuGet ---

@_register_handler("nuget")
async def fetch_nuget(
    session: aiohttp.ClientSession, purl: PackageURL
) -> PackageMetadata:
    lowered = purl.name.lower()
    url = f"https://api.nuget.org/v3/registration5-gz-semver2/{lowered}/index.json"
    try:
        data = await fetch_json(session, url)
    except aiohttp.ClientResponseError as exc:
        if exc.status == 404:
            return PackageMetadata()
        logger.error("NuGet API error for %s: %s", purl.to_string(), exc)
        return PackageMetadata(error=True)
    except Exception as exc:
        logger.error("NuGet fetch failed for %s: %s", purl.to_string(), exc)
        return PackageMetadata(error=True)

    versions: dict[str, datetime] = {}
    pages = data.get("items", [])
    for page in pages:
        items = page.get("items")
        if items is None:
            page_url = page.get("@id")
            if not page_url:
                continue
            try:
                page_data = await fetch_json(session, page_url)
                items = page_data.get("items", [])
            except Exception as exc:
                logger.debug("NuGet page fetch failed: %s", exc)
                continue

        for item in items:
            entry = item.get("catalogEntry", {})
            ver = entry.get("version")
            published = entry.get("published", "")
            listed = entry.get("listed", True)
            if not listed or not ver:
                continue
            if published.startswith("1900"):
                continue
            try:
                versions[ver] = _parse_iso(published)
            except (ValueError, TypeError):
                pass

    return PackageMetadata(versions=versions)


# --- Cargo ---

@_register_handler("cargo")
async def fetch_cargo(
    session: aiohttp.ClientSession, purl: PackageURL
) -> PackageMetadata:
    url = f"https://crates.io/api/v1/crates/{quote(purl.name, safe='')}"
    headers = {"User-Agent": "purls-fetcher/1.0"}
    try:
        data = await fetch_json(session, url, headers=headers)
    except aiohttp.ClientResponseError as exc:
        if exc.status == 404:
            return PackageMetadata()
        logger.error("Cargo API error for %s: %s", purl.to_string(), exc)
        return PackageMetadata(error=True)
    except Exception as exc:
        logger.error("Cargo fetch failed for %s: %s", purl.to_string(), exc)
        return PackageMetadata(error=True)

    versions: dict[str, datetime] = {}
    for entry in data.get("versions", []):
        num = entry.get("num")
        created_at = entry.get("created_at")
        yanked = entry.get("yanked", False)
        if yanked or not num or not created_at:
            continue
        try:
            versions[num] = _parse_iso(created_at)
        except (ValueError, TypeError):
            pass

    return PackageMetadata(versions=versions)


# --- RubyGems ---

@_register_handler("gem")
async def fetch_rubygems(
    session: aiohttp.ClientSession, purl: PackageURL
) -> PackageMetadata:
    url = f"https://rubygems.org/api/v1/versions/{quote(purl.name, safe='')}.json"
    try:
        data = await fetch_json(session, url)
    except aiohttp.ClientResponseError as exc:
        if exc.status == 404:
            return PackageMetadata()
        logger.error("RubyGems API error for %s: %s", purl.to_string(), exc)
        return PackageMetadata(error=True)
    except Exception as exc:
        logger.error("RubyGems fetch failed for %s: %s", purl.to_string(), exc)
        return PackageMetadata(error=True)

    versions: dict[str, datetime] = {}
    if not isinstance(data, list):
        return PackageMetadata()
    for entry in data:
        number = entry.get("number")
        created_at = entry.get("created_at")
        if not number or not created_at:
            continue
        try:
            versions[number] = _parse_iso(created_at)
        except (ValueError, TypeError):
            pass

    return PackageMetadata(versions=versions)


# ---------------------------------------------------------------------------
# CSV I/O
# ---------------------------------------------------------------------------


def read_input_csv(filepath: str) -> list[dict]:
    with open(filepath, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            logger.error("CSV file is empty or has no header row")
            sys.exit(1)
        missing = [c for c in INPUT_COLUMNS if c not in reader.fieldnames]
        if missing:
            logger.error("Missing required columns in input CSV: %s", missing)
            sys.exit(1)
        return list(reader)


def write_output_csv(filepath: str, rows: list[dict]) -> None:
    fieldnames = INPUT_COLUMNS + ENRICHED_COLUMNS
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _format_date(dt: datetime | None) -> str:
    if dt is None:
        return NOT_FOUND
    return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def enrich_row(row: dict, metadata: PackageMetadata, purl_str: str) -> dict:
    deployed_version = row["DEPLOYED_VERSION"]

    deployed_date = metadata.versions.get(deployed_version)
    latest_ver, latest_date = determine_latest_stable(
        metadata.versions, deployed_version
    )
    earliest = get_earliest_version(metadata.versions)
    earliest_ver = earliest[0] if earliest else NOT_FOUND
    earliest_date = earliest[1] if earliest else None

    enriched = dict(row)
    enriched["PUBLISHED_AT"] = _format_date(deployed_date)
    enriched["LATEST_VERSION"] = latest_ver
    enriched["LATEST_VERSION_PUBLISHED_AT"] = _format_date(latest_date)
    enriched["EARLIEST_REGISTRY_VERSION"] = earliest_ver
    enriched["EARLIEST_VERSION_PUBLISHED_AT"] = _format_date(earliest_date)
    enriched["AGING"] = calculate_aging(latest_date, deployed_date, earliest_date, purl_str)
    enriched["LATEST_AGING"] = calculate_latest_aging(latest_date, purl_str)
    enriched["LATEST_AGING_MONTHS"] = calculate_latest_aging_months(latest_date, purl_str)
    enriched["NEWER_VERSIONS_COUNT"] = count_newer_versions(
        metadata.versions, deployed_version
    )
    return enriched


async def process_rows(
    rows: list[dict], concurrency: int = 10
) -> list[dict]:
    semaphore = asyncio.Semaphore(concurrency)
    cache = PackageCache()
    timeout = aiohttp.ClientTimeout(total=60)

    # Parse PURLs upfront and identify unique packages + supported rows
    parsed_rows: list[tuple[dict, PackageURL | None]] = []
    for row in rows:
        purl_str = row.get("PURL", "")
        try:
            purl = PackageURL.from_string(purl_str)
        except Exception as exc:
            logger.warning("Failed to parse PURL '%s': %s", purl_str, exc)
            purl = None
        parsed_rows.append((row, purl))

    # Collect unique packages to fetch (only supported ecosystems)
    unique_purls: dict[str, PackageURL] = {}
    for row, purl in parsed_rows:
        if purl is None:
            continue
        if purl.type not in ECOSYSTEM_HANDLERS:
            continue
        key = cache.make_key(purl)
        if key not in unique_purls:
            unique_purls[key] = purl

    logger.info(
        "Processing %d rows, %d unique packages to fetch", len(rows), len(unique_purls)
    )

    fetched = 0
    total = len(unique_purls)

    async def fetch_one(purl: PackageURL) -> None:
        nonlocal fetched
        handler = ECOSYSTEM_HANDLERS[purl.type]
        await cache.get_or_fetch(purl, session, semaphore, handler)
        fetched += 1
        if fetched % 10 == 0 or fetched == total:
            logger.info("Progress: fetched %d/%d unique packages", fetched, total)

    async with aiohttp.ClientSession(
        headers={"User-Agent": "purls-fetcher/1.0"},
        timeout=timeout,
    ) as session:
        tasks = [fetch_one(purl) for purl in unique_purls.values()]
        await asyncio.gather(*tasks, return_exceptions=True)

    # Enrich rows
    enriched_rows: list[dict] = []
    skipped_unsupported = 0
    for row, purl in parsed_rows:
        if purl is None:
            # PURL parse failure — still include with ERROR fields
            enriched = dict(row)
            for col in ENRICHED_COLUMNS:
                enriched[col] = ERROR
            enriched_rows.append(enriched)
            continue

        if purl.type not in ECOSYSTEM_HANDLERS:
            skipped_unsupported += 1
            logger.info(
                "Skipping unsupported ecosystem '%s': %s", purl.type, row.get("PURL")
            )
            continue

        key = cache.make_key(purl)
        metadata = cache._store.get(key, PackageMetadata())

        if metadata.error and not metadata.versions:
            enriched = dict(row)
            for col in ENRICHED_COLUMNS:
                enriched[col] = ERROR
            enriched_rows.append(enriched)
        else:
            enriched_rows.append(enrich_row(row, metadata, row.get("PURL", "")))

    if skipped_unsupported:
        logger.info("Skipped %d rows with unsupported ecosystems", skipped_unsupported)

    return enriched_rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich a CSV of software dependencies with version metadata from package registries."
    )
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    parser.add_argument("--output", required=True, help="Path to output CSV file")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Max concurrent API requests (default: 10)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    rows = read_input_csv(args.input)
    logger.info("Read %d rows from %s", len(rows), args.input)

    enriched = asyncio.run(process_rows(rows, concurrency=args.concurrency))

    write_output_csv(args.output, enriched)
    logger.info("Wrote %d enriched rows to %s", len(enriched), args.output)


if __name__ == "__main__":
    main()
