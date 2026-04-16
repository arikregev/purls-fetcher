# PURL Metadata Fetcher — Implementation Plan

## Context

We need a Python CLI tool that reads a CSV of software dependencies (with Package URLs), queries official package registry APIs for version metadata, and outputs an enriched CSV with five new columns:
1. **`PUBLISHED_AT`** — the publish date of the exact package version currently deployed (from input CSV)
2. **`LATEST_VERSION`** — the latest stable version available in the registry
3. **`LATEST_VERSION_PUBLISHED_AT`** — the publish date of that latest version
4. **`EARLIEST_REGISTRY_VERSION`** — the first-ever version of the package published to the registry
5. **`EARLIEST_VERSION_PUBLISHED_AT`** — the publish date of that earliest version
6. **`AGING`** — days between latest and deployed version publish dates (falls back to latest minus earliest if deployed date unavailable)
7. **`LATEST_AGING`** — days between today and the latest version publish date (`today - LATEST_VERSION_PUBLISHED_AT`)
8. **`LATEST_AGING_MONTHS`** — whole months between the latest version publish date and today
9. **`NEWER_VERSIONS_COUNT`** — number of stable versions published after the deployed version (by SemVer comparison)

**Input CSV columns:** `CSI_ID`, `COMPONENT`, `BUILD_ID`, `PURL`, `DEPLOYED_VERSION`
**Output CSV columns:** all input columns + the 5 enriched columns above

The input CSVs are large (1000+ rows) with repeated PURLs, so async fetching with caching is essential. Seven ecosystems: Maven, PyPI, NPM, Go, NuGet, Cargo, RubyGems.

## Files to Create

| File | Purpose |
|------|---------|
| `main.py` | Single-file CLI tool, all logic in well-sectioned code |
| `requirements.txt` | Dependencies: `packageurl-python`, `aiohttp`, `semver` |
| `sample_input.csv` | Example input for testing |
| `tests/test_version.py` | Unit tests for version parsing and aging logic |
| `tests/test_handlers.py` | Unit tests for each ecosystem handler (mocked HTTP) |

## Step 1: Project skeleton

Create `requirements.txt`:
```
packageurl-python>=0.15.0
aiohttp>=3.9.0
semver>=3.0.0
```

Create `main.py` with:
- CLI via `argparse`: `--input` (required), `--output` (required), `--concurrency` (default 10), `--log-level` (default INFO)
- `read_input_csv()` — reads CSV with `csv.DictReader`, validates expected columns: `CSI_ID`, `COMPONENT`, `BUILD_ID`, `PURL`, `DEPLOYED_VERSION`
- `write_output_csv()` — writes all original columns + `PUBLISHED_AT`, `LATEST_VERSION`, `LATEST_VERSION_PUBLISHED_AT`, `EARLIEST_REGISTRY_VERSION`, `EARLIEST_VERSION_PUBLISHED_AT`, `AGING`, `LATEST_AGING`, `LATEST_AGING_MONTHS`, `NEWER_VERSIONS_COUNT`
- Dataclass: `PackageMetadata(versions: dict[str, datetime])` — cache value holding all version->date mappings

Create `sample_input.csv` with rows spanning Maven, PyPI, NPM, Go, and other ecosystems.

## Step 2: Version utilities

- `parse_version_flexible(version_string) -> semver.Version | None` — strips `v`/`V` prefix, tries strict parse then `optional_minor_and_patch=True`, returns `None` if unparseable
- `determine_latest_stable(versions: dict[str, datetime], current_version: str) -> tuple[str, datetime | None]` — filters to stable (no pre-release unless current is also pre-release), sorts by SemVer, returns highest
- `get_earliest_version(versions: dict[str, datetime]) -> tuple[str, datetime] | None` — returns the version string and datetime with the minimum publish date
- `calculate_aging(latest_date, deployed_date, earliest_date) -> str` — implements the aging formula:
  ```python
  if deployed_date and latest_date:
      return str((latest_date - deployed_date).days)
  elif latest_date and earliest_date:
      return str((latest_date - earliest_date).days)
  else:
      return "NOT_FOUND"
  ```
- `calculate_latest_aging(latest_date) -> str` — days from latest version publish date to today: `str((date.today() - latest_date.date()).days)` or `"NOT_FOUND"`
- `calculate_latest_aging_months(latest_date) -> str` — whole months from latest version publish date to today: `(today.year - d.year) * 12 + (today.month - d.month)`, adjusted down by 1 if day hasn't been reached. Returns `"NOT_FOUND"` if unavailable.
- `count_newer_versions(versions: dict[str, datetime], current_version: str) -> str` — parses all versions with `parse_version_flexible`, counts how many stable versions are strictly greater than the current version by SemVer. Returns the count as a string, or `"NOT_FOUND"` if current version is unparseable.
- **Aging quality check:** All aging columns (`AGING`, `LATEST_AGING`, `LATEST_AGING_MONTHS`) must be clamped to `max(0, value)`. If the computed value is negative (e.g., due to clock skew, pre-release dates, or registry data inconsistencies), output `0` and log a warning with the PURL and raw values for investigation.

## Step 3: HTTP utilities with retry

- `_request_with_retry(session, url, headers, parse_mode)` — exponential backoff (`1s * 2^attempt + jitter`), max 3 retries. Retryable: HTTP 429/500/502/503/504 and connection errors. Honors `Retry-After` header on 429.
- `fetch_json(session, url, headers=None) -> dict` — wrapper returning parsed JSON
- `fetch_text(session, url, headers=None) -> str` — wrapper returning text (for Go proxy)

## Step 4: Cache

```python
class PackageCache:
    _store: dict[str, PackageMetadata]   # key = versionless PURL string
    _locks: dict[str, asyncio.Lock]      # per-key lock prevents duplicate fetches
    _global_lock: asyncio.Lock           # protects _locks dict creation
```

Cache key = `PackageURL(type=..., namespace=..., name=...).to_string()` (strips version).
Double-checked locking: fast-path cache hit, then per-key lock, then fetch inside semaphore.

## Step 5: Ecosystem handlers

Each handler: `async def fetch_{eco}(session, purl) -> PackageMetadata` returning `{version_string: publish_datetime}`.

### Maven
- URL: `https://search.maven.org/solrsearch/select?q=g:{namespace}+AND+a:{name}&core=gav&rows=200&wt=json`
- Paginate if `numFound > 200` (add `&start=N`)
- Each doc: `v` = version, `timestamp` = epoch ms

### PyPI
- URL: `https://pypi.org/pypi/{name}/json`
- `releases` dict: version -> array of dists, take `upload_time_iso_8601` from first dist
- Skip empty release arrays

### NPM
- URL: `https://registry.npmjs.org/{name}` (scoped: `{namespace}%2F{name}`)
- `time` dict: version -> ISO date. Skip `modified`/`created` keys.

### Go
- List: `https://proxy.golang.org/{encoded_module}/@v/list` (newline-separated)
- Info: `https://proxy.golang.org/{encoded_module}/@v/{version}.info` -> `{"Version":..., "Time":...}`
- Module path encoding: uppercase -> `!` + lowercase
- Fetch all version `.info` endpoints concurrently within the handler

### NuGet
- URL: `https://api.nuget.org/v3/registration5-gz-semver2/{lowered_name}/index.json`
- Pages may have inline `items` or require fetching page `@id`
- Each leaf: `catalogEntry.version`, `catalogEntry.published`
- Skip `listed=false` and `published=1900-01-01`

### Cargo
- URL: `https://crates.io/api/v1/crates/{name}`
- Requires `User-Agent` header
- Each version: `num`, `created_at`, `yanked` (skip yanked)

### RubyGems
- URL: `https://rubygems.org/api/v1/versions/{name}.json`
- Each entry: `number`, `created_at`, `prerelease`

Unsupported ecosystem type -> **skip the row entirely** (do not include it in the output CSV). Log an INFO message noting the skipped PURL and its ecosystem type.

## Step 6: Async orchestration with progress

```python
async def process_all_rows(rows, concurrency=10):
    semaphore = asyncio.Semaphore(concurrency)
    cache = PackageCache()
    async with aiohttp.ClientSession(headers={"User-Agent": "purls-fetcher/1.0"}) as session:
        # Deduplicate: one task per unique versionless PURL
        # asyncio.gather with return_exceptions=True
        # Log progress: "Fetched 42/150 unique packages..."
```

Per-row enrichment (synchronous, after all fetches):
- Look up `PackageMetadata` from cache by versionless PURL
- `PUBLISHED_AT` = `versions.get(row_version)` formatted as ISO date, or `"NOT_FOUND"`
- `LATEST_VERSION`, `LATEST_VERSION_PUBLISHED_AT` = `determine_latest_stable(versions, row_version)`
- `EARLIEST_REGISTRY_VERSION`, `EARLIEST_VERSION_PUBLISHED_AT` = `get_earliest_version(versions)`
- `AGING` = `calculate_aging(latest_date, deployed_date, earliest_date)`
- `LATEST_AGING` = `calculate_latest_aging(latest_date)` — days from latest version to today
- `LATEST_AGING_MONTHS` = `calculate_latest_aging_months(latest_date)` — whole months from latest version to today
- `NEWER_VERSIONS_COUNT` = `count_newer_versions(versions, row_version)` — count of stable versions newer than deployed

## Step 7: Error handling summary

| Layer | Behavior |
|-------|----------|
| HTTP retry | Backoff on 429/5xx/connection errors, max 3 retries |
| Handler | Catch 404 -> empty metadata. Catch all other -> log + empty metadata |
| Row enrichment | Missing version -> `"NOT_FOUND"`. Handler error -> `"ERROR"` |
| PURL parse failure | Log warning, all enriched fields = `"ERROR"`, row preserved |
| Unsupported ecosystem | Skip row entirely, log info message |

## Step 8: Tests

### `tests/test_version.py`
- `parse_version_flexible`: standard SemVer, v-prefix, two-segment, unparseable
- `determine_latest_stable`: all stable, mixed stable/pre-release, current is pre-release, empty dict
- `calculate_aging`: both dates present, deployed missing (fallback to earliest), both missing
- `calculate_latest_aging`: date present, date missing
- `calculate_latest_aging_months`: date present (verify month boundary), date missing
- `get_earliest_version`: normal case, single version, empty dict

### `tests/test_handlers.py`
- Mock `aiohttp.ClientSession` for each ecosystem handler
- Test normal responses, 404s, malformed JSON
- Test Go module path encoding
- Test NuGet paged vs inline items
- Test Cargo yanked filtering

## Verification

1. `pip install -r requirements.txt`
2. `python main.py --input sample_input.csv --output output.csv --log-level DEBUG`
3. Inspect `output.csv`:
   - Original columns (`CSI_ID`, `COMPONENT`, `BUILD_ID`, `PURL`, `DEPLOYED_VERSION`) preserved exactly
   - Enriched columns populated with dates/versions or `NOT_FOUND`/`ERROR`
   - `AGING` column has integer day counts (or `NOT_FOUND`)
4. Check logs for cache hit messages (repeated PURLs should hit cache)
5. `python -m pytest tests/ -v`
