# PURL Metadata Fetcher

A Python CLI tool that enriches a CSV of software dependencies with version metadata from official package registries. Given a list of Package URLs (PURLs), it queries each ecosystem's public API to determine how current your dependencies are.

## What It Does

For each dependency in your input CSV, the tool:

- Looks up the **publish date** of the exact version you're running
- Finds the **latest stable version** available (using strict SemVer rules)
- Finds the **earliest version** ever published to the registry
- Calculates **aging metrics** — how many days behind you are, how stale the latest version is, and how many newer versions exist

## Supported Ecosystems

| Ecosystem | Registry API | PURL Example |
|-----------|-------------|--------------|
| Maven | search.maven.org | `pkg:maven/org.apache.commons/commons-lang3@3.12.0` |
| PyPI | pypi.org | `pkg:pypi/requests@2.28.0` |
| NPM | registry.npmjs.org | `pkg:npm/express@4.18.2` |
| Go | proxy.golang.org | `pkg:golang/github.com/sirupsen/logrus@v1.9.0` |
| NuGet | api.nuget.org | `pkg:nuget/Newtonsoft.Json@13.0.1` |
| Cargo | crates.io | `pkg:cargo/serde@1.0.160` |
| RubyGems | rubygems.org | `pkg:gem/rails@7.0.4` |

Rows with unsupported ecosystems are silently skipped (logged at INFO level).

## Installation

```bash
pip install -r requirements.txt
```

**Dependencies:** `packageurl-python`, `aiohttp`, `semver`

## Usage

```bash
python3 main.py --input dependencies.csv --output enriched.csv
```

With a corporate proxy:

```bash
python3 main.py --input dependencies.csv --output enriched.csv --proxy http://proxy.corp:8080
```

### CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--input` | *(required)* | Path to input CSV |
| `--output` | *(required)* | Path to output CSV |
| `--proxy` | *(none)* | HTTP/HTTPS proxy URL (e.g., `http://proxy.corp:8080`) |
| `--concurrency` | `10` | Max concurrent API requests |
| `--log-level` | `INFO` | `DEBUG`, `INFO`, `WARNING`, or `ERROR` |

## Input Format

CSV with these columns:

```
CSI_ID,PROJECT_NAME,BUILD_ID,PURL
CSI-001,WebApp,build-100,pkg:pypi/requests@2.28.0
CSI-002,BackendSvc,build-201,pkg:maven/org.apache.commons/commons-lang3@3.12.0
```

The version is extracted automatically from the PURL — no separate version column needed.

## Output Format

The output CSV preserves all input columns and appends:

| Column | Description |
|--------|-------------|
| `DEPLOYED_VERSION` | Version extracted from the PURL |
| `PUBLISHED_AT` | Publish date of the deployed version |
| `LATEST_VERSION` | Latest stable version in the registry |
| `LATEST_VERSION_PUBLISHED_AT` | Publish date of the latest version |
| `EARLIEST_REGISTRY_VERSION` | First version ever published |
| `EARLIEST_VERSION_PUBLISHED_AT` | Publish date of the earliest version |
| `AGING` | Days between latest and deployed version publish dates. Falls back to latest minus earliest if the deployed date is unavailable |
| `LATEST_AGING` | Days between today and the latest version publish date |
| `LATEST_AGING_MONTHS` | Whole months between today and the latest version publish date |
| `NEWER_VERSIONS_COUNT` | Number of stable versions newer than the deployed version |

When data cannot be resolved, cells are populated with `NOT_FOUND` (package/version missing from registry) or `ERROR` (API failure).

## Performance

- **Async I/O** — all API calls run concurrently via `asyncio`/`aiohttp` with a configurable concurrency limit
- **In-memory caching** — duplicate PURLs across rows trigger only one API call per unique package
- **Retry with backoff** — transient failures (429, 5xx, connection errors) are retried up to 3 times with exponential backoff and jitter

## Testing

```bash
pip install pytest
python3 -m pytest tests/ -v
```

48 unit tests cover version parsing, aging calculations, SemVer logic, and all ecosystem handlers (with mocked HTTP).

## Troubleshooting

### SSL Certificate Errors on macOS

If you see `CERTIFICATE_VERIFY_FAILED` errors, your Python installation is missing root certificates. Fix with one of:

```bash
# Option 1: Set SSL_CERT_FILE for this run
SSL_CERT_FILE=$(python3 -c "import certifi; print(certifi.where())") python3 main.py --input input.csv --output output.csv

# Option 2: Install certificates system-wide (run once)
/Applications/Python\ 3.11/Install\ Certificates.command
```
