# WACM Bucket Report

Produces a CSV listing every active bucket under a Wasabi WACM control account,
with per-bucket storage, a 90-day storage growth figure per sub-account, and
per-bucket features (versioning, object lock, lifecycle, replication, CORS,
encryption, tagging).

Account hierarchy and storage come from the WACM Connect API. Bucket features
are not exposed by that API, so for each sub-account the tool reads its
**existing** root access key (via the documented `includeKeys` parameter) and
queries S3 directly. It never creates or modifies keys, and never writes to your
storage — every call is read-only.

## Requirements

- [uv](https://docs.astral.sh/uv/) (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- A WACM Connect API username and key, from the WACM console

## Setup

```bash
cp .env.example .env
# edit .env and fill in WACM_CONNECT_USERNAME and WACM_CONNECT_API_KEY
```

## Run

```bash
# Full report (auto-detects the control account if your key has exactly one)
uv run wacm-bucket-report

# Quick test against the first 20 sub-accounts
uv run wacm-bucket-report --limit 20

# Storage and growth only, skipping the per-bucket feature reads (much faster)
uv run wacm-bucket-report --no-features

# A specific control account, custom output path
uv run wacm-bucket-report --control-account 12345 --out my_report.csv
```

Options: `--control-account`, `--out`, `--limit`, `--no-features`, `--workers`.

## Output columns

| Column | Source | Notes |
|---|---|---|
| control / channel / sub account id, name, email, status | WACM | account hierarchy |
| `wasabi_account_number` | WACM | the sub-account's Wasabi account |
| `sub_storage_now_tib`, `sub_storage_90d_ago_tib` | WACM | sub-account totals |
| `sub_growth_90d_tib`, `sub_growth_90d_pct` | WACM | 90-day growth; % is blank when the base was zero |
| `bucket`, `bucket_number`, `region` | WACM | |
| `active_storage_tib`, `deleted_storage_tib` | WACM | |
| `active_objects`, `deleted_objects` | WACM | |
| `versioning`, `object_lock` (+ `mode`, `days`) | S3 | |
| `lifecycle_rules`, `replication`, `cors_rules` | S3 | |
| `encryption`, `tagging` | S3 | |
| `features_error` | — | e.g. `no keys` when the sub-account has no root key to read |

Storage is in TiB (binary, 2⁴⁰ bytes), rounded to three decimals. Sub-accounts
without a root access key still report storage and growth; their feature columns
are left blank with `features_error = no keys`.

## Notes

- Growth uses the daily utilization series over the last 90 days. For a
  sub-account younger than 90 days, the baseline is its earliest available day.
- Runtime scales with the number of buckets. `--no-features` is a fast first
  pass; the full feature read can take a while on large accounts.
