# WACM Bucket Report

Reports every active bucket under a Wasabi WACM control account at three levels,
writing three CSVs per run:

- **`..._buckets.csv`** — one row per bucket: storage, 90-day growth, and
  features (versioning, object lock + mode, lifecycle, replication, CORS,
  encryption, tagging).
- **`..._subaccounts.csv`** — one row per sub-account: storage, growth, and a
  count of each feature across its buckets.
- **`..._channels.csv`** — one row per channel account: the same, rolled up
  across its sub-accounts.

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

# A specific control account and output prefix
uv run wacm-bucket-report --control-account 12345 --out acme
```

Options: `--control-account`, `--out`, `--limit`, `--no-features`, `--workers`.

Output files are named from the control account and run time, for example
`wacm_12345_acme-storage_20260901-1524_buckets.csv`. Pass `--out PREFIX` to name
them yourself (`PREFIX_buckets.csv`, …).

## Output columns

**`_buckets.csv`** — one row per bucket:

| Column | Source |
|---|---|
| control / channel / sub account id, name, email, status | WACM |
| `wasabi_account_number` | WACM |
| `bucket`, `bucket_number`, `region` | WACM |
| `active_storage_tib`, `deleted_storage_tib`, `active_objects`, `deleted_objects` | WACM |
| `storage_90d_ago_tib`, `growth_90d_tib`, `growth_90d_pct` | WACM (daily series) |
| `versioning`, `object_lock`, `object_lock_mode`, `object_lock_days` | S3 |
| `lifecycle_rules`, `replication`, `cors_rules`, `encryption`, `tagging` | S3 |
| `features_error` | e.g. `no keys` when the sub-account has no root key to read |

**`_subaccounts.csv`** and **`_channels.csv`** — one row per sub-account /
channel account, with `storage_now_tib`, `storage_90d_ago_tib`, `growth_90d_tib`,
`growth_90d_pct`, and a count of each feature:

`bucket_count`, `versioned_buckets`, `object_lock_buckets`,
`object_lock_compliance`, `object_lock_governance`, `object_lock_no_default`,
`replication_buckets`, `lifecycle_buckets`, `cors_buckets`, `encryption_buckets`,
`tagging_buckets`, `features_unavailable`. The channel file also has
`sub_account_count`.

Storage is in TiB (binary, 2⁴⁰ bytes), rounded to three decimals. The growth
percentage is blank when the 90-day-ago baseline was zero. Sub-accounts without a
root access key still report storage and growth; their feature columns are blank
and they are tallied under `features_unavailable`.

## Notes

- Growth uses the daily utilization series over the last 90 days. For a
  sub-account younger than 90 days, the baseline is its earliest available day.
- Runtime scales with the number of buckets. `--no-features` is a fast first
  pass; the full feature read can take a while on large accounts.
