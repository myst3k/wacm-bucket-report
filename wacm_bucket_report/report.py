"""Bucket rollup for a Wasabi WACM control account, at three levels.

For a control account this writes three CSVs:

  * <out>_buckets.csv    - one row per active bucket (the raw data), with
                           storage, 90-day growth, and features.
  * <out>_subaccounts.csv- one row per sub-account: storage, growth, and counts
                           of each major bucket feature.
  * <out>_channels.csv   - one row per channel account: the same, rolled up
                           across its sub-accounts.

Account hierarchy and storage come from the WACM Connect API. The 90-day growth
comes from that API's daily utilization series. Per-bucket features - versioning,
object lock, lifecycle, replication, CORS, encryption, tagging - are not exposed
by the WACM API, so for each sub-account the tool reads its *existing* root
access key (documented `includeKeys` parameter) and queries S3 directly. It
never creates or modifies keys, and never writes to storage.
"""

import argparse
import base64
import csv
import os
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

import boto3
import httpx
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

DEFAULT_BASE = "https://api.wacm.wasabisys.com/api/v1"
GROWTH_DAYS = 90
STORAGE_DECIMALS = 3  # WACM "TB" fields are actually binary TiB (2^40 bytes).

BUCKET_COLUMNS = [
    "control_account_id", "control_account_name",
    "channel_account_id", "channel_account_name",
    "sub_account_id", "wasabi_account_number", "sub_account_name",
    "sub_account_email", "sub_account_status",
    "bucket", "bucket_number", "region",
    "active_storage_tib", "deleted_storage_tib", "active_objects", "deleted_objects",
    "storage_90d_ago_tib", "growth_90d_tib", "growth_90d_pct",
    "versioning", "object_lock", "object_lock_mode", "object_lock_days",
    "lifecycle_rules", "replication", "cors_rules", "encryption", "tagging",
    "features_error",
]
FEATURE_FIELDS = BUCKET_COLUMNS[BUCKET_COLUMNS.index("versioning"):]

# Feature-count columns shared by the sub-account and channel rollups.
COUNT_COLUMNS = [
    "bucket_count", "versioned_buckets",
    "object_lock_buckets", "object_lock_compliance", "object_lock_governance",
    "object_lock_no_default", "replication_buckets", "lifecycle_buckets",
    "cors_buckets", "encryption_buckets", "tagging_buckets", "features_unavailable",
]
STORAGE_COLUMNS = ["storage_now_tib", "storage_90d_ago_tib",
                   "growth_90d_tib", "growth_90d_pct"]


# --------------------------------------------------------------------------- API

def config() -> dict:
    username = os.getenv("WACM_CONNECT_USERNAME")
    api_key = os.getenv("WACM_CONNECT_API_KEY")
    if not username or not api_key:
        sys.exit("Set WACM_CONNECT_USERNAME and WACM_CONNECT_API_KEY "
                 "(see .env.example).")
    return {
        "base": os.getenv("WACM_CONNECT_BASE_URL", DEFAULT_BASE).rstrip("/"),
        "username": username,
        "api_key": api_key,
        "ca_bundle": os.getenv("WACM_CA_BUNDLE") or os.getenv("REQUESTS_CA_BUNDLE"),
        "s3_endpoint_tpl": os.getenv("WASABI_S3_ENDPOINT",
                                     "https://s3.{region}.wasabisys.com"),
    }


def wacm_client(cfg: dict) -> httpx.Client:
    cred = base64.b64encode(f"{cfg['username']}:{cfg['api_key']}".encode()).decode()
    return httpx.Client(headers={"Authorization": f"Basic {cred}"},
                        timeout=90, verify=cfg["ca_bundle"] or True)


def paginated(client: httpx.Client, base: str, endpoint: str, **params) -> list[dict]:
    items, page = [], 0
    while True:
        r = client.get(f"{base}{endpoint}",
                       params={"size": 2000, "page": page, **params})
        r.raise_for_status()
        data = r.json()
        if not data.get("success"):
            raise RuntimeError(f"{endpoint}: {data.get('message')}")
        batch = data.get("data", {}).get("items", [])
        items.extend(batch)
        if len(items) >= data.get("data", {}).get("total", 0) or not batch:
            return items
        page += 1


def read_features(cfg: dict, access_key: str, secret_key: str,
                  bucket: str, region: str) -> dict:
    """Bucket configuration from S3. Each read is isolated so one missing feature
    (the common case) never hides the others."""
    s3 = boto3.client(
        "s3", endpoint_url=cfg["s3_endpoint_tpl"].format(region=region),
        aws_access_key_id=access_key, aws_secret_access_key=secret_key,
        region_name=region, verify=cfg["ca_bundle"] or True,
        config=Config(connect_timeout=15, read_timeout=40,
                      retries={"max_attempts": 2}, signature_version="s3v4"))
    out = {"versioning": "", "object_lock": "no", "object_lock_mode": "",
           "object_lock_days": "", "lifecycle_rules": 0, "replication": "no",
           "cors_rules": 0, "encryption": "", "tagging": "no", "features_error": ""}

    def attempt(fn):
        try:
            return fn()
        except Exception:
            return None

    out["versioning"] = attempt(
        lambda: s3.get_bucket_versioning(Bucket=bucket).get("Status")) or "Disabled"

    ol = attempt(lambda: s3.get_object_lock_configuration(
        Bucket=bucket)["ObjectLockConfiguration"])
    if ol:
        out["object_lock"] = "yes"
        rule = ol.get("Rule", {}).get("DefaultRetention", {})
        out["object_lock_mode"] = rule.get("Mode", "")
        out["object_lock_days"] = rule.get("Days") or rule.get("Years") or ""

    lc = attempt(lambda: s3.get_bucket_lifecycle_configuration(
        Bucket=bucket).get("Rules", []))
    out["lifecycle_rules"] = len(lc) if lc else 0

    rep = attempt(lambda: s3.get_bucket_replication(
        Bucket=bucket)["ReplicationConfiguration"].get("Rules", []))
    out["replication"] = "yes" if rep else "no"

    cors = attempt(lambda: s3.get_bucket_cors(Bucket=bucket).get("CORSRules", []))
    out["cors_rules"] = len(cors) if cors else 0

    out["encryption"] = attempt(
        lambda: s3.get_bucket_encryption(Bucket=bucket)
        ["ServerSideEncryptionConfiguration"]["Rules"][0]
        ["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"]) or "none"

    tags = attempt(lambda: s3.get_bucket_tagging(Bucket=bucket).get("TagSet", []))
    out["tagging"] = "yes" if tags else "no"
    return out


# ------------------------------------------------------------------ bucket level

def process_sub_account(cfg: dict, sub: dict, control: dict,
                        want_features: bool) -> list[dict]:
    """Return one row per active bucket in this sub-account, with per-bucket
    90-day growth and (optionally) S3 features."""
    base = {
        "control_account_id": control["id"],
        "control_account_name": control.get("name", ""),
        "channel_account_id": sub.get("channelAccountId", ""),
        "channel_account_name": sub.get("channelAccountName", ""),
        "sub_account_id": sub["id"],
        "wasabi_account_number": sub.get("wasabiAccountNumber", ""),
        "sub_account_name": sub.get("wasabiAccountName") or sub.get("name", ""),
        "sub_account_email": sub.get("contactEmail", ""),
        "sub_account_status": sub.get("status", ""),
    }
    today = date.today()
    window_start = today - timedelta(days=GROWTH_DAYS)
    with wacm_client(cfg) as client:
        daily = paginated(client, cfg["base"], f"/sub-accounts/{sub['id']}/buckets",
                          **{"from": window_start.isoformat(),
                             "to": today.isoformat()})
        active = [r for r in daily if not r.get("bucketDeleteTime")]
        if not active:
            return []

        # Per bucket: its own daily series, earliest vs latest day = growth.
        series = defaultdict(list)
        for r in active:
            series[r.get("bucketNumber")].append(r)

        keys = {}
        if want_features:
            detail = client.get(f"{cfg['base']}/sub-accounts/{sub['id']}",
                                params={"includeKeys": "true"}).json().get("data", {})
            keys = {"ak": detail.get("accessKey"), "sk": detail.get("secretKey")}

    rows = []
    for num, points in series.items():
        points.sort(key=lambda r: r["endTime"])
        latest, earliest = points[-1], points[0]
        now_tb = latest.get("activeStorage") or 0
        ago_tb = earliest.get("activeStorage") or 0
        row = dict(base)
        row.update({
            "bucket": latest.get("name", ""),
            "bucket_number": num,
            "region": latest.get("region", ""),
            "active_storage_tib": round(now_tb, STORAGE_DECIMALS),
            "deleted_storage_tib": round(latest.get("deletedStorage") or 0,
                                         STORAGE_DECIMALS),
            "active_objects": latest.get("activeObjects", 0),
            "deleted_objects": latest.get("deletedObjects", 0),
            "storage_90d_ago_tib": round(ago_tb, STORAGE_DECIMALS),
            "growth_90d_tib": round(now_tb - ago_tb, STORAGE_DECIMALS),
            "growth_90d_pct": (round((now_tb - ago_tb) / ago_tb * 100, 1)
                               if ago_tb else ""),
        })
        for col in FEATURE_FIELDS:
            row.setdefault(col, "")
        if want_features and keys.get("ak") and keys.get("sk") and latest.get("region"):
            try:
                row.update(read_features(cfg, keys["ak"], keys["sk"],
                                         latest["name"], latest["region"]))
            except Exception as exc:
                row["features_error"] = type(exc).__name__
        elif want_features:
            row["features_error"] = "no keys"
        rows.append(row)
    return rows


# --------------------------------------------------------------- aggregate level

def feature_counts(bucket_rows: list[dict]) -> dict:
    def n(pred):
        return sum(1 for r in bucket_rows if pred(r))

    return {
        "bucket_count": len(bucket_rows),
        "versioned_buckets": n(lambda r: r["versioning"] == "Enabled"),
        "object_lock_buckets": n(lambda r: r["object_lock"] == "yes"),
        "object_lock_compliance": n(lambda r: r["object_lock_mode"] == "COMPLIANCE"),
        "object_lock_governance": n(lambda r: r["object_lock_mode"] == "GOVERNANCE"),
        "object_lock_no_default": n(lambda r: r["object_lock"] == "yes"
                                    and not r["object_lock_mode"]),
        "replication_buckets": n(lambda r: r["replication"] == "yes"),
        "lifecycle_buckets": n(lambda r: (r["lifecycle_rules"] or 0)
                               not in ("", 0, "0")),
        "cors_buckets": n(lambda r: (r["cors_rules"] or 0) not in ("", 0, "0")),
        "encryption_buckets": n(lambda r: r["encryption"] not in ("", "none")),
        "tagging_buckets": n(lambda r: r["tagging"] == "yes"),
        "features_unavailable": n(lambda r: bool(r["features_error"])),
    }


def storage_totals(bucket_rows: list[dict]) -> dict:
    now = sum(float(r["active_storage_tib"] or 0) for r in bucket_rows)
    ago = sum(float(r["storage_90d_ago_tib"] or 0) for r in bucket_rows)
    return {
        "storage_now_tib": round(now, STORAGE_DECIMALS),
        "storage_90d_ago_tib": round(ago, STORAGE_DECIMALS),
        "growth_90d_tib": round(now - ago, STORAGE_DECIMALS),
        "growth_90d_pct": round((now - ago) / ago * 100, 1) if ago else "",
    }


def rollup_sub_accounts(bucket_rows: list[dict]) -> list[dict]:
    groups = defaultdict(list)
    for r in bucket_rows:
        groups[r["sub_account_id"]].append(r)
    rows = []
    for sub_id, rs in groups.items():
        head = rs[0]
        row = {k: head[k] for k in (
            "control_account_id", "control_account_name",
            "channel_account_id", "channel_account_name",
            "sub_account_id", "wasabi_account_number", "sub_account_name",
            "sub_account_email", "sub_account_status")}
        row.update(storage_totals(rs))
        row.update(feature_counts(rs))
        rows.append(row)
    return rows


def rollup_channels(bucket_rows: list[dict]) -> list[dict]:
    groups = defaultdict(list)
    for r in bucket_rows:
        groups[r["channel_account_id"] or "(direct)"].append(r)
    rows = []
    for chan_id, rs in groups.items():
        head = rs[0]
        row = {
            "control_account_id": head["control_account_id"],
            "control_account_name": head["control_account_name"],
            "channel_account_id": head["channel_account_id"] or "(direct)",
            "channel_account_name": head["channel_account_name"] or "(no channel account)",
            "sub_account_count": len({r["sub_account_id"] for r in rs}),
        }
        row.update(storage_totals(rs))
        row.update(feature_counts(rs))
        rows.append(row)
    return rows


def write_csv(path: str, columns: list[str], rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ------------------------------------------------------------------------- entry

def resolve_control_account(cfg: dict, chosen) -> dict:
    with wacm_client(cfg) as client:
        accounts = paginated(client, cfg["base"], "/control-accounts")
    if chosen:
        for a in accounts:
            if a["id"] == chosen:
                return a
        sys.exit(f"Control account {chosen} not found for this key.")
    if len(accounts) == 1:
        return accounts[0]
    print("This key has access to multiple control accounts - pick one with "
          "--control-account:", file=sys.stderr)
    for a in accounts:
        print(f"  {a['id']:>8}  {a.get('name','')}", file=sys.stderr)
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Roll up buckets under a WACM control account to three CSVs.")
    parser.add_argument("--control-account", type=int,
                        help="Control account id (auto-detected if the key has one)")
    parser.add_argument("--out",
                        help="Output prefix; writes <out>_buckets/_subaccounts/"
                             "_channels.csv. Default is built from the control "
                             "account and current date-time.")
    parser.add_argument("--limit", type=int,
                        help="Only the first N sub-accounts (for a quick test)")
    parser.add_argument("--no-features", action="store_true",
                        help="Storage and growth only - skip S3 feature reads")
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args()

    cfg = config()
    control = resolve_control_account(cfg, args.control_account)

    if args.out:
        prefix = args.out
    else:
        slug = re.sub(r"[^a-z0-9]+", "-",
                      (control.get("name") or "").lower()).strip("-") or "control"
        prefix = f"wacm_{control['id']}_{slug}_{datetime.now():%Y%m%d-%H%M}"

    with wacm_client(cfg) as client:
        subs = paginated(client, cfg["base"], "/sub-accounts",
                         controlAccountId=control["id"])
    print(f"Control account {control['id']} '{control.get('name')}' - "
          f"{len(subs)} sub-accounts")
    if args.limit:
        subs = subs[:args.limit]
        print(f"Limited to {len(subs)}")

    bucket_rows, done = [], 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_sub_account, cfg, s, control,
                               not args.no_features): s for s in subs}
        for future in as_completed(futures):
            sub, done = futures[future], done + 1
            try:
                got = future.result()
                bucket_rows.extend(got)
                if got:
                    print(f"  [{done}/{len(subs)}] {sub.get('wasabiAccountNumber')}: "
                          f"{len(got)} buckets")
            except Exception as exc:
                print(f"  [{done}/{len(subs)}] sub {sub['id']} FAILED: {exc}",
                      file=sys.stderr)

    sub_rows = rollup_sub_accounts(bucket_rows)
    chan_rows = rollup_channels(bucket_rows)

    write_csv(f"{prefix}_buckets.csv", BUCKET_COLUMNS, bucket_rows)
    write_csv(f"{prefix}_subaccounts.csv",
              BUCKET_COLUMNS[:9] + STORAGE_COLUMNS + COUNT_COLUMNS, sub_rows)
    write_csv(f"{prefix}_channels.csv",
              ["control_account_id", "control_account_name",
               "channel_account_id", "channel_account_name", "sub_account_count"]
              + STORAGE_COLUMNS + COUNT_COLUMNS, chan_rows)

    tib = sum(float(r["active_storage_tib"] or 0) for r in bucket_rows)
    print(f"\n{len(bucket_rows)} buckets / {len(sub_rows)} sub-accounts / "
          f"{len(chan_rows)} channel groups, {tib:,.3f} TiB active")
    for suffix in ("buckets", "subaccounts", "channels"):
        print(f"  wrote {prefix}_{suffix}.csv")


if __name__ == "__main__":
    main()
