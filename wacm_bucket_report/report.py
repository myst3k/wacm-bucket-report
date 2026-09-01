"""Bucket rollup for a Wasabi WACM control account.

For every sub-account under a control account this produces one CSV row per
active bucket, combining three things:

  * the account hierarchy and per-bucket storage, from the WACM Connect API;
  * a 90-day storage growth figure per sub-account, from the same API's daily
    utilization series;
  * per-bucket features - versioning, object lock, lifecycle, replication, CORS,
    encryption, tagging - which the WACM API does not expose, read directly from
    S3 using each sub-account's existing root access key.

No access keys are ever created: the sub-account's existing key is read via the
documented `includeKeys` parameter and used only to inspect bucket configuration.
"""

import argparse
import base64
import csv
import os
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import boto3
import httpx
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

DEFAULT_BASE = "https://api.wacm.wasabisys.com/api/v1"
GROWTH_DAYS = 90

# Storage numbers from WACM are binary (a "TB" field is really TiB, 2^40 bytes).
STORAGE_DECIMALS = 3

COLUMNS = [
    "control_account_id", "control_account_name",
    "channel_account_id", "channel_account_name",
    "sub_account_id", "wasabi_account_number", "sub_account_name",
    "sub_account_email", "sub_account_status",
    "sub_storage_now_tib", "sub_storage_90d_ago_tib",
    "sub_growth_90d_tib", "sub_growth_90d_pct",
    "bucket", "bucket_number", "region",
    "active_storage_tib", "deleted_storage_tib", "active_objects", "deleted_objects",
    "versioning", "object_lock", "object_lock_mode", "object_lock_days",
    "lifecycle_rules", "replication", "cors_rules", "encryption", "tagging",
    "features_error",
]
FEATURE_COLUMNS = COLUMNS[COLUMNS.index("versioning"):]


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
        # Corporate TLS-inspection proxies can point these at a CA bundle;
        # left unset, the system trust store is used.
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


def process_sub_account(cfg: dict, sub: dict, control: dict,
                        want_features: bool) -> list[dict]:
    rows = []
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
        # One windowed pull gives both the current inventory (latest row per
        # bucket) and the growth (first vs last day of the window).
        daily = paginated(client, cfg["base"], f"/sub-accounts/{sub['id']}/buckets",
                          **{"from": window_start.isoformat(),
                             "to": today.isoformat()})
        active = [r for r in daily if not r.get("bucketDeleteTime")]
        if not active:
            return rows

        current, by_day = {}, defaultdict(float)
        for r in active:
            by_day[r["endTime"]] += r.get("activeStorage") or 0
            num = r.get("bucketNumber")
            if num not in current or r["endTime"] > current[num]["endTime"]:
                current[num] = r

        days = sorted(by_day)
        now_tb, ago_tb = by_day[days[-1]], by_day[days[0]]
        base["sub_storage_now_tib"] = round(now_tb, STORAGE_DECIMALS)
        base["sub_storage_90d_ago_tib"] = round(ago_tb, STORAGE_DECIMALS)
        base["sub_growth_90d_tib"] = round(now_tb - ago_tb, STORAGE_DECIMALS)
        base["sub_growth_90d_pct"] = (round((now_tb - ago_tb) / ago_tb * 100, 1)
                                      if ago_tb else "")

        keys = {}
        if want_features:
            detail = client.get(f"{cfg['base']}/sub-accounts/{sub['id']}",
                                params={"includeKeys": "true"}).json().get("data", {})
            keys = {"ak": detail.get("accessKey"), "sk": detail.get("secretKey")}

    for b in current.values():
        row = dict(base)
        row.update({
            "bucket": b.get("name", ""),
            "bucket_number": b.get("bucketNumber", ""),
            "region": b.get("region", ""),
            "active_storage_tib": round(b.get("activeStorage") or 0, STORAGE_DECIMALS),
            "deleted_storage_tib": round(b.get("deletedStorage") or 0, STORAGE_DECIMALS),
            "active_objects": b.get("activeObjects", 0),
            "deleted_objects": b.get("deletedObjects", 0),
        })
        for col in FEATURE_COLUMNS:
            row.setdefault(col, "")
        if want_features and keys.get("ak") and keys.get("sk") and b.get("region"):
            try:
                row.update(read_features(cfg, keys["ak"], keys["sk"],
                                         b["name"], b["region"]))
            except Exception as exc:
                row["features_error"] = type(exc).__name__
        elif want_features:
            row["features_error"] = "no keys"
        rows.append(row)
    return rows


def resolve_control_account(cfg: dict, chosen: int | None) -> dict:
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
        description="Roll up buckets under a WACM control account to CSV.")
    parser.add_argument("--control-account", type=int,
                        help="Control account id (auto-detected if the key has one)")
    parser.add_argument("--out", default="wacm_bucket_report.csv")
    parser.add_argument("--limit", type=int,
                        help="Only the first N sub-accounts (for a quick test)")
    parser.add_argument("--no-features", action="store_true",
                        help="Storage and growth only - skip S3 feature reads")
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args()

    cfg = config()
    control = resolve_control_account(cfg, args.control_account)
    with wacm_client(cfg) as client:
        subs = paginated(client, cfg["base"], "/sub-accounts",
                         controlAccountId=control["id"])
    print(f"Control account {control['id']} '{control.get('name')}' - "
          f"{len(subs)} sub-accounts")
    if args.limit:
        subs = subs[:args.limit]
        print(f"Limited to {len(subs)}")

    rows, done = [], 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_sub_account, cfg, s, control,
                               not args.no_features): s for s in subs}
        for future in as_completed(futures):
            sub, done = futures[future], done + 1
            try:
                got = future.result()
                rows.extend(got)
                if got:
                    print(f"  [{done}/{len(subs)}] {sub.get('wasabiAccountNumber')}: "
                          f"{len(got)} buckets")
            except Exception as exc:
                print(f"  [{done}/{len(subs)}] sub {sub['id']} FAILED: {exc}",
                      file=sys.stderr)

    with open(args.out, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    accts = {r["sub_account_id"] for r in rows}
    tib = sum(float(r["active_storage_tib"] or 0) for r in rows)
    print(f"\n{len(rows)} buckets across {len(accts)} sub-accounts, "
          f"{tib:,.3f} TiB active")
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
