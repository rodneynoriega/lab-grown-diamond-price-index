#!/usr/bin/env python3
"""Kill switch for Shopify pilot pages (rings.com/pages/).

One command to remove pilot pages from the store via the Shopify GraphQL
Admin API, driven by a manifest of page IDs. Two modes:

  delete    - pageDelete: the page is permanently removed (hard kill).
  unpublish - pageUpdate isPublished:false: the page 404s for visitors and
              crawlers (the noindex-equivalent available at the API level;
              a literal <meta name="robots" content="noindex"> would need
              theme-template support, which the pilot template can add later).

Usage (dry-run by default; nothing is modified without --yes):

  python3 kill_switch.py --manifest pilot-manifest.json --mode delete --yes
  python3 kill_switch.py --id 123456789 --mode unpublish --yes

Manifest format (JSON): either a list of entries or {"pages": [entries]}.
Each entry: {"id": "gid://shopify/Page/123" or 123, "handle": "optional-check"}.

Safety rails (all hard, none skippable):
  * PROTECTED_HANDLES are never touched, whatever the manifest says. The
    live index pages are GemPages-built and must NEVER be modified via the
    API (conflicts with GemPages).
  * Every page is fetched first; if the manifest supplies a handle and it
    does not match the live handle for that ID, the entry is refused.
  * Credentials come from .env next to this script (SHOPIFY_CLIENT_ID /
    SHOPIFY_CLIENT_SECRET) and are never printed.
  * Exit code 0 only if every requested page is verified gone/unpublished
    after the run (already-absent pages count as success: idempotent).

This script only removes/hides pages. It never creates or publishes anything.
"""

import argparse
import json
import sys
import time
from pathlib import Path

import requests

STORE = "09c241-13.myshopify.com"
API_VERSION = "2026-01"
TOKEN_URL = f"https://{STORE}/admin/oauth/access_token"
GRAPHQL_URL = f"https://{STORE}/admin/api/{API_VERSION}/graphql.json"

# Pages that must never be touched by this script, whatever any manifest
# says. The two live index pages are GemPages-built: API writes conflict
# with GemPages and could corrupt them.
PROTECTED_HANDLES = {
    "lab-grown-diamond-price-index",
    "lab-grown-diamond-price-index-uk",
    "uk-lab-grown-diamond-price-index",
    "about-us",
    "contact-us",
}

MUTATION_DELAY_S = 0.6  # be polite to the rate limiter between mutations
MAX_RETRIES = 3


def load_env(path):
    env = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def mint_token(env):
    r = requests.post(
        TOKEN_URL,
        json={
            "client_id": env["SHOPIFY_CLIENT_ID"],
            "client_secret": env["SHOPIFY_CLIENT_SECRET"],
            "grant_type": "client_credentials",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def gql(token, query, variables=None):
    """POST a GraphQL request with throttle-aware retries."""
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.post(
            GRAPHQL_URL,
            json={"query": query, "variables": variables or {}},
            headers={"X-Shopify-Access-Token": token},
            timeout=30,
        )
        if resp.status_code == 429:
            wait = 2.0 * attempt
            print(f"  throttled (HTTP 429), retrying in {wait:.0f}s...")
            time.sleep(wait)
            last_err = "HTTP 429"
            continue
        resp.raise_for_status()
        body = resp.json()
        errors = body.get("errors") or []
        if any(
            (e.get("extensions") or {}).get("code") == "THROTTLED" for e in errors
        ):
            wait = 2.0 * attempt
            print(f"  throttled (GraphQL), retrying in {wait:.0f}s...")
            time.sleep(wait)
            last_err = "THROTTLED"
            continue
        if errors:
            raise RuntimeError(f"GraphQL errors: {errors}")
        return body["data"]
    raise RuntimeError(f"gave up after {MAX_RETRIES} throttled attempts ({last_err})")


def to_gid(raw):
    s = str(raw)
    return s if s.startswith("gid://") else f"gid://shopify/Page/{s}"


def fetch_page(token, gid):
    data = gql(
        token,
        """
        query($id: ID!) {
          node(id: $id) {
            ... on Page { id handle title isPublished }
          }
        }
        """,
        {"id": gid},
    )
    return data["node"]  # None if the page does not exist


def delete_page(token, gid):
    data = gql(
        token,
        """
        mutation($id: ID!) {
          pageDelete(id: $id) {
            deletedPageId
            userErrors { field message }
          }
        }
        """,
        {"id": gid},
    )
    errs = data["pageDelete"]["userErrors"]
    if errs:
        raise RuntimeError(f"pageDelete userErrors: {errs}")
    return data["pageDelete"]["deletedPageId"]


def unpublish_page(token, gid):
    data = gql(
        token,
        """
        mutation($id: ID!, $page: PageUpdateInput!) {
          pageUpdate(id: $id, page: $page) {
            page { id isPublished }
            userErrors { field message }
          }
        }
        """,
        {"id": gid, "page": {"isPublished": False}},
    )
    errs = data["pageUpdate"]["userErrors"]
    if errs:
        raise RuntimeError(f"pageUpdate userErrors: {errs}")
    return data["pageUpdate"]["page"]


def load_manifest(path):
    raw = json.loads(Path(path).read_text())
    entries = raw["pages"] if isinstance(raw, dict) else raw
    if not isinstance(entries, list) or not entries:
        raise SystemExit(f"manifest {path} contains no page entries")
    return entries


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--manifest", help="JSON manifest of pages to kill")
    src.add_argument("--id", help="single page ID (numeric or gid://) to kill")
    ap.add_argument(
        "--mode",
        choices=["delete", "unpublish"],
        default="delete",
        help="delete = pageDelete (default); unpublish = isPublished:false",
    )
    ap.add_argument(
        "--yes",
        action="store_true",
        help="actually perform the action (default is a dry-run report)",
    )
    args = ap.parse_args()

    here = Path(__file__).resolve().parent
    env_path = here / ".env"
    if not env_path.exists():
        raise SystemExit(f"missing {env_path}; cannot authenticate")
    env = load_env(env_path)
    for k in ("SHOPIFY_CLIENT_ID", "SHOPIFY_CLIENT_SECRET"):
        if not env.get(k):
            raise SystemExit(f"{k} not set in {env_path}")

    if args.manifest:
        entries = load_manifest(args.manifest)
    else:
        entries = [{"id": args.id}]

    token = mint_token(env)
    print(f"kill_switch: mode={args.mode} targets={len(entries)} "
          f"{'LIVE RUN' if args.yes else 'DRY RUN (no changes; add --yes)'}")

    ok, failed, refused = 0, 0, 0
    for entry in entries:
        gid = to_gid(entry["id"])
        expected_handle = entry.get("handle")
        page = fetch_page(token, gid)

        if page is None:
            print(f"  {gid}: already absent -- OK (idempotent)")
            ok += 1
            continue

        handle, title = page["handle"], page["title"]
        if handle in PROTECTED_HANDLES:
            print(f"  {gid} (/pages/{handle}): PROTECTED handle -- REFUSED. "
                  f"This page must never be touched via the API.")
            refused += 1
            continue
        if expected_handle and handle != expected_handle:
            print(f"  {gid}: live handle '{handle}' != manifest handle "
                  f"'{expected_handle}' -- REFUSED (possible ID mixup)")
            refused += 1
            continue

        if not args.yes:
            print(f"  {gid} (/pages/{handle}, '{title}', "
                  f"published={page['isPublished']}): would {args.mode}")
            continue

        try:
            if args.mode == "delete":
                delete_page(token, gid)
                after = fetch_page(token, gid)
                if after is None:
                    print(f"  {gid} (/pages/{handle}): deleted, verified gone")
                    ok += 1
                else:
                    print(f"  {gid}: pageDelete returned but page still "
                          f"exists -- FAILED verification")
                    failed += 1
            else:
                unpublish_page(token, gid)
                after = fetch_page(token, gid)
                if after and after["isPublished"] is False:
                    print(f"  {gid} (/pages/{handle}): unpublished, "
                          f"verified isPublished=false")
                    ok += 1
                else:
                    print(f"  {gid}: unpublish not verified -- FAILED")
                    failed += 1
        except Exception as e:  # keep going; report at the end
            print(f"  {gid}: ERROR {e}")
            failed += 1
        time.sleep(MUTATION_DELAY_S)

    print(f"kill_switch summary: ok={ok} failed={failed} refused={refused} "
          f"of {len(entries)}")
    if not args.yes:
        sys.exit(0)
    sys.exit(0 if failed == 0 and refused == 0 and ok == len(entries) else 1)


if __name__ == "__main__":
    main()
