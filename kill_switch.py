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
  python3 kill_switch.py --id 123456789 --handle some-pilot-handle --yes

Manifest format (JSON): either a list of entries or {"pages": [entries]}.
Each entry: {"id": <numeric or gid://shopify/Page/N>, "handle": "<handle>"}.
"handle" is REQUIRED for every entry (it is the aim check); optional
human-note keys "title" and "note" are allowed, anything else is rejected.
The ENTIRE manifest is validated before the first API call: bad id
namespace, missing/empty handle, duplicate ids, or unknown keys abort with
exit 2 and zero mutations. No partial batches from a malformed manifest.

Safety rails (all hard, none skippable):
  * PROTECTED_IDS (checked before any fetch) and PROTECTED_HANDLES are
    never touched, whatever the manifest says. The live index pages are
    GemPages-built and must NEVER be modified via the API (conflicts with
    GemPages). IDs are the primary rail (rename-proof); handles are a
    second layer.
  * Every page is fetched first; if its live handle does not match the
    manifest handle for that ID (after normalization: trim, lowercase,
    strip any /pages/ prefix), the entry is refused.
  * --id requires --handle: single-page kills get the same aim check.
  * Credentials come from .env next to this script (SHOPIFY_CLIENT_ID /
    SHOPIFY_CLIENT_SECRET) and are never printed.
  * Transient trouble (429/5xx/timeouts/connection errors/THROTTLED) is
    retried per call with exponential backoff + jitter, honoring
    Retry-After; a 401 re-mints the ~24h token and replays. An entry that
    still fails is recorded FAILED and the run CONTINUES; the summary
    always prints, even on crash or Ctrl-C. (Backoff pattern vendored from
    the fleet's shopify_batch.py; deliberately not imported so the kill
    switch stays standalone in an emergency.)
  * Output is tee'd to a timestamped log file next to the manifest.

Exit codes:
  0  every entry verified killed (or a minority already absent)
  1  at least one entry FAILED or was REFUSED, or the run was interrupted
     (dry-run included: refused entries make a dry-run exit 1)
  2  argument or manifest validation error; nothing was attempted
  3  NOTHING WAS DONE: >=90% of targets resolved "already absent" and zero
     mutations ran. Either this is a verification re-run after a
     successful kill (fine, expected), or the run is aimed at the wrong
     store / bad token and silently did nothing. Distinct code so a false
     green can never look like a completed kill.

This script only removes/hides pages. It never creates or publishes anything.
"""

import argparse
import datetime
import json
import random
import re
import sys
import time
from pathlib import Path

import requests

STORE = "09c241-13.myshopify.com"  # Rings.com store (Publisher live-verified 2026-08-09)
API_VERSION = "2026-01"
TOKEN_URL = f"https://{STORE}/admin/oauth/access_token"
GRAPHQL_URL = f"https://{STORE}/admin/api/{API_VERSION}/graphql.json"

# Immutable page GIDs that must never be touched, checked BEFORE any fetch.
# Primary protection rail: survives renames. TODO(before any batch run):
# pin the real GIDs of the GemPages-built pages (US index, UK index,
# about/contact) via one read-only fetch. Until pinned, the script warns
# loudly that the handle blocklist is the only layer.
PROTECTED_IDS = set()

# Handle blocklist, second layer (rename-fragile, which is why the ID rail
# above is primary). UK variants are best-guess pending live verification.
PROTECTED_HANDLES = {
    "lab-grown-diamond-price-index",
    "lab-grown-diamond-price-index-uk",
    "uk-lab-grown-diamond-price-index",
    "about-us",
    "contact-us",
}

MUTATION_DELAY_S = 0.6   # inter-entry pause; measured cost (10 pts/mutation,
                         # 100 pts/s restore) means this can never throttle
MAX_RETRIES = 5
BASE_BACKOFF_S = 1.0     # doubles per retry, +/-30% jitter (vendored pattern)
ABSENT_ALARM_FRACTION = 0.9  # >=90% already-absent => exit 3, not 0

PAGE_GID_RE = re.compile(r"^gid://shopify/Page/\d+$")
ALLOWED_ENTRY_KEYS = {"id", "handle", "title", "note"}


class RunLog:
    """Print to stdout and tee every line to a timestamped log file."""

    def __init__(self, directory):
        stamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        self.path = Path(directory) / f"kill-switch-{stamp}.log"
        try:
            self._fh = open(self.path, "a")
        except OSError:
            self._fh = None  # logging must never block the kill itself
            self.path = None

    def line(self, msg):
        print(msg)
        if self._fh:
            self._fh.write(msg + "\n")
            self._fh.flush()

    def close(self):
        if self._fh:
            self._fh.close()


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


class Auth:
    """Token holder that can re-mint on 401 (tokens live ~24h; a stale
    token must not abort a kill run)."""

    def __init__(self, env):
        self.env = env
        self._token = None

    def token(self):
        if self._token is None:
            self._token = mint_token(self.env)
        return self._token

    def refresh(self):
        self._token = mint_token(self.env)
        return self._token


def _backoff(attempt, retry_after=None):
    """Seconds to wait: honor Retry-After when the server sent one, else
    exponential with +/-30% jitter (pattern vendored from shopify_batch)."""
    if retry_after:
        try:
            return max(0.0, float(retry_after))
        except ValueError:
            pass
    base = BASE_BACKOFF_S * (2 ** (attempt - 1))
    return base * random.uniform(0.7, 1.3)


def gql(auth, query, variables=None, log=None):
    """POST a GraphQL request. Retries 429 (honoring Retry-After), 5xx,
    timeouts/connection errors, and GraphQL THROTTLED with backoff+jitter.
    Re-mints the token once per call on 401. Raises only on persistent or
    non-retryable errors; the caller treats that as a per-entry failure,
    never a run abort."""
    say = log.line if log else print
    reminted = False
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                GRAPHQL_URL,
                json={"query": query, "variables": variables or {}},
                headers={"X-Shopify-Access-Token": auth.token()},
                timeout=30,
            )
        except requests.exceptions.RequestException as e:
            last_err = f"network: {type(e).__name__}"
            wait = _backoff(attempt)
            say(f"  transient network error ({type(e).__name__}), "
                f"retry {attempt}/{MAX_RETRIES} in {wait:.1f}s...")
            time.sleep(wait)
            continue

        if resp.status_code == 401 and not reminted:
            say("  401 unauthorized: re-minting token and replaying...")
            auth.refresh()
            reminted = True
            last_err = "HTTP 401"
            continue
        if resp.status_code == 429 or resp.status_code >= 500:
            headers = getattr(resp, "headers", None) or {}
            wait = _backoff(attempt, headers.get("Retry-After"))
            say(f"  HTTP {resp.status_code}, retry {attempt}/{MAX_RETRIES} "
                f"in {wait:.1f}s...")
            last_err = f"HTTP {resp.status_code}"
            time.sleep(wait)
            continue
        resp.raise_for_status()  # remaining 4xx: not retryable here, raise

        body = resp.json()
        errors = body.get("errors") or []
        if any((e.get("extensions") or {}).get("code") == "THROTTLED"
               for e in errors):
            wait = _backoff(attempt)
            say(f"  throttled (GraphQL), retry {attempt}/{MAX_RETRIES} "
                f"in {wait:.1f}s...")
            last_err = "THROTTLED"
            time.sleep(wait)
            continue
        if errors:
            raise RuntimeError(f"GraphQL errors: {errors}")
        return body["data"]
    raise RuntimeError(
        f"gave up after {MAX_RETRIES} attempts (last error: {last_err})")


def normalize_handle(h):
    """Trim, lowercase, strip a /pages/ or slash prefix so a padded or
    pasted-URL manifest still aims correctly in an emergency."""
    h = str(h).strip().lower()
    if h.startswith("/pages/"):
        h = h[len("/pages/"):]
    return h.strip("/")


def to_gid(raw):
    s = str(raw).strip()
    return s if s.startswith("gid://") else f"gid://shopify/Page/{s}"


def validate_manifest(entries, source_desc):
    """Validate EVERYTHING before the first API call. Returns a list of
    (gid, normalized_handle) or exits 2 with zero mutations attempted."""
    problems = []
    normalized = []
    seen = set()
    for i, entry in enumerate(entries):
        where = f"entry {i + 1}"
        if not isinstance(entry, dict):
            problems.append(f"{where}: not an object")
            continue
        unknown = set(entry) - ALLOWED_ENTRY_KEYS
        if unknown:
            problems.append(f"{where}: unknown keys {sorted(unknown)}")
            continue
        if "id" not in entry or entry["id"] in (None, ""):
            problems.append(f"{where}: missing 'id'")
            continue
        gid = to_gid(entry["id"])
        if not PAGE_GID_RE.match(gid):
            problems.append(
                f"{where}: id '{entry['id']}' is not a Page id "
                f"(need numeric or gid://shopify/Page/N)")
            continue
        if gid in seen:
            problems.append(f"{where}: duplicate id {gid}")
            continue
        seen.add(gid)
        handle = entry.get("handle")
        if not handle or not str(handle).strip():
            problems.append(
                f"{where} ({gid}): missing 'handle' (required: the handle "
                f"is the aim check)")
            continue
        normalized.append((gid, normalize_handle(handle)))
    if problems:
        for p in problems:
            print(f"manifest INVALID ({source_desc}): {p}")
        print(f"manifest INVALID: {len(problems)} problem(s); "
              f"NOTHING was attempted.")
        raise SystemExit(2)
    if not normalized:
        print(f"manifest {source_desc} contains no page entries")
        raise SystemExit(2)
    return normalized


def load_manifest(path):
    raw = json.loads(Path(path).read_text())
    entries = raw["pages"] if isinstance(raw, dict) else raw
    if not isinstance(entries, list):
        print(f"manifest {path}: top level must be a list or "
              f"{{'pages': [...]}}")
        raise SystemExit(2)
    return entries


def fetch_page(auth, gid, log=None):
    data = gql(
        auth,
        """
        query($id: ID!) {
          node(id: $id) {
            ... on Page { id handle title isPublished }
          }
        }
        """,
        {"id": gid},
        log=log,
    )
    node = data["node"]  # None if the id resolves to nothing
    if node is not None and (not isinstance(node, dict)
                             or "handle" not in node):
        # id resolved to a non-Page node: inline fragment matched nothing.
        # (Upfront validation makes this near-impossible, but never KeyError
        # mid-batch on a surprise.)
        raise RuntimeError(f"{gid} resolved to a non-Page node: {node!r}")
    return node


def delete_page(auth, gid, log=None):
    data = gql(
        auth,
        """
        mutation($id: ID!) {
          pageDelete(id: $id) {
            deletedPageId
            userErrors { field message }
          }
        }
        """,
        {"id": gid},
        log=log,
    )
    errs = data["pageDelete"]["userErrors"]
    if errs:
        raise RuntimeError(f"pageDelete userErrors: {errs}")
    return data["pageDelete"]["deletedPageId"]


def unpublish_page(auth, gid, log=None):
    data = gql(
        auth,
        """
        mutation($id: ID!, $page: PageUpdateInput!) {
          pageUpdate(id: $id, page: $page) {
            page { id isPublished }
            userErrors { field message }
          }
        }
        """,
        {"id": gid, "page": {"isPublished": False}},
        log=log,
    )
    errs = data["pageUpdate"]["userErrors"]
    if errs:
        raise RuntimeError(f"pageUpdate userErrors: {errs}")
    return data["pageUpdate"]["page"]


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--manifest", help="JSON manifest of pages to kill")
    src.add_argument("--id", help="single page ID (numeric or gid://) to "
                                  "kill; requires --handle")
    ap.add_argument("--handle",
                    help="expected live handle for --id (the aim check)")
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

    if args.id and not args.handle:
        ap.error("--id requires --handle (single-page kills get the same "
                 "aim check as manifest entries)")
    if args.manifest and args.handle:
        ap.error("--handle only applies to --id")

    here = Path(__file__).resolve().parent
    env_path = here / ".env"
    if not env_path.exists():
        print(f"missing {env_path}; cannot authenticate")
        raise SystemExit(2)
    env = load_env(env_path)
    for k in ("SHOPIFY_CLIENT_ID", "SHOPIFY_CLIENT_SECRET"):
        if not env.get(k):
            print(f"{k} not set in {env_path}")
            raise SystemExit(2)

    if args.manifest:
        raw_entries = load_manifest(args.manifest)
        targets = validate_manifest(raw_entries, args.manifest)
        log_dir = Path(args.manifest).resolve().parent
    else:
        targets = validate_manifest(
            [{"id": args.id, "handle": args.handle}], "--id/--handle")
        log_dir = Path.cwd()

    log = RunLog(log_dir)
    say = log.line
    if log.path:
        say(f"kill_switch log: {log.path}")

    auth = Auth(env)
    say(f"kill_switch: mode={args.mode} targets={len(targets)} "
        f"{'LIVE RUN' if args.yes else 'DRY RUN (no changes; add --yes)'}")
    if not PROTECTED_IDS:
        say("WARNING: PROTECTED_IDS is not pinned yet; the handle blocklist "
            "is the only protection layer this run.")

    ok = failed = refused = absent = 0
    interrupted = False
    exit_code = 1
    try:
        for gid, expected_handle in targets:
            try:
                if gid in PROTECTED_IDS:
                    say(f"  {gid}: PROTECTED id -- REFUSED. This page must "
                        f"never be touched via the API.")
                    refused += 1
                    continue

                page = fetch_page(auth, gid, log=log)
                if page is None:
                    say(f"  {gid}: already absent -- OK (idempotent)")
                    absent += 1
                    continue

                handle, title = page["handle"], page["title"]
                live_handle = normalize_handle(handle)
                if live_handle in PROTECTED_HANDLES:
                    say(f"  {gid} (/pages/{handle}): PROTECTED handle -- "
                        f"REFUSED. This page must never be touched via "
                        f"the API.")
                    refused += 1
                    continue
                if live_handle != expected_handle:
                    say(f"  {gid}: live handle '{handle}' != manifest "
                        f"handle '{expected_handle}' -- REFUSED "
                        f"(possible ID mixup)")
                    refused += 1
                    continue

                if not args.yes:
                    say(f"  {gid} (/pages/{handle}, '{title}', "
                        f"published={page['isPublished']}): "
                        f"would {args.mode}")
                    ok += 1
                    continue

                if args.mode == "delete":
                    delete_page(auth, gid, log=log)
                    after = fetch_page(auth, gid, log=log)
                    if after is None:
                        say(f"  {gid} (/pages/{handle}): deleted, "
                            f"verified gone")
                        ok += 1
                    else:
                        say(f"  {gid}: pageDelete returned but page still "
                            f"exists -- FAILED verification")
                        failed += 1
                else:
                    unpublish_page(auth, gid, log=log)
                    after = fetch_page(auth, gid, log=log)
                    if after and after["isPublished"] is False:
                        say(f"  {gid} (/pages/{handle}): unpublished, "
                            f"verified isPublished=false")
                        ok += 1
                    else:
                        say(f"  {gid}: unpublish not verified -- FAILED")
                        failed += 1
                time.sleep(MUTATION_DELAY_S)
            except Exception as e:
                # Per-entry failure: record and KEEP GOING. One bad page or
                # one persistent transient must not strand the rest of a
                # 1,000-page kill half-done with no summary.
                say(f"  {gid}: ERROR {e} -- recorded as FAILED, continuing")
                failed += 1
                time.sleep(MUTATION_DELAY_S)
    except KeyboardInterrupt:
        interrupted = True
    finally:
        processed = ok + failed + refused + absent
        say(f"kill_switch summary: ok={ok} already_absent={absent} "
            f"failed={failed} refused={refused} processed={processed} "
            f"of {len(targets)}"
            + (" [INTERRUPTED before completion]" if interrupted else ""))

        if interrupted or failed or refused or processed < len(targets):
            say("RESULT: NOT CLEAN -- re-run this exact command after "
                "fixing the above; already-killed pages are skipped "
                "idempotently.")
            exit_code = 1
        elif args.yes and absent / len(targets) >= ABSENT_ALARM_FRACTION:
            say(f"WARNING: NOTHING WAS DONE -- {absent}/{len(targets)} "
                f"targets resolved 'already absent' and zero pages were "
                f"actually {args.mode}d. If this was not a verification "
                f"re-run after a successful kill, this run may be aimed "
                f"at the wrong store or using a token that cannot see "
                f"these pages. Exit code 3 so a false green can never "
                f"pass as a completed kill.")
            exit_code = 3
        else:
            exit_code = 0
        log.close()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
