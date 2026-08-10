#!/usr/bin/env python3
"""Rollback manifest machinery for Shopify pilot batches (queue item #4).

Records EVERYTHING a pilot batch will create BEFORE any publish, so the
batch is fully reversible in both directions:
  - kill direction: export a kill_switch.py-compatible manifest (removal)
  - restore direction: recreate any page from the stored body as a DRAFT
    (recovery from an over-aggressive kill or an accidental delete)

Workflow per batch:
  1. snapshot    - freeze the batch spec into a rollback manifest + gzipped
                   body store, BEFORE anything is created in Shopify.
  2. (pages are created as drafts by the page pipeline, item #6)
  3. record-ids  - look up the live page GIDs by handle and write them into
                   the manifest (the kill switch needs ids).
  4. export-kill - emit the kill-switch manifest for this batch.
  5. restore     - recreate one page (or verify restorability) from the
                   manifest. ALWAYS a draft: isPublished is hard-forced
                   False here, publishing is publish-gate territory.

Usage:
  python3 rollback_manifest.py snapshot   --spec spec.json --batch NAME
  python3 rollback_manifest.py record-ids --manifest M.json
  python3 rollback_manifest.py export-kill --manifest M.json --out K.json
  python3 rollback_manifest.py restore    --manifest M.json --handle H [--yes]
  python3 rollback_manifest.py verify     --manifest M.json --handle H

Safety rails:
  - restore NEVER publishes: pageCreate is sent with isPublished False,
    and the post-create fetch verifies isPublished is actually false.
  - Protected handles/ids (imported from kill_switch.py, the pinned rails)
    are refused in every mode that touches the API.
  - restore refuses to create a handle that already exists live (no
    accidental duplicates; Shopify would silently suffix the handle).
  - Dry-run by default for restore; --yes required to mutate.
  - Bodies are stored gzipped next to the manifest with sha256 recorded;
    snapshot round-trips (write, re-read, re-hash) before reporting OK.
  - API access reuses kill_switch's client (backoff + jitter + Retry-After
    + 401 re-mint, the pattern vendored fleet-wide from shopify_batch.py),
    so there is exactly one hardened client in this repo.

Exit codes: 0 ok; 1 a check failed or an entry was refused; 2 bad
arguments/spec/manifest (nothing attempted).

Manifest hygiene (same runbook as kill_switch): manifests are REGENERATED
per batch, never appended across batches. One batch = one manifest = one
body store directory.
"""

import argparse
import datetime
import gzip
import hashlib
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from kill_switch import (  # noqa: E402  (single hardened API client)
    Auth, PROTECTED_HANDLES, PROTECTED_IDS, fetch_page, gql, load_env,
    normalize_handle,
)

HANDLE_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
REQUIRED_SPEC_KEYS = {"handle", "title", "body_file"}
OPTIONAL_SPEC_KEYS = {"template_suffix", "source", "target_keyword", "note"}


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


def sha256_bytes(b):
    return hashlib.sha256(b).hexdigest()


def normalize_html(s):
    """Shopify's pageCreate normalizes stored HTML (inserts newlines around
    block tags). Live-vs-manifest body comparison is therefore done on a
    whitespace-normalized form; equality here proves the difference is
    whitespace-only. The raw sha256 in the manifest still guards the local
    body STORE byte-for-byte."""
    s = re.sub(r">\s+<", "><", s or "")
    return re.sub(r"\s+", " ", s).strip()


def die(msg, code=2):
    print(msg)
    raise SystemExit(code)


def auth_from_env():
    env_path = HERE / ".env"
    if not env_path.exists():
        die(f"missing {env_path}; cannot authenticate")
    env = load_env(env_path)
    for k in ("SHOPIFY_CLIENT_ID", "SHOPIFY_CLIENT_SECRET"):
        if not env.get(k):
            die(f"{k} not set in {env_path}")
    return Auth(env)


def load_manifest(path):
    try:
        m = json.loads(Path(path).read_text())
    except OSError as e:
        die(f"manifest {path}: cannot read ({e})")
    except json.JSONDecodeError as e:
        die(f"manifest {path}: malformed JSON ({e})")
    for k in ("batch", "created_at", "pages", "body_dir"):
        if k not in m:
            die(f"manifest {path}: missing key '{k}'")
    return m


def save_manifest(path, m):
    Path(path).write_text(json.dumps(m, indent=2) + "\n")


def find_entry(manifest, handle):
    h = normalize_handle(handle)
    for e in manifest["pages"]:
        if e["handle"] == h:
            return e
    die(f"handle '{h}' not in manifest", 2)


def body_path(manifest_path, manifest, entry):
    return Path(manifest_path).resolve().parent / manifest["body_dir"] / (
        entry["handle"] + ".html.gz")


def read_body(manifest_path, manifest, entry):
    p = body_path(manifest_path, manifest, entry)
    if not p.exists():
        die(f"body store missing: {p}", 1)
    body = gzip.decompress(p.read_bytes()).decode("utf-8")
    got = sha256_bytes(body.encode("utf-8"))
    if got != entry["body_sha256"]:
        die(f"body store CORRUPT for {entry['handle']}: sha256 {got} != "
            f"manifest {entry['body_sha256']}", 1)
    return body


def page_by_handle(auth, handle):
    data = gql(auth, """
      query($q: String!) {
        pages(first: 2, query: $q) {
          nodes { id handle title isPublished }
        }
      }
    """, {"q": f"handle:{handle}"})
    nodes = [n for n in data["pages"]["nodes"]
             if normalize_handle(n["handle"]) == handle]
    return nodes[0] if nodes else None


def fetch_body(auth, gid):
    data = gql(auth, """
      query($id: ID!) {
        node(id: $id) { ... on Page { id handle title isPublished body } }
      }
    """, {"id": gid})
    return data["node"]


# ---------------------------------------------------------------- snapshot

def cmd_snapshot(args):
    try:
        spec = json.loads(Path(args.spec).read_text())
    except (OSError, json.JSONDecodeError) as e:
        die(f"spec {args.spec}: {e}")
    if isinstance(spec, dict):
        spec = spec.get("pages")
    if not isinstance(spec, list) or not spec:
        die(f"spec {args.spec}: need a non-empty list (or {{'pages': [...]}})")

    out_dir = Path(args.out_dir or (HERE / "manifests")).resolve()
    body_dir_name = f"bodies-{args.batch}"
    body_dir = out_dir / body_dir_name
    manifest_path = out_dir / f"{args.batch}-rollback-manifest.json"
    if manifest_path.exists() and not args.force:
        die(f"{manifest_path} already exists; manifests are regenerated per "
            f"batch, not appended. Use --force to overwrite THIS batch's "
            f"manifest deliberately.")

    problems, pages, seen = [], [], set()
    for i, e in enumerate(spec):
        w = f"spec entry {i + 1}"
        if not isinstance(e, dict):
            problems.append(f"{w}: not an object")
            continue
        missing = REQUIRED_SPEC_KEYS - set(e)
        unknown = set(e) - REQUIRED_SPEC_KEYS - OPTIONAL_SPEC_KEYS
        if missing:
            problems.append(f"{w}: missing {sorted(missing)}")
            continue
        if unknown:
            problems.append(f"{w}: unknown keys {sorted(unknown)}")
            continue
        h = normalize_handle(e["handle"])
        if not HANDLE_RE.match(h):
            problems.append(f"{w}: bad handle '{e['handle']}'")
            continue
        if h in PROTECTED_HANDLES:
            problems.append(f"{w}: handle '{h}' is PROTECTED; a pilot batch "
                            f"may never claim it")
            continue
        if h in seen:
            problems.append(f"{w}: duplicate handle '{h}'")
            continue
        seen.add(h)
        body_file = Path(e["body_file"])
        if not body_file.is_absolute():
            body_file = Path(args.spec).resolve().parent / body_file
        if not body_file.exists():
            problems.append(f"{w}: body_file {body_file} does not exist")
            continue
        pages.append((h, e, body_file))
    if problems:
        for p in problems:
            print(f"spec INVALID: {p}")
        die(f"spec INVALID: {len(problems)} problem(s); nothing written.")

    body_dir.mkdir(parents=True, exist_ok=True)
    entries = []
    for h, e, body_file in pages:
        body = body_file.read_text()
        digest = sha256_bytes(body.encode("utf-8"))
        gz_path = body_dir / f"{h}.html.gz"
        gz_path.write_bytes(gzip.compress(body.encode("utf-8")))
        # round-trip check before claiming the body is stored
        back = gzip.decompress(gz_path.read_bytes()).decode("utf-8")
        if sha256_bytes(back.encode("utf-8")) != digest:
            die(f"round-trip FAILED for {h}; body store unreliable", 1)
        entries.append({
            "handle": h,
            "title": e["title"],
            "template_suffix": e.get("template_suffix"),
            "body_sha256": digest,
            "body_bytes": len(body.encode("utf-8")),
            "source": e.get("source", {}),
            "target_keyword": e.get("target_keyword"),
            "note": e.get("note"),
            "page_id": None,       # filled by record-ids after creation
            "recorded_at": None,
        })
    manifest = {
        "batch": args.batch,
        "created_at": utcnow(),
        "store": "09c241-13.myshopify.com",
        "body_dir": body_dir_name,
        "pages": entries,
    }
    save_manifest(manifest_path, manifest)
    print(f"rollback manifest written: {manifest_path}")
    print(f"body store: {body_dir} ({len(entries)} bodies, round-tripped)")
    print(f"entries: {len(entries)}; ids pending record-ids after creation")


# -------------------------------------------------------------- record-ids

def cmd_record_ids(args):
    manifest = load_manifest(args.manifest)
    auth = auth_from_env()
    missing = 0
    for e in manifest["pages"]:
        page = page_by_handle(auth, e["handle"])
        if page is None:
            print(f"  {e['handle']}: NOT FOUND live (not created yet?)")
            missing += 1
            continue
        if page["id"] in PROTECTED_IDS:
            die(f"  {e['handle']}: resolves to a PROTECTED id; refusing", 1)
        e["page_id"] = page["id"]
        e["recorded_at"] = utcnow()
        print(f"  {e['handle']}: {page['id']} "
              f"(published={page['isPublished']})")
    save_manifest(args.manifest, manifest)
    print(f"record-ids: {len(manifest['pages']) - missing} recorded, "
          f"{missing} missing")
    raise SystemExit(1 if missing else 0)


# -------------------------------------------------------------- export-kill

def cmd_export_kill(args):
    manifest = load_manifest(args.manifest)
    pages, skipped = [], 0
    for e in manifest["pages"]:
        if not e.get("page_id"):
            print(f"  {e['handle']}: no page_id recorded; SKIPPED "
                  f"(run record-ids first)")
            skipped += 1
            continue
        pages.append({"id": e["page_id"], "handle": e["handle"],
                      "title": e["title"]})
    out = {"pages": pages}
    Path(args.out).write_text(json.dumps(out, indent=2) + "\n")
    print(f"kill manifest written: {args.out} ({len(pages)} entries, "
          f"{skipped} skipped)")
    raise SystemExit(1 if skipped else 0)


# ----------------------------------------------------------------- restore

def cmd_restore(args):
    manifest = load_manifest(args.manifest)
    entry = find_entry(manifest, args.handle)
    if entry["handle"] in PROTECTED_HANDLES:
        die(f"{entry['handle']}: PROTECTED handle; refusing", 1)
    body = read_body(args.manifest, manifest, entry)
    auth = auth_from_env()

    existing = page_by_handle(auth, entry["handle"])
    if existing is not None:
        die(f"restore REFUSED: /pages/{entry['handle']} already exists live "
            f"({existing['id']}, published={existing['isPublished']}). "
            f"Shopify would silently create a suffixed duplicate handle. "
            f"Delete/inspect the live page first.", 1)

    if not args.yes:
        print(f"DRY RUN: would create DRAFT /pages/{entry['handle']} "
              f"('{entry['title']}', {entry['body_bytes']} bytes, "
              f"sha256 {entry['body_sha256'][:12]}...). Add --yes.")
        raise SystemExit(0)

    page_input = {
        "title": entry["title"],
        "handle": entry["handle"],
        "body": body,
        "isPublished": False,   # hard rule: restore NEVER publishes
    }
    if entry.get("template_suffix"):
        page_input["templateSuffix"] = entry["template_suffix"]
    data = gql(auth, """
      mutation($page: PageCreateInput!) {
        pageCreate(page: $page) {
          page { id handle title isPublished }
          userErrors { field message }
        }
      }
    """, {"page": page_input})
    out = data["pageCreate"]
    if out["userErrors"]:
        die(f"pageCreate userErrors: {out['userErrors']}", 1)
    page = out["page"]
    print(f"created: {page['id']} /pages/{page['handle']} "
          f"published={page['isPublished']}")

    # independent verification fetch
    live = fetch_body(auth, page["id"])
    checks = {
        "handle": normalize_handle(live["handle"]) == entry["handle"],
        "title": live["title"] == entry["title"],
        "draft": live["isPublished"] is False,
        "body_normalized": normalize_html(live["body"]) ==
                           normalize_html(body),
    }
    for k, ok in checks.items():
        print(f"  verify {k}: {'OK' if ok else 'MISMATCH'}")
    if checks["body_normalized"]:
        raw_same = sha256_bytes(
            (live["body"] or "").encode("utf-8")) == entry["body_sha256"]
        if not raw_same:
            print("  (raw bytes differ only by Shopify's whitespace "
                  "normalization; normalized forms are identical)")
    raise SystemExit(0 if all(checks.values()) else 1)


# ------------------------------------------------------------------ verify

def cmd_verify(args):
    manifest = load_manifest(args.manifest)
    entry = find_entry(manifest, args.handle)
    stored = read_body(args.manifest, manifest, entry)  # store integrity
    print(f"{entry['handle']}: body store intact "
          f"(sha256 {entry['body_sha256'][:12]}...)")
    auth = auth_from_env()
    page = page_by_handle(auth, entry["handle"])
    if page is None:
        print(f"{entry['handle']}: not live (absent)")
        raise SystemExit(0)
    live = fetch_body(auth, page["id"])
    same = normalize_html(live["body"]) == normalize_html(stored)
    print(f"{entry['handle']}: live {page['id']} "
          f"published={page['isPublished']} "
          f"body (normalized) {'MATCHES' if same else 'DIFFERS FROM'} "
          f"manifest")
    raise SystemExit(0 if same else 1)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("snapshot", help="freeze a batch spec into a manifest")
    s.add_argument("--spec", required=True)
    s.add_argument("--batch", required=True,
                   help="batch name, e.g. batch-1-canary")
    s.add_argument("--out-dir", default=None)
    s.add_argument("--force", action="store_true")
    s.set_defaults(fn=cmd_snapshot)

    r = sub.add_parser("record-ids", help="record live GIDs into manifest")
    r.add_argument("--manifest", required=True)
    r.set_defaults(fn=cmd_record_ids)

    k = sub.add_parser("export-kill", help="emit kill_switch manifest")
    k.add_argument("--manifest", required=True)
    k.add_argument("--out", required=True)
    k.set_defaults(fn=cmd_export_kill)

    t = sub.add_parser("restore", help="recreate one page as a DRAFT")
    t.add_argument("--manifest", required=True)
    t.add_argument("--handle", required=True)
    t.add_argument("--yes", action="store_true")
    t.set_defaults(fn=cmd_restore)

    v = sub.add_parser("verify", help="check store integrity + live state")
    v.add_argument("--manifest", required=True)
    v.add_argument("--handle", required=True)
    v.set_defaults(fn=cmd_verify)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
