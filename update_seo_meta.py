"""update_seo_meta: refresh the SEO metafields (global.title_tag /
global.description_tag) of already-LIVE pilot pages.

Companion to update_batch.py (which is body-only). Every monthly refresh
changes the SEO title (cycle label) and description (numbers) of every
page, and the delisted-stone variant changes titles materially, so the
metafields must be refreshed alongside the bodies or search results keep
showing last month's title.

Same rails as update_batch.py: targets come ONLY from the live rollback
manifest; PROTECTED refused; requires an AUTHORIZED gate report (the same
one the body update runs under); dry-run by default prints a per-page
diff of live vs new; --yes writes via metafieldsSet and re-reads each
page to verify; JSON run log in manifests/.

Usage:
  python3 update_seo_meta.py --manifest manifests/batch-1-canary-rollback-manifest.json \
      --meta page_output_2026-08/refresh-2026-08-meta.json \
      --gate-report OUTBOX/<AUTHORIZED report>.md [--yes]
"""
import argparse
import datetime
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from kill_switch import (  # noqa: E402
    Auth, gql, load_env, normalize_handle, PROTECTED_HANDLES, PROTECTED_IDS,
)

META_Q = """
query($id: ID!) {
  page(id: $id) {
    id handle isPublished
    metafields(namespace: "global", first: 10) { nodes { id key value } }
  }
}
"""
META_M = """
mutation($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { key value }
    userErrors { field message code }
  }
}
"""
KEYS = ("title_tag", "description_tag")


def live_meta(auth, gid):
    p = gql(auth, META_Q, {"id": gid})["page"]
    if p is None:
        return None, {}
    return p, {n["key"]: n["value"] for n in p["metafields"]["nodes"]}


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--meta", required=True, help="<handle>: {title_tag, description_tag}")
    ap.add_argument("--gate-report", required=True)
    ap.add_argument("--yes", action="store_true")
    args = ap.parse_args()

    gate_report = Path(args.gate_report)
    if not gate_report.exists():
        raise SystemExit(f"gate report not found: {gate_report}")
    if "VERDICT: AUTHORIZED" not in gate_report.read_text():
        raise SystemExit("gate report is not AUTHORIZED; refusing")
    manifest = json.loads(Path(args.manifest).read_text())
    meta = json.loads(Path(args.meta).read_text())

    problems = []
    for e in manifest["pages"]:
        h = normalize_handle(e["handle"])
        gid = e.get("page_id") or ""
        if not gid:
            problems.append(f"{h}: no recorded page_id")
        if h in PROTECTED_HANDLES or gid in PROTECTED_IDS:
            problems.append(f"{h}: PROTECTED; refusing the whole run")
        if h not in meta or not all(k in meta[h] for k in KEYS):
            problems.append(f"{h}: no new meta")
    if problems:
        for p in problems:
            print(f"INVALID: {p}")
        raise SystemExit(2)

    env = load_env(HERE / ".env")
    auth = Auth(env)
    changes, same, results = [], 0, []
    for e in manifest["pages"]:
        h = normalize_handle(e["handle"])
        gid = e["page_id"]
        page, cur = live_meta(auth, gid)
        if page is None or normalize_handle(page["handle"]) != h or not page["isPublished"]:
            print(f"REFUSED {h}: absent / handle mismatch / not published")
            results.append({"handle": h, "status": "refused"})
            continue
        diff = {k: (cur.get(k), meta[h][k]) for k in KEYS if cur.get(k) != meta[h][k]}
        if not diff:
            same += 1
            results.append({"handle": h, "status": "already-current"})
            continue
        changes.append((h, gid, diff))
        for k, (old, new) in diff.items():
            print(f"{h} {k}:\n   live: {old}\n   new:  {new}")
    print(f"\n{len(changes)} page(s) with metafield changes, {same} already current, "
          f"of {len(manifest['pages'])}")
    if not args.yes:
        print("DRY RUN: nothing written. Add --yes to apply.")
        raise SystemExit(0)

    stamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    ok = failed = 0
    for h, gid, diff in changes:
        inputs = [{"ownerId": gid, "namespace": "global", "key": k,
                   "type": "single_line_text_field", "value": new}
                  for k, (_, new) in diff.items()]
        out = gql(auth, META_M, {"metafields": inputs})["metafieldsSet"]
        if out["userErrors"]:
            print(f"FAILED {h}: {out['userErrors']}")
            failed += 1
            results.append({"handle": h, "status": "userErrors", "errors": out["userErrors"]})
            continue
        _, after = live_meta(auth, gid)
        verified = all(after.get(k) == new for k, (_, new) in diff.items())
        if verified:
            ok += 1
            print(f"UPDATED {h} ({', '.join(diff)}) verified by re-read")
            results.append({"handle": h, "status": "updated", "keys": list(diff), "at": stamp})
        else:
            failed += 1
            print(f"FAILED {h}: re-read does not match")
            results.append({"handle": h, "status": "verify-failed"})
    outlog = HERE / "manifests" / ("update-seo-meta-" + datetime.datetime.now(
        datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + ".json")
    outlog.write_text(json.dumps({"gate_report": str(gate_report), "results": results}, indent=1) + "\n")
    print(f"summary: updated={ok} failed={failed} already={same}; run log: {outlog}")
    raise SystemExit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
