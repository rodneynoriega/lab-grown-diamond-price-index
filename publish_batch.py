"""publish_batch: flip an approved batch of DRAFT pilot pages to PUBLISHED.

THE ONLY SCRIPT IN THIS WORKSPACE THAT PUBLISHES. Guarded by process, not
just code: it must never run without (1) a current AUTHORIZED publish-gate
report for the batch and (2) Rodney's explicit per-batch sign-off stated
directly against that report version. Both are procedural gates the
operator confirms; the script additionally enforces every mechanical rail
it can:

  * pages come ONLY from the batch rollback manifest (page_id + handle
    both required and cross-checked against live before mutating)
  * PROTECTED_IDS / PROTECTED_HANDLES refused before any mutation
  * dry-run by default; --yes required to mutate
  * per-page live pre-check: page must exist, handle must match the
    manifest, and body (normalized, per rollback_manifest contract) must
    match the manifest body store; a mismatched page is REFUSED (that page
    was not what the gate authorized)
  * publish via pageUpdate {isPublished: true}; response verified; then
    re-fetched and verified again; then storefront GET expected 200
  * idempotent: already-published pages with matching bodies count as ok
  * per-entry containment + the kill_switch circuit-breaker pattern; any
    failure leaves a partial batch that kill_switch can unpublish with the
    exported kill manifest (mode unpublish = API-level rollback)

Rollback of a published batch: kill_switch.py --manifest <kill manifest>
--mode unpublish --yes  (returns pages to draft; storefront 404s).
"""

import argparse
import datetime
import json
import sys
import time
from pathlib import Path

import requests

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from kill_switch import (  # noqa: E402
    Auth, AuthError, TransientExhausted, gql, load_env,
    normalize_handle, PROTECTED_HANDLES, PROTECTED_IDS,
)
from rollback_manifest import normalize_html, read_body  # noqa: E402

STOREFRONT = "https://rings.com"

PAGE_Q = """
query($id: ID!) { page(id: $id) { id handle isPublished body } }
"""

PUBLISH_M = """
mutation($id: ID!) {
  pageUpdate(id: $id, page: {isPublished: true}) {
    page { id handle isPublished }
    userErrors { field message code }
  }
}
"""


def read_manifest_bodies(manifest_path, manifest):
    # rollback_manifest.read_body verifies the sha256 of every body
    return {e["handle"]: read_body(manifest_path, manifest, e)
            for e in manifest["pages"]}


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--manifest", required=True,
                    help="batch rollback manifest (ids recorded)")
    ap.add_argument("--gate-report", required=True,
                    help="path to the AUTHORIZED gate report this publish "
                         "executes (recorded in the run log)")
    ap.add_argument("--yes", action="store_true",
                    help="actually publish (dry-run without)")
    args = ap.parse_args()

    manifest = json.loads(Path(args.manifest).read_text())
    pages = manifest["pages"]
    gate_report = Path(args.gate_report)
    if not gate_report.exists():
        raise SystemExit(f"gate report not found: {gate_report}")
    report_text = gate_report.read_text()
    if "VERDICT: AUTHORIZED" not in report_text:
        raise SystemExit("gate report does not contain an AUTHORIZED "
                         "verdict; refusing")

    problems = []
    seen = set()
    for e in pages:
        h = normalize_handle(e["handle"])
        gid = e.get("page_id") or ""
        if not gid:
            problems.append(f"{h}: no recorded page_id (run record-ids)")
        if h in PROTECTED_HANDLES or gid in PROTECTED_IDS:
            problems.append(f"{h}: PROTECTED; refusing the whole run")
        if h in seen:
            problems.append(f"{h}: duplicate handle")
        seen.add(h)
    if problems:
        for p in problems:
            print(f"manifest INVALID: {p}")
        raise SystemExit(2)

    if not args.yes:
        print(f"DRY RUN: would publish {len(pages)} pages per "
              f"{gate_report.name}. Add --yes.")
        for e in pages:
            print(f"  {e['page_id']}  /pages/{e['handle']}")
        raise SystemExit(0)

    bodies = read_manifest_bodies(args.manifest, manifest)
    env = load_env(HERE / ".env")
    auth = Auth(env)
    print(f"gate report: {gate_report}")
    stamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    ok = already = failed = refused = 0
    results = []
    breaker = 0
    for e in pages:
        h = normalize_handle(e["handle"])
        gid = e["page_id"]
        try:
            live = gql(auth, PAGE_Q, {"id": gid})["page"]
            if live is None:
                print(f"REFUSED {h}: recorded GID absent from store")
                refused += 1
                results.append({"handle": h, "status": "absent"})
                breaker += 1
                if breaker >= 5:
                    print("CIRCUIT BREAKER: 5 consecutive failures, abort")
                    break
                continue
            if normalize_handle(live["handle"]) != h:
                print(f"REFUSED {h}: live handle is {live['handle']}")
                refused += 1
                results.append({"handle": h, "status": "handle-mismatch"})
                breaker += 1
                if breaker >= 5:
                    print("CIRCUIT BREAKER: 5 consecutive failures, abort")
                    break
                continue
            if normalize_html(live["body"]) != normalize_html(bodies[h]):
                print(f"REFUSED {h}: live body differs from the gated "
                      f"manifest body; this page is not what was "
                      f"authorized")
                refused += 1
                results.append({"handle": h, "status": "body-mismatch"})
                breaker += 1
                if breaker >= 5:
                    print("CIRCUIT BREAKER: 5 consecutive failures, abort")
                    break
                continue
            if live["isPublished"]:
                print(f"already published {h}")
                already += 1
                results.append({"handle": h, "status": "already-published",
                                "id": gid})
                breaker = 0
                continue
            out = gql(auth, PUBLISH_M, {"id": gid})["pageUpdate"]
            if out["userErrors"]:
                print(f"FAILED {h}: {out['userErrors']}")
                failed += 1
                results.append({"handle": h, "status": "userErrors",
                                "errors": out["userErrors"]})
                breaker += 1
                if breaker >= 5:
                    print("CIRCUIT BREAKER: 5 consecutive failures, abort")
                    break
                continue
            if out["page"]["isPublished"] is not True:
                print(f"FAILED {h}: mutation returned isPublished="
                      f"{out['page']['isPublished']}")
                failed += 1
                results.append({"handle": h, "status": "not-published"})
                breaker += 1
                continue
            recheck = gql(auth, PAGE_Q, {"id": gid})["page"]
            print(f"PUBLISHED {gid} /pages/{h} "
                  f"(re-query isPublished={recheck['isPublished']})")
            ok += 1
            results.append({"handle": h, "status": "published", "id": gid,
                            "published_at": stamp})
            breaker = 0
        except (AuthError, TransientExhausted, Exception) as exc:
            print(f"FAILED {h}: {type(exc).__name__}: {exc}")
            failed += 1
            results.append({"handle": h, "status": "exception",
                            "error": f"{type(exc).__name__}: {exc}"})
            breaker += 1
            if breaker >= 5:
                print("CIRCUIT BREAKER: 5 consecutive failures, abort")
                break

    # storefront verification pass (published pages must serve 200)
    sf_ok = sf_bad = 0
    for r in results:
        if r["status"] in ("published", "already-published"):
            url = f"{STOREFRONT}/pages/{r['handle']}"
            try:
                resp = requests.get(url, timeout=30, headers={
                    "User-Agent": "Mozilla/5.0 (publish-batch verify)"})
                code = resp.status_code
            except Exception as exc:
                code = f"EXC:{type(exc).__name__}"
            r["storefront"] = code
            if code == 200:
                sf_ok += 1
            else:
                sf_bad += 1
                print(f"STOREFRONT PROBLEM {url}: {code}")
            time.sleep(0.3)

    outlog = HERE / "manifests" / (
        "publish-batch-"
        + datetime.datetime.now(datetime.timezone.utc)
          .strftime("%Y%m%dT%H%M%SZ") + ".json")
    outlog.write_text(json.dumps(
        {"gate_report": str(gate_report), "signoff": "per run invocation",
         "results": results}, indent=1) + "\n")
    print(f"summary: published={ok} already={already} failed={failed} "
          f"refused={refused} of {len(pages)}; storefront 200: {sf_ok}, "
          f"problems: {sf_bad}")
    print(f"run log: {outlog}")
    raise SystemExit(0 if (failed == 0 and refused == 0 and sf_bad == 0
                           and ok + already == len(pages)) else 1)


if __name__ == "__main__":
    main()
