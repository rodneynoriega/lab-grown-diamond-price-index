"""update_batch: in-place body update of already-PUBLISHED pilot pages.

The refresh path for live pages (style fixes, monthly data refreshes).
Never creates, deletes, publishes, or unpublishes anything: pageUpdate
body only, on pages that are already live. Kill/recreate is the wrong
tool once a page is published (URL/GID churn); this is the right one.

An in-place body update of a published page IS a publish under the
standing instruction, so process gates apply exactly as for
publish_batch: a current AUTHORIZED publish-gate report for the new
bodies plus Rodney's explicit sign-off against that report version.

Mechanical rails:
  * targets come ONLY from the batch rollback manifest (the record of
    what is currently live); PROTECTED refused; dry-run default
  * per-page pre-check: live page exists, handle matches, is published,
    and live body matches the OLD manifest body (normalized) -- proves
    we are replacing exactly the content that was previously gated; any
    mismatch is REFUSED (out-of-band edit detected, investigate first)
  * new body comes from --pages-dir/<handle>.html (the freshly gated
    build output)
  * post-update: response checked, page re-fetched, normalized equality
    to the new body verified, script blocks compared raw (entity-safe)
  * idempotent: live body already matching the new body counts as ok
  * circuit breaker after 5 consecutive failures; JSON run log

AFTER a successful run: re-snapshot the rollback manifest from the new
spec and re-run record-ids so the manifest matches live again:
  rollback_manifest.py snapshot --spec <new spec> --batch <batch> --force
  rollback_manifest.py record-ids --manifest <manifest>
(The kill manifest stays valid: GIDs do not change.)
"""

import argparse
import datetime
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from kill_switch import (  # noqa: E402
    Auth, gql, load_env, normalize_handle,
    PROTECTED_HANDLES, PROTECTED_IDS,
)
from rollback_manifest import normalize_html, read_body  # noqa: E402

PAGE_Q = """
query($id: ID!) { page(id: $id) { id handle isPublished body } }
"""

UPDATE_M = """
mutation($id: ID!, $body: String!) {
  pageUpdate(id: $id, page: {body: $body}) {
    page { id handle isPublished }
    userErrors { field message code }
  }
}
"""

SCRIPT_RE = re.compile(r"<script[^>]*>(.*?)</script>", re.S | re.I)


def scripts_of(s):
    return [re.sub(r"\s+", " ", x).strip() for x in SCRIPT_RE.findall(s or "")]


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--manifest", required=True,
                    help="rollback manifest of the CURRENTLY LIVE batch")
    ap.add_argument("--pages-dir", required=True,
                    help="directory of freshly gated new bodies")
    ap.add_argument("--gate-report", required=True,
                    help="AUTHORIZED gate report covering the new bodies")
    ap.add_argument("--yes", action="store_true",
                    help="actually update (dry-run without)")
    args = ap.parse_args()

    manifest = json.loads(Path(args.manifest).read_text())
    pages = manifest["pages"]
    pages_dir = Path(args.pages_dir)
    gate_report = Path(args.gate_report)
    if not gate_report.exists():
        raise SystemExit(f"gate report not found: {gate_report}")
    if "VERDICT: AUTHORIZED" not in gate_report.read_text():
        raise SystemExit("gate report is not AUTHORIZED; refusing")

    problems = []
    new_bodies = {}
    for e in pages:
        h = normalize_handle(e["handle"])
        gid = e.get("page_id") or ""
        if not gid:
            problems.append(f"{h}: no recorded page_id")
        if h in PROTECTED_HANDLES or gid in PROTECTED_IDS:
            problems.append(f"{h}: PROTECTED; refusing the whole run")
        f = pages_dir / f"{h}.html"
        if not f.exists():
            problems.append(f"{h}: new body missing ({f})")
        else:
            new_bodies[h] = f.read_text()
    if problems:
        for p in problems:
            print(f"INVALID: {p}")
        raise SystemExit(2)

    if not args.yes:
        print(f"DRY RUN: would update {len(pages)} live page bodies per "
              f"{gate_report.name}. Add --yes.")
        raise SystemExit(0)

    env = load_env(HERE / ".env")
    auth = Auth(env)
    stamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    ok = already = failed = refused = 0
    results = []
    breaker = 0
    for e in pages:
        h = normalize_handle(e["handle"])
        gid = e["page_id"]
        try:
            old_body = read_body(args.manifest, manifest, e)
            live = gql(auth, PAGE_Q, {"id": gid})["page"]
            if (live is None or normalize_handle(live["handle"]) != h
                    or live["isPublished"] is not True):
                state = ("absent" if live is None else
                         "handle-mismatch" if normalize_handle(
                             live["handle"]) != h else "not-published")
                print(f"REFUSED {h}: {state}")
                refused += 1
                results.append({"handle": h, "status": state})
                breaker += 1
                if breaker >= 5:
                    print("CIRCUIT BREAKER: abort")
                    break
                continue
            if normalize_html(live["body"]) == normalize_html(new_bodies[h]):
                print(f"already current {h}")
                already += 1
                results.append({"handle": h, "status": "already-current"})
                breaker = 0
                continue
            if normalize_html(live["body"]) != normalize_html(old_body):
                print(f"REFUSED {h}: live body matches neither the old "
                      f"manifest nor the new build (out-of-band edit?)")
                refused += 1
                results.append({"handle": h, "status": "out-of-band-edit"})
                breaker += 1
                if breaker >= 5:
                    print("CIRCUIT BREAKER: abort")
                    break
                continue
            out = gql(auth, UPDATE_M,
                      {"id": gid, "body": new_bodies[h]})["pageUpdate"]
            if out["userErrors"]:
                print(f"FAILED {h}: {out['userErrors']}")
                failed += 1
                results.append({"handle": h, "status": "userErrors",
                                "errors": out["userErrors"]})
                breaker += 1
                if breaker >= 5:
                    print("CIRCUIT BREAKER: abort")
                    break
                continue
            re_live = gql(auth, PAGE_Q, {"id": gid})["page"]
            body_ok = (normalize_html(re_live["body"])
                       == normalize_html(new_bodies[h]))
            script_ok = scripts_of(re_live["body"]) == scripts_of(
                new_bodies[h])
            pub_ok = re_live["isPublished"] is True
            if body_ok and script_ok and pub_ok:
                print(f"UPDATED {gid} /pages/{h} (verified: body match, "
                      f"scripts intact, still published)")
                ok += 1
                results.append({"handle": h, "status": "updated",
                                "id": gid, "updated_at": stamp})
                breaker = 0
            else:
                print(f"FAILED {h}: post-update verify body={body_ok} "
                      f"scripts={script_ok} published={pub_ok}")
                failed += 1
                results.append({"handle": h, "status": "verify-failed",
                                "body_ok": body_ok, "script_ok": script_ok,
                                "published": pub_ok})
                breaker += 1
        except Exception as exc:
            print(f"FAILED {h}: {type(exc).__name__}: {exc}")
            failed += 1
            results.append({"handle": h, "status": "exception",
                            "error": f"{type(exc).__name__}: {exc}"})
            breaker += 1
            if breaker >= 5:
                print("CIRCUIT BREAKER: abort")
                break

    outlog = HERE / "manifests" / (
        "update-batch-"
        + datetime.datetime.now(datetime.timezone.utc)
          .strftime("%Y%m%dT%H%M%SZ") + ".json")
    outlog.write_text(json.dumps(
        {"gate_report": str(gate_report), "results": results},
        indent=1) + "\n")
    print(f"summary: updated={ok} already={already} failed={failed} "
          f"refused={refused} of {len(pages)}")
    print(f"run log: {outlog}")
    raise SystemExit(0 if (failed == 0 and refused == 0
                           and ok + already == len(pages)) else 1)


if __name__ == "__main__":
    main()
