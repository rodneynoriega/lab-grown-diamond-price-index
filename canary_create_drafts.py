#!/usr/bin/env python3
"""canary_create_drafts: create the batch-1 canary pages as Shopify DRAFTS.

Creates ONLY the pages named in the batch's rollback spec (snapshotted
FIRST via rollback_manifest.py, per its workflow), as drafts, via the
fleet's shared batching/backoff client (Shopify Page Publisher /
PROJECTS/shopify-batch-publish/shopify_batch.py, imported, not
reimplemented, per its README).

HARD SAFETY PROPERTIES:
  - isPublished is set False in every payload AND verified False in every
    mutation response; a single isPublished=true response ABORTS the run.
  - Publishing is not implemented anywhere in this file.
  - Protected handles (kill_switch.PROTECTED_HANDLES) are refused.
  - A handle that already exists live is refused (Shopify would silently
    create a suffixed duplicate), counted as an error.
  - Dry-run by default; --yes required to mutate.
  - SEO metafields (global.title_tag / global.description_tag) are set at
    create time from the canary-meta.json emitted by page_pipeline.py, so
    the approved titles/metas serve without any GemPages/theme editing.
    Pilot pages are NEVER opened in the GemPages editor (standing rule).

Usage:
  python3 canary_create_drafts.py --spec page_output/canary-rollback-spec.json \
      --meta page_output/canary-meta.json [--yes]

Exit codes: 0 all created as drafts (or clean dry run); 1 any refusal,
error, or verification failure; 2 bad arguments/spec.
"""

import argparse
import datetime
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, "/Users/rodneynoriega/My Agents/Shopify Page Publisher"
                   "/PROJECTS/shopify-batch-publish")

import shopify_batch  # noqa: E402  (fleet-shared batching/backoff client)
from kill_switch import PROTECTED_HANDLES, normalize_handle  # noqa: E402

STORE = "09c241-13.myshopify.com"
TEMPLATE_SUFFIX = "lgd-research"

PAGE_CREATE_WITH_META = """
mutation PageCreate($page: PageCreateInput!) {
  pageCreate(page: $page) {
    page { id title handle isPublished templateSuffix }
    userErrors { field message code }
  }
}
"""


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--spec", required=True,
                    help="rollback spec JSON (canary-rollback-spec.json)")
    ap.add_argument("--meta", required=True,
                    help="canary-meta.json (SEO title/description per handle)")
    ap.add_argument("--yes", action="store_true",
                    help="actually create the drafts (dry-run without)")
    args = ap.parse_args()

    try:
        spec = json.loads(Path(args.spec).read_text())["pages"]
        meta = json.loads(Path(args.meta).read_text())
    except (OSError, json.JSONDecodeError, KeyError) as e:
        print(f"cannot load spec/meta: {e}")
        raise SystemExit(2)
    spec_dir = Path(args.spec).resolve().parent

    entries = []
    problems = []
    seen = set()
    for i, e in enumerate(spec):
        h = normalize_handle(e["handle"])
        if h in PROTECTED_HANDLES:
            problems.append(f"{h}: PROTECTED handle; refusing the whole run")
            continue
        if h in seen:
            problems.append(f"{h}: duplicate handle in spec")
            continue
        seen.add(h)
        body_file = spec_dir / e["body_file"]
        if not body_file.exists():
            problems.append(f"{h}: body file missing ({body_file})")
            continue
        entries.append((h, e["title"], body_file, meta.get(h) or {}))
    if problems:
        for p in problems:
            print(f"spec INVALID: {p}")
        raise SystemExit(2)

    if not args.yes:
        print(f"DRY RUN: would create {len(entries)} DRAFT pages "
              f"(isPublished false, SEO metafields set). Add --yes.")
        for h, title, _, m in entries:
            print(f"  /pages/{h}  '{title}'"
                  f"{'  [meta ok]' if m else '  [NO META]'}")
        raise SystemExit(0)

    creds = shopify_batch.load_credentials(HERE / ".env")
    token = shopify_batch.mint_token(STORE, creds["SHOPIFY_CLIENT_ID"],
                                     creds["SHOPIFY_CLIENT_SECRET"])
    client = shopify_batch.ShopifyBatchClient(STORE, token)

    ok, failed = 0, 0
    results = []
    for h, title, body_file, m in entries:
        # refuse existing handles (prevents silent -1 suffixing)
        nodes = client.execute(client.PAGES_BY_QUERY,
                               {"q": f"handle:{h}", "first": 2},
                               op_name="pagesQuery")["pages"]["nodes"]
        if any(normalize_handle(n["handle"]) == h for n in nodes):
            print(f"REFUSED {h}: handle already exists live")
            failed += 1
            results.append({"handle": h, "status": "refused-exists"})
            continue
        page_input = {
            "title": title,
            "handle": h,
            "body": body_file.read_text(),
            "isPublished": False,      # hard rule; never publish here
            # Rodney's page template (created by him in the theme editor
            # 2026-08-10); its title block is hidden by in-body CSS, see
            # templates/_shared.html.j2 header macro.
            "templateSuffix": TEMPLATE_SUFFIX,
            "metafields": [
                {"namespace": "global", "key": "title_tag",
                 "type": "single_line_text_field",
                 "value": m.get("title_tag", title)},
                {"namespace": "global", "key": "description_tag",
                 "type": "single_line_text_field",
                 "value": m.get("description_tag", "")},
            ],
        }
        try:
            out = client.execute(PAGE_CREATE_WITH_META,
                                 {"page": page_input}, op_name="pageCreate")
            pc = out["pageCreate"]
            if pc["userErrors"]:
                print(f"FAILED {h}: {pc['userErrors']}")
                failed += 1
                results.append({"handle": h, "status": "userErrors",
                                "errors": pc["userErrors"]})
                continue
            page = pc["page"]
            if page["isPublished"] is not False:
                print(f"ABORT: {h} came back isPublished="
                      f"{page['isPublished']}; this run never publishes.")
                results.append({"handle": h, "status": "PUBLISHED?!",
                                "id": page["id"]})
                _write_log(results, client)
                raise SystemExit(1)
            if page.get("templateSuffix") != TEMPLATE_SUFFIX:
                print(f"FAILED {h}: templateSuffix came back "
                      f"{page.get('templateSuffix')!r}, expected "
                      f"{TEMPLATE_SUFFIX!r} (template missing on theme?)")
                failed += 1
                results.append({"handle": h, "status": "wrong-template",
                                "id": page["id"],
                                "templateSuffix": page.get("templateSuffix")})
                continue
            print(f"created DRAFT {page['id']} /pages/{page['handle']} "
                  f"published={page['isPublished']} "
                  f"template={page['templateSuffix']}")
            ok += 1
            results.append({"handle": h, "status": "draft-created",
                            "id": page["id"],
                            "isPublished": page["isPublished"],
                            "templateSuffix": page["templateSuffix"]})
        except SystemExit:
            raise
        except Exception as e:  # keep batch containment, report at end
            print(f"FAILED {h}: {type(e).__name__}: {e}")
            failed += 1
            results.append({"handle": h, "status": "exception",
                            "error": f"{type(e).__name__}: {e}"})

    _write_log(results, client)
    print(f"summary: ok={ok} failed={failed} total={len(entries)}")
    raise SystemExit(1 if failed else 0)


def _write_log(results, client):
    logdir = HERE / "manifests"
    logdir.mkdir(exist_ok=True)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ")
    (logdir / f"canary-create-{stamp}.json").write_text(json.dumps(
        {"results": results, "throttle_log": client.log}, indent=1) + "\n")


if __name__ == "__main__":
    main()
