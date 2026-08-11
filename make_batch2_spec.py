#!/usr/bin/env python3
"""make_batch2_spec: carve the batch-2 spec/meta/QA manifest out of the
full page_output build.

Batch-2 = every STONE page in the current build that is not already live
in batch-1 (the 28-page canary). Aggregates are all live in batch-1, so
this is 1,018 - 28 = 990 pages by construction; the script hard-asserts
that count and refuses to emit anything if the build or the batch-1
manifest do not reconcile.

Inputs (read-only):
  page_output/qa-manifest.json           full-build QA records (1,018)
  page_output/keyword-log.csv            SEO title / H1 / meta per handle
  manifests/batch-1-canary-rollback-manifest.json   the 28 live handles

Outputs (page_output/):
  batch-2-rollback-spec.json    rollback_manifest.py snapshot spec
  batch-2-meta.json             SEO metafields for canary_create_drafts.py
  qa-manifest-batch-2.json      publish_gate.py input for this batch

Never touches the Shopify API. Deterministic: same inputs -> same bytes.
"""

import csv
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "page_output"
EXPECTED_BATCH2 = 990

qa = json.loads((OUT / "qa-manifest.json").read_text())["pages"]
batch1 = json.loads((HERE / "manifests" /
                     "batch-1-canary-rollback-manifest.json").read_text())
live_handles = {e["handle"] for e in batch1["pages"]}

kw_rows = {}
with open(OUT / "keyword-log.csv", newline="") as fh:
    for row in csv.DictReader(fh):
        kw_rows[row["handle"]] = row

problems = []
if len(live_handles) != 28:
    problems.append(f"batch-1 manifest has {len(live_handles)} handles, "
                    f"expected 28")
missing_kw = [r["handle"] for r in qa if r["handle"] not in kw_rows]
if missing_kw:
    problems.append(f"{len(missing_kw)} qa handles missing from keyword "
                    f"log, e.g. {missing_kw[:3]}")
not_built = [e for e in live_handles
             if not (OUT / "pages" / f"{e}.html").exists()]
if not_built:
    problems.append(f"live handles missing from build: {not_built[:3]}")

batch2 = [r for r in qa
          if r["page_type"] == "stone" and r["handle"] not in live_handles]
leftover_agg = [r["handle"] for r in qa
                if r["page_type"] != "stone"
                and r["handle"] not in live_handles]
if leftover_agg:
    problems.append(f"non-stone pages NOT live in batch-1 (unexpected, "
                    f"they would be silently dropped): {leftover_agg}")
if len(batch2) != EXPECTED_BATCH2:
    problems.append(f"batch-2 selects {len(batch2)} pages, expected "
                    f"{EXPECTED_BATCH2}")
missing_body = [r["handle"] for r in batch2
                if not (OUT / "pages" / f"{r['handle']}.html").exists()]
if missing_body:
    problems.append(f"batch-2 pages missing body files: {missing_body[:3]}")

if problems:
    for p in problems:
        print(f"REFUSING to emit: {p}")
    sys.exit(2)

spec = {"pages": [{
    "handle": r["handle"],
    # Shopify page.title = the SHORT scheme H1 (theme injects page.title
    # as served H1); the long SEO title serves via global.title_tag.
    "title": kw_rows[r["handle"]]["page_title_h1"],
    "body_file": f"pages/{r['handle']}.html",
    "source": {"cycle": r["cycle"], "data_timestamp": r["data_timestamp"],
               "source_files": r["source_files"],
               "sample_size": r["sample_size"],
               "retailer_set": r["retailer_set"]},
    "target_keyword": r["target_keyword"],
    "note": "batch-2; stone page; meta in batch-2-meta.json",
} for r in batch2]}
meta = {r["handle"]: {
    "title_tag": kw_rows[r["handle"]]["seo_title_tag"],
    "description_tag": kw_rows[r["handle"]]["meta_description"],
} for r in batch2}

(OUT / "batch-2-rollback-spec.json").write_text(
    json.dumps(spec, indent=1) + "\n")
(OUT / "batch-2-meta.json").write_text(json.dumps(meta, indent=1) + "\n")
(OUT / "qa-manifest-batch-2.json").write_text(
    json.dumps({"pages": batch2}, indent=1) + "\n")

print(f"batch-2: {len(batch2)} stone pages "
      f"(full build {len(qa)}, live batch-1 {len(live_handles)})")
print("wrote batch-2-rollback-spec.json, batch-2-meta.json, "
      "qa-manifest-batch-2.json")
