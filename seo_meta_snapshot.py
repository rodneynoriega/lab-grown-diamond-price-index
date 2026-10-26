#!/usr/bin/env python3
"""seo_meta_snapshot: READ-ONLY dump of the live global.title_tag and
global.description_tag of every page in the given rollback manifest(s), as
the pre-image update_seo_meta.py does not save. Companion: seo_meta_restore.py.

Usage:
  python3 seo_meta_snapshot.py --manifest A.json --manifest B.json \
      --out manifests/seo-meta-live-2026-10-pre-refresh.json
Only GraphQL reads are issued (page(id) { metafields }). Re-run it right
before the real update_seo_meta --yes so the pre-image is as current as the
body snapshots.
"""
import argparse
import datetime
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from kill_switch import Auth, gql, load_env, normalize_handle  # noqa: E402
from update_seo_meta import META_Q, KEYS  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--manifest", action="append", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    auth = Auth(load_env(HERE / ".env"))
    pages, missing = {}, []
    for mp in a.manifest:
        for e in json.loads(Path(mp).read_text())["pages"]:
            h = normalize_handle(e["handle"])
            p = gql(auth, META_Q, {"id": e["page_id"]})["page"]
            if p is None or normalize_handle(p["handle"]) != h:
                missing.append(h)
                continue
            cur = {n["key"]: {"id": n["id"], "value": n["value"]}
                   for n in p["metafields"]["nodes"]}
            pages[h] = {"page_id": e["page_id"],
                        **{k: (cur[k]["value"] if k in cur else None) for k in KEYS}}
    out = {"captured_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
           "manifests": a.manifest, "namespace": "global", "pages": pages,
           "missing": missing}
    Path(a.out).write_text(json.dumps(out, indent=1, ensure_ascii=False) + "\n")
    print(f"wrote {a.out}: {len(pages)} pages, {len(missing)} missing; "
          f"{sum(1 for p in pages.values() if p['title_tag'] is None or p['description_tag'] is None)} with an unset key")


if __name__ == "__main__":
    main()
