#!/usr/bin/env python3
"""seo_meta_restore: write the SEO metafields saved by seo_meta_snapshot.py
back to the live pages (rollback for update_seo_meta.py). DRY RUN by default
(reads live, prints what would change); --yes writes via metafieldsSet and
re-reads to verify. Keys that were unset in the snapshot are reported and
skipped (metafieldsSet cannot restore "unset"; delete by hand if needed).
Same rails as update_seo_meta.py: PROTECTED handles/ids refused, only pages
whose handle matches and that are published are touched. Needs no gate
report because it only restores a state that was already live; announce it
to Rodney first anyway.

Usage:
  python3 seo_meta_restore.py --snapshot manifests/seo-meta-live-2026-10-pre-refresh.json [--handle H ...] [--yes]
"""
import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from kill_switch import (Auth, gql, load_env, normalize_handle,  # noqa: E402
                         PROTECTED_HANDLES, PROTECTED_IDS)
from update_seo_meta import META_Q, META_M, KEYS, live_meta  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--snapshot", required=True)
    ap.add_argument("--handle", action="append", help="limit to these handles")
    ap.add_argument("--yes", action="store_true")
    a = ap.parse_args()
    snap = json.loads(Path(a.snapshot).read_text())["pages"]
    if a.handle:
        snap = {h: snap[h] for h in a.handle}
    for h, e in snap.items():
        if h in PROTECTED_HANDLES or e["page_id"] in PROTECTED_IDS:
            raise SystemExit(f"{h}: PROTECTED; refusing the whole run")
    auth = Auth(load_env(HERE / ".env"))
    todo, same, skipped = [], 0, 0
    for h, e in snap.items():
        page, cur = live_meta(auth, e["page_id"])
        if page is None or normalize_handle(page["handle"]) != h or not page["isPublished"]:
            print(f"REFUSED {h}: absent / handle mismatch / not published")
            skipped += 1
            continue
        diff = {k: e[k] for k in KEYS if e[k] is not None and cur.get(k) != e[k]}
        if any(e[k] is None for k in KEYS):
            print(f"NOTE {h}: key unset in snapshot, not restorable by write")
        if not diff:
            same += 1
            continue
        todo.append((h, e["page_id"], diff))
        for k, v in diff.items():
            print(f"{h} {k}:\n   live:    {cur.get(k)}\n   restore: {v}")
    print(f"\n{len(todo)} page(s) to restore, {same} already match, {skipped} refused")
    if not a.yes:
        print("DRY RUN: nothing written. Add --yes to apply.")
        return
    bad = 0
    for h, gid, diff in todo:
        inputs = [{"ownerId": gid, "namespace": "global", "key": k,
                   "type": "single_line_text_field", "value": v} for k, v in diff.items()]
        out = gql(auth, META_M, {"metafields": inputs})["metafieldsSet"]
        _, after = live_meta(auth, gid)
        ok = not out["userErrors"] and all(after.get(k) == v for k, v in diff.items())
        print(("RESTORED " if ok else "FAILED ") + h)
        bad += 0 if ok else 1
    raise SystemExit(1 if bad else 0)


if __name__ == "__main__":
    main()
