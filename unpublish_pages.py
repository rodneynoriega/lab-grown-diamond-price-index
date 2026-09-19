#!/usr/bin/env python3
"""unpublish_pages.py: reversible unpublish (or re-publish) of a named list of
Shopify pages via the Admin GraphQL API. Built 2026-09-03 for the 15 legacy
promo pages that still carry the "newly launched brand ... no reviews yet"
FAQ answer (Rodney: "remove all language that says no reviews yet").

Dry run by default: looks every handle up, refuses PROTECTED_IDS and any
handle outside the list, prints what would change, writes nothing. With
--yes: snapshots each page (id, handle, title, isPublished, body) to
manifests/unpublish-<label>-<UTC>.json BEFORE the first write, then sets
isPublished=false one page at a time and re-queries each to verify.
--restore <snapshot.json> re-publishes exactly the pages in that snapshot
(same verification), which is the rollback.

Never deletes. Never touches body. Same auth/retry client as kill_switch.
"""
import argparse, datetime, json, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from kill_switch import Auth, gql, load_env, PROTECTED_IDS  # noqa: E402

LOOKUP_Q = '''query($q:String!){ pages(first:5, query:$q){ nodes{ id handle title isPublished updatedAt body } } }'''
UPDATE_M = '''mutation($id: ID!, $page: PageUpdateInput!){ pageUpdate(id:$id, page:$page){ page{ id handle isPublished } userErrors{ field message code } } }'''
VERIFY_Q = '''query($id: ID!){ page(id:$id){ id handle isPublished } }'''

LEGACY_PROMO_2026_09_03 = [
    "boxing-week", "usa-boxing-week-2024", "toronto-new-year-2025", "la-new-year-2025",
    "usa-miami-new-years-special-2025", "vancouver-valentines-day-2025",
    "toronto-valentines-day-2025", "calgary-valentines-day-2025", "oakville-valentines-day-2025",
    "richmond-valentines-day-2025", "usa-valentines-day-2025", "canada-valentines-day-2025",
    "canada-self-love-diamond-deal", "usa-self-love-diamond-deal", "richmond-self-love-diamond-deal",
]


def lookup(auth, handle):
    nodes = gql(auth, LOOKUP_Q, {"q": f"handle:{handle}"})["pages"]["nodes"]
    hits = [n for n in nodes if n["handle"] == handle]
    if len(hits) != 1:
        raise SystemExit(f"{handle}: expected exactly 1 page, found {len(hits)}")
    return hits[0]


def set_published(auth, gid, value):
    out = gql(auth, UPDATE_M, {"id": gid, "page": {"isPublished": value}})["pageUpdate"]
    if out["userErrors"]:
        raise SystemExit(f"{gid}: userErrors {out['userErrors']}")
    chk = gql(auth, VERIFY_Q, {"id": gid})["page"]
    if chk["isPublished"] != value:
        raise SystemExit(f"{gid}: re-query shows isPublished={chk['isPublished']}, expected {value}")
    return chk


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--handles", nargs="*", default=LEGACY_PROMO_2026_09_03)
    ap.add_argument("--label", default="legacy-promo")
    ap.add_argument("--yes", action="store_true", help="actually unpublish")
    ap.add_argument("--restore", help="snapshot json from a previous --yes run: re-publish those pages")
    a = ap.parse_args()
    auth = Auth(load_env(HERE / ".env"))

    if a.restore:
        snap = json.loads(Path(a.restore).read_text())
        print(f"RESTORE from {a.restore}: {len(snap['pages'])} pages")
        for p in snap["pages"]:
            if not a.yes:
                print(f"  would re-publish {p['handle']} ({p['id']})")
                continue
            chk = set_published(auth, p["id"], True)
            print(f"  re-published {chk['handle']} -> isPublished={chk['isPublished']}")
        if not a.yes:
            print("DRY RUN: nothing written. Add --yes.")
        return

    pages = []
    for h in a.handles:
        p = lookup(auth, h)
        if p["id"] in PROTECTED_IDS:
            raise SystemExit(f"REFUSED: {h} is a protected page ({p['id']})")
        pages.append(p)
        print(f"  {p['id']}  {h:40s} published={p['isPublished']}  updated={p['updatedAt'][:10]}  body={len(p.get('body') or '')} chars")
    todo = [p for p in pages if p["isPublished"]]
    print(f"{len(pages)} pages looked up; {len(todo)} currently published would be set to isPublished=false")
    if not a.yes:
        print("DRY RUN: nothing written (no snapshot either). Add --yes.")
        return
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snap_path = HERE / "manifests" / f"unpublish-{a.label}-{stamp}.json"
    snap_path.write_text(json.dumps({"taken": stamp, "pages": pages}, indent=1))
    print(f"snapshot written: manifests/{snap_path.name} (restore with --restore {snap_path.name} --yes)")
    for p in todo:
        chk = set_published(auth, p["id"], False)
        print(f"  unpublished {chk['handle']} -> isPublished={chk['isPublished']}")
    print("done; storefront should now 404 these handles (verify with a cold mobile-UA curl)")


if __name__ == "__main__":
    main()
