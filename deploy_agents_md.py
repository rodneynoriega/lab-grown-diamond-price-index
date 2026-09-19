#!/usr/bin/env python3
"""deploy_agents_md.py: create or update templates/agents.md.liquid in the
MAIN theme via themeFilesUpsert (same app scopes as fix_theme_liquid.py:
read_themes + write_themes). Dry run by default: reports whether the file
already exists in the live theme, shows size and a preview, writes nothing.
With --yes: backs up any existing live copy to manifests/, upserts, re-reads
to verify byte equality, then prints the storefront check command.

Rollback: --delete removes the template (Shopify then serves its generated
default again), or --source <backup file> --yes restores a previous copy.

Source default: OUTBOX/2026-09-03-agents-md-liquid-draft.liquid.
"""
import argparse, datetime, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from kill_switch import Auth, gql, load_env  # noqa: E402

FILENAME = "templates/agents.md.liquid"
THEMES_Q = '{ themes(first: 20) { nodes { id name role } } }'
FILE_Q = '''query($id: ID!, $fn: [String!]!){ theme(id:$id){ files(filenames:$fn, first:1){
  nodes{ filename size checksumMd5 body{ ... on OnlineStoreThemeFileBodyText { content } } } } } }'''
UPSERT_M = '''mutation($themeId: ID!, $files: [OnlineStoreThemeFilesUpsertFileInput!]!) {
  themeFilesUpsert(themeId: $themeId, files: $files) {
    upsertedThemeFiles { filename } userErrors { field message code } } }'''
DELETE_M = '''mutation($themeId: ID!, $files: [String!]!) {
  themeFilesDelete(themeId: $themeId, files: $files) {
    deletedThemeFiles { filename } userErrors { field message code } } }'''


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default=str(HERE.parent.parent / "OUTBOX" / "2026-09-03-agents-md-liquid-draft.liquid"))
    ap.add_argument("--yes", action="store_true")
    ap.add_argument("--delete", action="store_true", help="remove the template (rollback to Shopify default)")
    a = ap.parse_args()
    auth = Auth(load_env(HERE / ".env"))
    themes = gql(auth, THEMES_Q, {})["themes"]["nodes"]
    main_theme = next(t for t in themes if t["role"] == "MAIN")
    print("MAIN theme:", main_theme["name"], main_theme["id"])
    nodes = gql(auth, FILE_Q, {"id": main_theme["id"], "fn": [FILENAME]})["theme"]["files"]["nodes"]
    live = nodes[0] if nodes else None
    print(f"live {FILENAME}: " + (f"EXISTS, {live['size']} bytes, md5 {live['checksumMd5']}" if live else "absent (Shopify default is being served)"))

    if a.delete:
        if not live:
            raise SystemExit("nothing to delete")
        if not a.yes:
            print("DRY RUN: would delete the template. Add --yes.")
            return
        out = gql(auth, DELETE_M, {"themeId": main_theme["id"], "files": [FILENAME]})["themeFilesDelete"]
        if out["userErrors"]:
            raise SystemExit(f"delete userErrors: {out['userErrors']}")
        print("deleted:", out["deletedThemeFiles"])
        return

    body = Path(a.source).read_text(encoding="utf-8")
    if "{{ agents.store_url }}" not in body or "Research and data" not in body:
        raise SystemExit("source does not look like the agents.md draft (missing agents.store_url or the research section)")
    if any(ord(c) > 0x2FFF for c in body):
        raise SystemExit("unexpected non-text characters in source")
    print(f"source: {a.source} ({len(body)} chars); first lines:")
    for line in body.splitlines()[:3]:
        print("   ", line[:100])
    if live and live["body"]["content"] == body:
        print("live copy already identical; nothing to do")
        return
    if not a.yes:
        print("DRY RUN: nothing written. Add --yes.")
        return
    if live:
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        bk = HERE / "manifests" / f"agents-md-before-{stamp}-md5-{(live['checksumMd5'] or 'nomd5')[:8]}.liquid"
        bk.write_text(live["body"]["content"], encoding="utf-8")
        print("backup of the live template:", bk.name)
    out = gql(auth, UPSERT_M, {"themeId": main_theme["id"], "files": [
        {"filename": FILENAME, "body": {"type": "TEXT", "value": body}}]})["themeFilesUpsert"]
    if out["userErrors"]:
        raise SystemExit(f"upsert userErrors: {out['userErrors']}")
    nodes = gql(auth, FILE_Q, {"id": main_theme["id"], "fn": [FILENAME]})["theme"]["files"]["nodes"]
    ok = bool(nodes) and nodes[0]["body"]["content"] == body
    print("re-fetched after write: match =", ok)
    if not ok:
        raise SystemExit("re-fetch does not match what was written")
    print("verify (cache is broad, allow time): curl -s https://rings.com/agents.md | head -20 ; curl -s https://rings.com/llms.txt | head -5")


if __name__ == "__main__":
    main()
