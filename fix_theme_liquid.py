"""fix_theme_liquid: replace the LGD script block inside the live theme.liquid
via the Admin GraphQL API (themeFilesUpsert), instead of a manual paste.

Needs the custom app to hold `read_themes` + `write_themes`. Dry-run by
default: fetches layout/theme.liquid from the MAIN theme, locates the block
`{% if page.handle == 'lab-grown-diamond-price-index' %} ... <script> ... </script> ... {% endif %}`
(the bottom-of-body script block, NOT the <head> style block, which starts
with the same tag), splices in page-assets/theme-liquid-lgd-body.liquid,
checks Liquid tag balance (if/endif, capture/endcapture), prints a diff
summary, and only writes with --yes. Re-fetches after writing to verify.

Usage: python3 fix_theme_liquid.py [--yes] [--source page-assets/theme-liquid-lgd-body.liquid]
"""
import argparse, re, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from kill_switch import Auth, gql, load_env  # noqa: E402

THEMES_Q = '{ themes(first: 20) { nodes { id name role } } }'
FILE_Q = '''query($id: ID!){ theme(id:$id){ files(filenames:["layout/theme.liquid"], first:1){
  nodes{ filename size checksumMd5 body{ ... on OnlineStoreThemeFileBodyText { content } } } } } }'''
UPSERT_M = '''mutation($themeId: ID!, $files: [OnlineStoreThemeFilesUpsertFileInput!]!) {
  themeFilesUpsert(themeId: $themeId, files: $files) {
    upsertedThemeFiles { filename } userErrors { field message code } } }'''
START = "{% if page.handle == 'lab-grown-diamond-price-index' %}"


def balance(text):
    return {t: (len(re.findall(r"{%-?\s*" + t + r"\b", text)),
                len(re.findall(r"{%-?\s*end" + t + r"\b", text)))
            for t in ("if", "capture", "for", "unless", "case", "comment", "raw", "javascript", "style", "form", "paginate", "schema")}


def find_script_block(content):
    """Return (start, end) of the LGD bottom-of-body block: the START tag whose
    block contains '<script>' + 'var DATA_URL', ending at the first
    '{% endif %}' after its '</script>'."""
    for m in re.finditer(re.escape(START), content):
        s = m.start()
        nxt = content.find("</script>", s)
        if nxt == -1:
            continue
        seg = content[s:nxt]
        if "<script>" in seg and "var DATA_URL" in seg and "<style>" not in seg[:200]:
            e = content.find("{% endif %}", nxt)
            if e == -1:
                raise SystemExit("no {% endif %} after the LGD </script>")
            return s, e + len("{% endif %}")
    raise SystemExit("LGD script block not found in theme.liquid")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default=str(HERE / "page-assets" / "theme-liquid-lgd-body.liquid"))
    ap.add_argument("--yes", action="store_true")
    a = ap.parse_args()
    new_block = Path(a.source).read_text(encoding="utf-8").strip("\n")
    assert new_block.startswith(START) and new_block.rstrip().endswith("{% endif %}"), "source must be the if...endif block"

    auth = Auth(load_env(HERE / ".env"))
    themes = gql(auth, THEMES_Q, {})["themes"]["nodes"]
    main_theme = next(t for t in themes if t["role"] == "MAIN")
    print("MAIN theme:", main_theme["name"], main_theme["id"])
    f = gql(auth, FILE_Q, {"id": main_theme["id"]})["theme"]["files"]["nodes"][0]
    content = f["body"]["content"]
    print(f"live layout/theme.liquid: {f['size']} bytes, md5 {f['checksumMd5']}")
    print("live tag balance:", {k: v for k, v in balance(content).items() if v != (0, 0)})
    s, e = find_script_block(content)
    old_block = content[s:e]
    print(f"LGD script block at chars {s}..{e} ({len(old_block)} chars); live DATA_URL:",
          re.search(r"index-data\.json\?v=\d+", old_block).group(0))
    new_content = content[:s] + new_block + content[e:]
    print("new tag balance:", {k: v for k, v in balance(new_content).items() if v != (0, 0)})
    bad = [k for k, (o, c) in balance(new_content).items() if o != c]
    if bad:
        raise SystemExit(f"tag imbalance in the composed file: {bad}; refusing")
    print(f"composed file: {len(new_content)} chars; new DATA_URL:",
          re.search(r"index-data\.json\?v=\d+", new_block).group(0))
    Path("/tmp").mkdir(exist_ok=True)
    (HERE / "manifests" / "theme-liquid-before-fix.liquid").write_text(content, encoding="utf-8")
    print("backup of the live file written to manifests/theme-liquid-before-fix.liquid")
    if not a.yes:
        print("DRY RUN: nothing written. Add --yes.")
        return
    out = gql(auth, UPSERT_M, {"themeId": main_theme["id"], "files": [
        {"filename": "layout/theme.liquid", "body": {"type": "TEXT", "value": new_content}}]})["themeFilesUpsert"]
    if out["userErrors"]:
        raise SystemExit(f"upsert userErrors: {out['userErrors']}")
    f2 = gql(auth, FILE_Q, {"id": main_theme["id"]})["theme"]["files"]["nodes"][0]
    ok = f2["body"]["content"] == new_content
    print("re-fetched after write: match =", ok, "| size", f2["size"], "| md5", f2["checksumMd5"])
    if not ok:
        raise SystemExit("re-fetch does not match what was written")


if __name__ == "__main__":
    main()
