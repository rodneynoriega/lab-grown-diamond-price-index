#!/usr/bin/env python3
"""theme_script_guard: the JS string rules for the LGD theme.liquid script
block, as ONE shared check that every path to the live theme runs.

Rules (MEMORY/feedback_gempages_shopify.md):
  flex       GROUND TRUTH. The Flex theme (PXU) runs the page through
             `split: ' <'` -> rejoin with a newline and ` <` prepended ->
             `replace: ' </', '</'`. The guard applies that transform to
             the script and runs `node --check` on the RESULT. Anything
             the split breaks (a ` <` inside a string literal, a `//`
             comment, a regex literal, whatever the lexer says) fails
             here, regardless of how it is written.
  js-string  a space before '<' inside a per-line JS string literal.
             Quoted literals: when flex FAILS the hits are listed as line
             pointers; when flex PASSES they are warnings (the per-line
             scan mis-pairs quotes inside block comments). Backtick
             template literals: ALWAYS fatal, because the injected
             newline is legal inside a template literal, so flex cannot
             see the corrupted text.
  liquid     no `{{` or `{%` inside the script body: Liquid would
             substitute text into the very source the split runs on, so
             the flex result would not be ground truth.
  ascii      the script is ASCII only (paste flows have mangled non-ASCII).
  html       no '</script' inside the script body (ends the <script>
             element early, whatever the JS around it says).
  syntax     `node --check` parses the script as written.
  shape      the block is `{% if page.handle == '<handle>' %} <script>
             ... </script> {% endif %}` with exactly one <script> element
             and `var DATA_URL`.

Callers:
  page-assets/sync_theme_liquid.py   at write time (generated regions)
  fix_theme_liquid.py                 at publish time (refuses a failing
                                      block, hand-edited or not)
  publish_gate.py --theme-liquid      at gate time (check F, recorded in
                                      the report)

Known limit: a MULTI-line template literal containing ` <` parses after
the transform (the newline lands inside the literal), so its rendered
text changes without a syntax error, and the per-line scan cannot see
it. Nothing in the block uses backticks; single-line ones are fatal.

Per SKILLS/publish-gate/SKILL.md check C: after ANY change to this file,
re-run the negative controls (`python3 theme_script_guard.py --self-test`)
before the gate's verdict is trusted again.

Usage: python3 theme_script_guard.py PATH [PATH ...]   (exit 1 on any FAIL)
       python3 theme_script_guard.py --self-test
"""
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

START = "{% if page.handle == 'lab-grown-diamond-price-index' %}"
START_RE = re.compile(r"\{%-?\s*if page\.handle == '[a-z0-9-]+'\s*-?%\}")
END = "{% endif %}"
SCRIPT_OPEN = re.compile(r"<script(?:\s[^>]*)?>", re.I)
SCRIPT_RE = re.compile(r"<script(?:\s[^>]*)?>\n?(.*?)\n?</script>(?!.*</script)", re.S | re.I)
# Per-line string literals: double-, single-quoted, and single-line backtick.
STR_LIT = re.compile(r'"(?:[^"\\]|\\.)*"|\'(?:[^\'\\]|\\.)*\'|`(?:[^`\\]|\\.)*`')
RULES = ("flex", "js-string", "liquid", "ascii", "html", "syntax", "shape")
END_RE = re.compile(r"\{%-?\s*endif\s*-?%\}\s*$")


def flex_transform(script):
    """What the Flex theme does to the page source: split on ' <', rejoin
    with a newline and ' <' prepended to each later chunk, then drop the
    space in ' </'."""
    chunks = script.split(" <")
    out = chunks[0] + "".join("\n <" + c for c in chunks[1:])
    return out.replace(" </", "</")


def extract_script(text):
    """The script body between the ONE <script ...> element's tags, or
    None. Greedy to the LAST </script> so nothing after an inner
    '</script' escapes the checks (the html rule then reports it)."""
    m = SCRIPT_RE.search(text)
    return m.group(1) if m else None


def node_check(script, label):
    """Run `node --check` on the script text; return None on success or a
    short failure string (temp path scrubbed, node internals dropped)."""
    if shutil.which("node") is None:
        return (f"{label}: node not found on PATH; cannot run `node --check` "
                "(install node; there is no non-node fallback for a gating run)")
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False,
                                     encoding="utf-8") as tmp:
        tmp.write(script)
        path = tmp.name
    try:
        r = subprocess.run(["node", "--check", path],
                           capture_output=True, text=True, timeout=60)
    except subprocess.TimeoutExpired:
        return f"{label}: node --check timed out after 60s"
    finally:
        Path(path).unlink(missing_ok=True)
    if r.returncode == 0:
        return None
    real = str(Path(path).resolve())
    lines = [ln.rstrip() for ln in (r.stderr or r.stdout).splitlines()]
    lines = [ln.replace(real, "script").replace(path, "script")
             for ln in lines if ln.strip() and not ln.lstrip().startswith("at ")]
    lines = [ln for ln in lines if not ln.startswith("Node.js v")]
    return f"{label}: node --check failed: " + " | ".join(lines[:5])


def check_theme_script(text, require_block=True, warnings=None):
    """Return a list of failure strings (empty = PASS). Every rule is
    evaluated; a failing file lists every reason, not just the first.
    Non-fatal notes (js-string hits when flex passes) are appended to
    `warnings` when a list is given."""
    fails = []
    if warnings is None:
        warnings = []
    text = text.replace("\r\n", "\n")
    t = text.strip("\n")
    if require_block:
        if not START_RE.match(t):
            fails.append("shape: block must start with "
                         "{% if page.handle == '<handle>' %}")
        if not END_RE.search(t):
            fails.append(f"shape: block must end with {END}")
    opens = len(SCRIPT_OPEN.findall(text))
    closes = text.lower().count("</script")
    if opens != 1 or closes != 1:
        fails.append(f"shape: expected exactly one <script> element, found "
                     f"{opens} opening and {closes} closing tag(s)")
    script = extract_script(text)
    if script is None:
        fails.append("shape: no <script ...>\\n...\\n</script> block found")
        return fails
    if "var DATA_URL" not in script:
        fails.append("shape: 'var DATA_URL' missing from the script")

    js_hits = []
    for i, ln in enumerate(script.splitlines(), 1):
        for lit in STR_LIT.findall(ln):
            if " <" in lit and lit.startswith("`"):
                fails.append("js-string: space before '<' inside a backtick "
                             "template literal (the Flex split corrupts its "
                             f"text without a syntax error), script line {i}: "
                             f"{ln.strip()[:120]}")
                break
            if " <" in lit:
                js_hits.append("js-string: space before '<' inside what "
                               "scans as a string literal, script line "
                               f"{i}: {ln.strip()[:120]}")
                break

    if "{{" in script or "{%" in script:
        lines = [i for i, ln in enumerate(script.splitlines(), 1)
                 if "{{" in ln or "{%" in ln]
        fails.append(f"liquid: '{{{{' or '{{%' inside the script body, line(s) "
                     f"{lines[:10]}: Liquid would substitute text into the "
                     f"source the split runs on. Keep the script Liquid-free; "
                     f"if this is plain JS (nested braces), write '{{ {{'")

    non_ascii = sorted({c for c in script if ord(c) > 127})
    if non_ascii:
        lines = [i for i, ln in enumerate(script.splitlines(), 1)
                 if any(ord(c) > 127 for c in ln)]
        fails.append(f"ascii: non-ASCII character(s) {non_ascii!r} in the "
                     f"script, line(s) {lines[:10]}")

    if "</script" in script.lower():
        lines = [i for i, ln in enumerate(script.splitlines(), 1)
                 if "</script" in ln.lower()]
        fails.append(f"html: '</script' inside the script body, line(s) "
                     f"{lines[:10]} (ends the <script> element early)")

    err = node_check(script, "syntax")
    if err:
        fails.append(err)
    # Ground truth for the Flex split: parse what the theme will serve.
    err = node_check(flex_transform(script), "flex")
    if err:
        if "node not found" not in err:
            hits = [i for i, ln in enumerate(script.splitlines(), 1) if " <" in ln]
            err += (f" | the script as the Flex theme serves it (newline "
                    f"injected at every ' <'); ' <' occurs on script "
                    f"line(s) {hits[:10]}")
        fails.append(err)
        fails.extend(js_hits)      # no ground truth, or it failed: pointers are fatal
    else:
        warnings.extend(js_hits)
    return fails


def self_test():
    """Negative controls: each deliberately bad block MUST fail on its rule;
    the clean control MUST pass. Returns True when every control behaves."""
    good = (START + "\n<script>\n(function () {\n  var DATA_URL = "
            "\"https://x/index-data.json?v=1\";\n  var s = \"<b>ok</b>\";\n"
            "  var n = 1;\n  if (n < 2) { n = 2; }\n"
            "})();\n</script>\n" + END + "\n")
    controls = [
        ("clean (bare 'n < 2' comparison allowed)", good, None),
        ("space-before-< in string", good.replace('"<b>ok</b>"', '"a <b>ok</b>"'), "flex"),
        ("space-before-< in string (js-string pointer listed)", good.replace('"<b>ok</b>"', '"a <b>ok</b>"'), "js-string"),
        ("block comment with apostrophes and ' <' (safe; js-string demoted to warning)",
         good.replace("  var n = 1;", "  /* Rodney's note: keep the a <b> wrapper, that's deliberate */ var n = 1;"), None),
        ("liquid output tag inside the script", good.replace("  var n = 1;", '  var t = "{{ page.title }}"; var n = 1;'), "liquid"),
        ("whitespace-control {%- if -%} / {%- endif -%} accepted",
         good.replace(START, "{%- if page.handle == 'lab-grown-diamond-price-index' -%}").replace(END, "{%- endif -%}"), None),
        ("code on the <script> line still checked", good.replace("<script>\n(function", "<script>(function"), None),
        ("single-line backtick literal with ' <' (flex passes; must still fail)",
         good.replace("  var n = 1;", "  var row = `<td>a <b>bold</b></td>`; var n = 1;"), "js-string"),
        ("nested-brace JS written '{ {' passes liquid", good.replace("  var n = 1;", "  var n = 1; if (n) { { n = 3; } }"), None),
        ("code on the <script> line, bad string", good.replace("<script>\n(function", "<script>(function").replace('"<b>ok</b>"', '"a <b>ok</b>"'), "flex"),
        ("space-before-< in single quotes", good.replace('"<b>ok</b>"', "'a <b>'"), "flex"),
        ("space-before-< in // comment", good.replace("  var n = 1;", "  var n = 1; // build a <span> here"), "flex"),
        ("space-before-< in regex literal", good.replace("  var n = 1;", "  var re = /a <b/g; var n = 1;"), "flex"),
        ("quote-pairing shift hides the string", good.replace("  var n = 1;", '  var q = "a" /* " */ + " <b>"; var n = 1;'), "flex"),
        ("second <script> with bad code after the first </script>",
         good.replace("</script>\n" + END, "</script>\n<script>\nvar z = ((;\n</script>\n" + END), "shape"),
        ("non-ascii", good.replace("ok", "ok—"), "ascii"),
        ("</script in string", good.replace('"<b>ok</b>"', '"</script>"'), "html"),
        ("syntax error", good.replace("})();", "})(;"), "syntax"),
        ("missing DATA_URL", good.replace("var DATA_URL", "var X"), "shape"),
        ("no endif", good.replace(END, ""), "shape"),
        ("<script type=...> and CRLF still checked", good.replace("<script>", '<script type="text/javascript">').replace("\n", "\r\n"), None),
    ]
    ok = True
    for name, text, rule in controls:
        warn = []
        fails = check_theme_script(text, warnings=warn)
        hit = [f for f in fails if f.startswith(rule + ":")] if rule else []
        good_result = (not fails) if rule is None else bool(hit)
        ok &= good_result
        verdict = "PASS" if not fails else "FAIL"
        want = "must PASS" if rule is None else f"must FAIL on {rule}"
        print(f"  {'ok ' if good_result else 'BAD'} control '{name}': {verdict} ({want})")
        for f in fails:
            print(f"        - {f[:160]}")
        for w in warn:
            print(f"        ~ warning: {w[:160]}")
    print(f"theme_script_guard self-test ({len(controls)} controls):",
          "ALL CONTROLS BEHAVED" if ok
          else "BROKEN: a control did not behave; do not trust the guard")
    return ok


def main(argv):
    if argv and argv[0] == "--self-test":
        raise SystemExit(0 if self_test() else 1)
    if not argv:
        print(__doc__)
        raise SystemExit(2)
    bad = 0
    for p in argv:
        text = Path(p).read_text(encoding="utf-8")
        warn = []
        fails = check_theme_script(text, warnings=warn)
        print(f"{'FAIL' if fails else 'PASS'} theme-script-guard {p}"
              f" ({len(text):,} chars; rules: {', '.join(RULES)})")
        for f in fails:
            print(f"  - {f}")
        for w in warn:
            print(f"  ~ warning (not fatal, flex passed): {w}")
        bad += bool(fails)
    raise SystemExit(1 if bad else 0)


if __name__ == "__main__":
    main(sys.argv[1:])
