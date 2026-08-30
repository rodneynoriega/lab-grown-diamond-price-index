#!/usr/bin/env python3
"""publish_gate: per-page QA/traceability checks for pilot batches.

Implements check D of SKILLS/publish-gate/SKILL.md (per-page QA), plus the
machinery the negative-control test (check C) exercises. This script never
publishes and never calls the Shopify API; it validates a batch's QA
manifest against the actual data on disk and says PASS/FAIL per page.

Input: a QA manifest (JSON list or {"pages": [...]}), one record per page,
emitted by the page pipeline (item #6) alongside the rollback manifest:

  {
    "handle": "...", "title": "...", "h1": "...",
    "target_keyword": "...",            # required; logged per retitle rule
    "page_type": "stone" | "aggregate",
    "data_timestamp": "YYYY-MM-DD",     # scrape date backing the numbers
    "cycle": "YYYY-MM",
    "source_files": ["data/raw/x.csv" | "data/snapshots/x.csv.gz", ...],
    "sample_size": <int>,               # stone: displayed offers;
                                        # aggregate: n listings
    "retailer_set": ["Brilliant Earth", ...],   # DISPLAYED retailers
    "cert": {"lab": "IGI", "number": "..."},    # stone pages only
    "scope": {"cert_matched_only": true,
              "ring_total_present": false,
              "ring_total_separated": false}
  }

Checks per page (a failure lists every reason, not just the first):
  1. structure     - required fields present and well-typed
  2. source        - every source file exists under the project data dirs;
                     data_timestamp appears in at least one source filename;
                     stone pages: the cert number is actually FOUND in a
                     referenced source file (true traceability, not
                     metadata trust)
  3. sample size   - present; stone pages need >= 2 displayed retailer
                     offers and sample_size == len(retailer_set);
                     aggregate pages need n >= 30 (the index n-rule)
  4. retailer set  - non-empty, known retailers only;
                     HARD FAIL if With Clarity is displayed (Rodney
                     condition 1, 2026-08-09: WC excluded from published
                     pilot pages); stone pages only cert-capable
                     retailers; VRAI/Blue Nile never on stone pages
  5. timestamp     - parseable, not future, not stale (> MAX_AGE_DAYS),
                     consistent with the stated cycle
  6. title         - retitle-compliant: "price index" never in the SEO
                     title (in-page branding only); a price-intent term
                     required; target_keyword required and logged;
                     HARD FAIL if the title leads with a panel retailer
                     brand (Family C held back, Rodney condition 2)
  7. scope         - stone pages: cert present + cert_matched_only;
                     any ring-total content must be flagged separated

Exit codes: 0 all pages pass; 1 at least one page fails (failing pages are
excluded, the rest may proceed, per Rodney's rule; the caller decides);
2 malformed manifest/arguments (nothing was checked).

Per SKILL.md check C: after ANY change to this file's check logic, re-run
the negative-control test (a deliberately bad page MUST fail) before the
gate's verdict is trusted again.
"""

import argparse
import datetime
import gzip
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA_DIRS = [HERE / "data" / "raw", HERE / "data" / "snapshots",
             HERE / "data" / "processed"]

MAX_AGE_DAYS = 35          # weekly cadence target; monthly tolerance cap
MIN_STONE_RETAILERS = 2    # a cross-retailer comparison needs >= 2 displayed
MIN_AGGREGATE_N = 30       # the index's published n-rule

# Rodney condition 1 (2026-08-09): With Clarity data does NOT appear on
# published pilot pages. Collection unaffected. Display banned.
BANNED_DISPLAY = {"with clarity"}

# Panel retailers whose brand LEADING a title marks a Family C page
# (held back per Rodney condition 2, 2026-08-09).
PANEL_BRANDS = ["brilliant earth", "blue nile", "ritani", "clean origin",
                "with clarity", "grown brilliance", "vrai", "james allen"]

KNOWN_RETAILERS = {"brilliant earth", "blue nile", "ritani", "clean origin",
                   "grown brilliance", "vrai", "with clarity"}
# Cert capture: BE/GB/Ritani 100%; CO fixed forward from 2026-08-08.
# BN has no certs in the schema; VRAI has no third-party certs; WC banned.
STONE_CAPABLE = {"brilliant earth", "grown brilliance", "ritani",
                 "clean origin"}

PRICE_INTENT = re.compile(r"\b(price|prices|priced|cost|costs)\b", re.I)

REQUIRED = {"handle", "title", "target_keyword", "page_type",
            "data_timestamp", "cycle", "source_files", "sample_size",
            "retailer_set", "scope"}


def resolve_source(rel):
    p = Path(rel)
    if not p.is_absolute():
        p = HERE / rel
    return p


def read_source_text(path):
    if path.suffix == ".gz":
        return gzip.decompress(path.read_bytes()).decode(
            "utf-8", errors="replace")
    return path.read_text(errors="replace")


def check_page(page, today):
    """Returns a list of failure reasons (empty = PASS)."""
    fails = []
    if not isinstance(page, dict):
        return ["record is not an object"]

    missing = REQUIRED - set(page)
    if missing:
        fails.append(f"structure: missing required fields {sorted(missing)}")
        # deliberately NO early return: every other defect on the page is
        # still reported, so one missing field can never mask a second
        # problem from the reviewer

    ptype = page.get("page_type")
    if ptype not in ("stone", "aggregate"):
        fails.append(f"structure: page_type '{ptype}' invalid")

    # ---- 2. source traceability -------------------------------------
    sources = page.get("source_files")
    resolved = []
    if not isinstance(sources, list) or not sources:
        fails.append("source: source_files missing or empty (numbers are "
                     "not traceable to any scrape)")
        sources = []
    for s in sources:
        p = resolve_source(s)
        if not p.exists():
            fails.append(f"source: {s} does not exist on disk")
        elif not any(str(p).startswith(str(d)) for d in DATA_DIRS):
            fails.append(f"source: {s} is outside the project data dirs")
        else:
            resolved.append(p)

    ts = str(page.get("data_timestamp") or "")
    if resolved and ts:
        if not any(ts in p.name for p in resolved):
            fails.append(f"source: data_timestamp {ts} matches no "
                         f"referenced source filename (dates disagree)")

    cert = page.get("cert") or {}
    if ptype == "stone":
        num = str(cert.get("number") or "").strip()
        if not num:
            fails.append("scope: stone page without a certificate number")
        elif resolved:
            digits = num.upper().removeprefix("LG")
            found = False
            for p in resolved:
                try:
                    if digits in read_source_text(p):
                        found = True
                        break
                except OSError as e:
                    fails.append(f"source: cannot read {p.name} ({e})")
            if not found:
                fails.append(f"source: cert {num} not found in any "
                             f"referenced source file (figures not "
                             f"traceable to a scrape row)")
            # Per-retailer, per-cycle traceability (added 2026-08-29 after
            # review): every retailer the page claims for a cycle must have
            # a referenced source file OF THAT RETAILER, dated IN THAT
            # CYCLE, that contains the cert. Catches stale counts relabeled
            # under a new cycle even when no `delisted` block is present.
            claims = [(page.get("retailer_set") or [], str(page.get("cycle") or ""))]
            dl = page.get("delisted")
            if isinstance(dl, dict):
                claims.append((dl.get("last_observed_retailers") or [],
                               str(dl.get("last_observed_cycle") or "")))
            for rets, cyc in claims:
                if not cyc:
                    continue
                for r in rets:
                    slug = str(r).strip().lower().replace(" ", "_")
                    cands = [q for q in resolved
                             if q.name.startswith(slug + "_" + cyc)
                             or q.name.startswith(cyc + "-" + slug)]
                    hit = False
                    for q in cands:
                        try:
                            if digits in read_source_text(q):
                                hit = True
                                break
                        except OSError:
                            pass
                    if not hit:
                        fails.append(f"source: cert {num} not found in any "
                                     f"referenced {r} file for cycle {cyc} "
                                     f"(claimed retailer/cycle not traceable)")

    # ---- 3. sample size ---------------------------------------------
    # Delisted variant (added 2026-08-29): a LIVE stone page whose stone no
    # longer has >= MIN_STONE_RETAILERS displayed prices this cycle. The
    # record carries the truth for the current cycle (sample_size =
    # current displayed count, may be 0; retailer_set = current displayed
    # retailers, may be empty) PLUS a `delisted` block naming the last
    # cycle that had a real comparison; that last-observed comparison must
    # itself satisfy the stone rule, and the cert must still be traceable
    # to a referenced source file (check 2).
    delisted = page.get("delisted")
    if delisted is not None:
        if ptype != "stone" or not isinstance(delisted, dict):
            fails.append("delisted: only stone pages may carry a delisted "
                         "block, and it must be an object")
            delisted = None
        else:
            lo_ret = delisted.get("last_observed_retailers")
            lo_n = delisted.get("last_observed_count")
            cur_ret = delisted.get("current_retailers")
            if (not isinstance(lo_ret, list) or not lo_ret
                    or not isinstance(lo_n, int) or isinstance(lo_n, bool)
                    or lo_n != len(lo_ret) or lo_n < MIN_STONE_RETAILERS):
                fails.append(f"delisted: last-observed comparison invalid "
                             f"(count {lo_n!r}, retailers {lo_ret!r}); needs "
                             f">= {MIN_STONE_RETAILERS} displayed retailers")
            if not isinstance(delisted.get("last_observed_cycle"), str):
                fails.append("delisted: last_observed_cycle missing")
            for r in (lo_ret or []):
                low = str(r).strip().lower()
                if low in BANNED_DISPLAY or low not in STONE_CAPABLE:
                    fails.append(f"delisted: '{r}' may not appear in a "
                                 f"stone comparison")
            if not isinstance(cur_ret, list) or \
                    sorted(map(str, cur_ret)) != sorted(map(str, page.get("retailer_set") or [])):
                fails.append("delisted: current_retailers must equal retailer_set")
    n = page.get("sample_size")
    if (not isinstance(n, int) or isinstance(n, bool)
            or n < (0 if delisted is not None else 1)):
        fails.append(f"sample: sample_size missing or invalid ({n!r})")
    else:
        if ptype == "stone" and delisted is not None:
            if n >= MIN_STONE_RETAILERS:
                fails.append(f"delisted: page claims delisted but has {n} "
                             f"displayed offers this cycle")
        elif ptype == "stone":
            if n < MIN_STONE_RETAILERS:
                fails.append(f"sample: stone page has {n} displayed "
                             f"offer(s); cross-retailer comparison needs "
                             f">= {MIN_STONE_RETAILERS}")
        elif ptype == "aggregate":
            if n < MIN_AGGREGATE_N:
                fails.append(f"sample: aggregate n={n} below the "
                             f"published n>={MIN_AGGREGATE_N} rule")

    # ---- 4. retailer set --------------------------------------------
    rset = page.get("retailer_set")
    if not isinstance(rset, list) or (not rset and delisted is None):
        fails.append("retailers: retailer_set missing or empty")
        rset = []
    lowered = [str(r).strip().lower() for r in rset]
    for r, low in zip(rset, lowered):
        if low in BANNED_DISPLAY:
            fails.append(f"retailers: '{r}' is EXCLUDED from published "
                         f"pilot pages (accepted-risk memo condition 1, "
                         f"2026-08-09); it must not be displayed")
        elif low not in KNOWN_RETAILERS:
            fails.append(f"retailers: '{r}' is not a known panel retailer")
        elif ptype == "stone" and low not in STONE_CAPABLE:
            fails.append(f"retailers: '{r}' cannot appear on a cert-matched "
                         f"stone page (no third-party cert capture)")
    if len(set(lowered)) != len(lowered):
        fails.append("retailers: duplicate retailer in retailer_set")
    if (ptype == "stone" and isinstance(n, int)
            and not isinstance(n, bool) and (rset or delisted is not None)
            and n != len(rset)):
        fails.append(f"sample: stone sample_size {n} != displayed "
                     f"retailer count {len(rset)}")

    # ---- 5. timestamp staleness -------------------------------------
    try:
        d = datetime.date.fromisoformat(ts)
        if d > today:
            fails.append(f"timestamp: {ts} is in the future")
        elif (today - d).days > MAX_AGE_DAYS:
            fails.append(f"timestamp: {ts} is {(today - d).days} days old "
                         f"(stale; max {MAX_AGE_DAYS})")
        cyc = str(page.get("cycle") or "")
        if cyc and ts[:7] != cyc:
            fails.append(f"timestamp: data_timestamp {ts} not in stated "
                         f"cycle {cyc}")
    except ValueError:
        fails.append(f"timestamp: data_timestamp '{ts}' unparseable "
                     f"(need YYYY-MM-DD)")

    # ---- 6. title / retitle compliance -------------------------------
    title = str(page.get("title") or "").strip()
    if not title:
        fails.append("title: missing")
    else:
        if re.search(r"\bprice\s+index\b", title, re.I):
            fails.append("title: 'Price Index' is in-page branding only, "
                         "never the SEO title/H1 target (retitle "
                         "instruction 2026-08-09)")
        if not PRICE_INTENT.search(title):
            fails.append(f"title: no price-intent term in '{title}'")
        tl = title.lower()
        for brand in PANEL_BRANDS:
            if tl.startswith(brand):
                fails.append(f"title: leads with retailer brand '{brand}': "
                             f"Family C is HELD BACK from the initial "
                             f"publish (condition 2, 2026-08-09)")
    if not str(page.get("target_keyword") or "").strip():
        fails.append("title: target_keyword missing (every title must be "
                     "logged against its keyword)")

    # ---- 7. scope rules ----------------------------------------------
    scope = page.get("scope") or {}
    if not isinstance(scope, dict):
        fails.append("scope: not an object")
        scope = {}
    if ptype == "stone" and scope.get("cert_matched_only") is not True:
        fails.append("scope: stone page must assert cert_matched_only "
                     "(certificate-matched comparison only, no blended "
                     "ring totals)")
    if scope.get("ring_total_present") and not scope.get(
            "ring_total_separated"):
        fails.append("scope: ring-total content present but not flagged "
                     "as a separated, labeled estimate section")

    return fails


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--qa-manifest", required=True,
                    help="JSON QA manifest for the batch")
    ap.add_argument("--today", default=None,
                    help="override gate date YYYY-MM-DD (tests only)")
    args = ap.parse_args()

    try:
        raw = json.loads(Path(args.qa_manifest).read_text())
    except (OSError, json.JSONDecodeError) as e:
        print(f"qa-manifest {args.qa_manifest}: {e}")
        raise SystemExit(2)
    pages = raw.get("pages") if isinstance(raw, dict) else raw
    if not isinstance(pages, list) or not pages:
        print(f"qa-manifest {args.qa_manifest}: need a non-empty list "
              f"(or {{'pages': [...]}})")
        raise SystemExit(2)

    today = (datetime.date.fromisoformat(args.today) if args.today
             else datetime.date.today())

    passed = failed = 0
    for i, page in enumerate(pages):
        label = (page.get("handle") if isinstance(page, dict) else None) \
            or f"entry-{i + 1}"
        fails = check_page(page, today)
        if fails:
            failed += 1
            print(f"FAIL {label}")
            for f in fails:
                print(f"  - {f}")
        else:
            passed += 1
            kw = page.get("target_keyword")
            print(f"PASS {label} (title -> keyword: '{kw}')")

    print(f"publish_gate check D: {passed} passed, {failed} failed, "
          f"{len(pages)} total")
    if failed:
        print("Failing pages are EXCLUDED from the batch; the remainder "
              "may proceed only under a current AUTHORIZED gate report "
              "(checks A/B/C/E still apply at batch level).")
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
