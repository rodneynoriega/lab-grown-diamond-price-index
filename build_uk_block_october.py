#!/usr/bin/env python3
"""Regenerate the October UK GemPages block from the staged JSON and add September to
Previous editions. Usage: python3 build_uk_block_october.py"""
import subprocess, sys
from pathlib import Path
H = Path(__file__).resolve().parent
O = H / "page_output_2026-10-uk"
out = O / "uk-index-gempages-2026-10-25.html"
subprocess.run([sys.executable, str(H / "page-assets/sync_uk_gempages_block.py"), "--template",
                str(H.parent.parent / "OUTBOX/2026-10-05-uk-index-gempages-signup-up.html"),
                "--json", str(O / "uk-index-data.json"), "--data-url-v", "14", "--out", str(out)], check=True)
h = out.read_text()
aug = '      <li style="font-size: 0.95rem; line-height: 2; color: #1a1a1a;"><a href="/pages/uk-lab-grown-diamond-price-index-2026-08"'
assert h.count(aug) == 1
sep = aug.replace("2026-08", "2026-09") + ' style="color: #1a1a1a; font-weight: 600; text-decoration: underline; text-underline-offset: 2px;">September 2026 edition</a></li>\n'
out.write_text(h.replace(aug, sep + aug))
print("ok", out)
