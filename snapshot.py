"""Listing-level snapshot persistence (Phase 0, task 1).

Mirrors every dated file in data/raw/ into data/snapshots/ as
    data/snapshots/YYYY-MM-DD-<name>[suffix].<ext>.gz
so the full listing-level record of each scrape run survives beyond this
machine once the snapshots directory is committed. data/raw/ stays
gitignored (uncompressed working copies); data/snapshots/ is the durable,
committed record.

Source filename convention it understands: <name>_<YYYY-MM-DD>[suffix].csv
(also .jsonl, used for UK raw API rows). Files that don't match the dated
pattern are skipped and reported -- nothing is ever modified or deleted in
data/raw/.

Gzip output is deterministic (mtime=0 in the gzip header), so re-compressing
an unchanged source produces byte-identical output and no git churn.

Usage:
    python3 snapshot.py sync          # snapshot anything new/changed in data/raw/
    python3 snapshot.py sync --force  # re-compress everything

Or from other scripts:
    from snapshot import snapshot_file, sync
"""

from __future__ import annotations

import gzip
import re
import shutil
import sys
from pathlib import Path

RAW_DIR = Path(__file__).parent / "data" / "raw"
SNAP_DIR = Path(__file__).parent / "data" / "snapshots"

DATED = re.compile(
    r"^(?P<name>.+?)_(?P<date>\d{4}-\d{2}-\d{2})(?P<suffix>[^.]*)\.(?P<ext>csv|jsonl)$"
)


def snapshot_dest(src: Path) -> Path | None:
    """Destination path for a raw file, or None if it isn't a dated data file."""
    m = DATED.match(src.name)
    if not m:
        return None
    return SNAP_DIR / (
        f"{m['date']}-{m['name']}{m['suffix']}.{m['ext']}.gz"
    )


def snapshot_file(src: Path, force: bool = False) -> Path | None:
    """Gzip one raw file into data/snapshots/. Returns the dest path if written,
    None if skipped (not a dated file, or snapshot already newer than source)."""
    dest = snapshot_dest(src)
    if dest is None:
        return None
    if dest.exists() and not force and dest.stat().st_mtime >= src.stat().st_mtime:
        return None
    SNAP_DIR.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    with src.open("rb") as f_in, tmp.open("wb") as f_raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=f_raw, mtime=0) as f_out:
            shutil.copyfileobj(f_in, f_out)
    tmp.replace(dest)
    return dest


def sync(force: bool = False, verbose: bool = True) -> list[Path]:
    """Snapshot every dated file in data/raw/. Idempotent. Returns paths written."""
    written: list[Path] = []
    skipped_undated: list[str] = []
    for src in sorted(RAW_DIR.iterdir()):
        if not src.is_file() or src.name.startswith("."):
            continue
        if snapshot_dest(src) is None:
            if src.suffix in (".csv", ".jsonl"):
                skipped_undated.append(src.name)
            continue
        dest = snapshot_file(src, force=force)
        if dest is not None:
            written.append(dest)
            if verbose:
                print(f"[snapshot] {src.name} -> {dest.name}")
    if verbose:
        if skipped_undated:
            print(f"[snapshot] skipped (no date in filename): {', '.join(skipped_undated)}")
        print(f"[snapshot] {len(written)} written, snapshots dir: {SNAP_DIR}")
    return written


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] != "sync":
        print(__doc__)
        sys.exit(1)
    sync(force="--force" in sys.argv)
    sys.exit(0)
