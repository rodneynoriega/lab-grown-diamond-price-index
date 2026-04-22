"""Shared types for retailer scrapers."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Benchmark:
    label_carat: float   # reported in the index: 1.0, 1.5, 2.0
    min_carat: float     # commercial bucket lower bound (inclusive)
    max_carat: float     # commercial bucket upper bound (inclusive)


@dataclass(frozen=True)
class Match:
    price_usd: float
    url: str
    actual_carat: float
    cut: str
    color: str
    clarity: str
    total_matches: int | None = None   # how many stones matched the spec


# Commercial carat buckets: a 1.03ct stone is sold as "1 carat".
BENCHMARKS = [
    Benchmark(1.0, 1.00, 1.09),
    Benchmark(1.5, 1.50, 1.59),
    Benchmark(2.0, 2.00, 2.19),
]

# Shared spec. "Excellent cut" is retailer-specific; each retailer module
# defines its own cut-grade mapping.
TARGET_COLORS = {"F", "G"}
TARGET_CLARITIES = {"VS2"}
