"""Shared types for retailer scrapers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

SHAPE_ALIASES: dict[str, str] = {
    "round brilliant": "round",
    "rb": "round",
    "oval": "oval",
    "pear": "pear",
    "pear shaped": "pear",
    "cushion": "cushion",
    "cushion modified": "cushion",
    "cushion brilliant": "cushion",
    "princess": "princess",
    "emerald": "emerald",
    "radiant": "radiant",
    "marquise": "marquise",
    "asscher": "asscher",
    "heart": "heart",
}

VALID_COLORS = {"D", "E", "F", "G", "H", "I", "J", "K"}
VALID_CLARITIES = {"FL", "IF", "VVS1", "VVS2", "VS1", "VS2", "SI1", "SI2", "I1"}
VALID_CERT_LABS = {"GIA", "IGI", "GCAL"}

CUT_ALIASES: dict[str, str] = {
    "super ideal": "Excellent",
    "ideal": "Excellent",
    "excellent": "Excellent",
    "very good": "Very Good",
    "good": "Good",
    "fair": "Fair",
    "poor": "Poor",
    # VRAI specific
    "cut for you": "Excellent",
}

FLUORESCENCE_ALIASES: dict[str, str] = {
    "none": "None",
    "faint": "Faint",
    "medium": "Medium",
    "strong": "Strong",
    "very strong": "Very Strong",
    "sl": "Faint",
    "sl1": "Faint",
    "vs": "Very Strong",
}


def normalize_shape(raw: str | None) -> str | None:
    if raw is None:
        return None
    key = raw.strip().lower()
    return SHAPE_ALIASES.get(key, key)


def normalize_cut(raw: str | None) -> str | None:
    if raw is None:
        return None
    key = raw.strip().lower()
    return CUT_ALIASES.get(key, raw.strip())


def normalize_fluorescence(raw: str | None) -> str | None:
    if raw is None:
        return None
    key = raw.strip().lower()
    return FLUORESCENCE_ALIASES.get(key, raw.strip())


def normalize_cert_lab(raw: str | None) -> str | None:
    if raw is None:
        return None
    upper = raw.strip().upper()
    return upper if upper in VALID_CERT_LABS else None


# ---------------------------------------------------------------------------
# Core dataclass
# ---------------------------------------------------------------------------

@dataclass
class Diamond:
    """One lab-grown diamond record as scraped from a retailer."""

    retailer: str
    date: str                        # YYYY-MM-DD
    scraped_at: str                  # ISO 8601 timestamp

    shape: str | None                # normalized to lowercase
    carat: float
    color: str | None
    clarity: str | None
    cut: str | None                  # normalized (Excellent / Very Good / Good / Fair / Poor)
    polish: str | None
    symmetry: str | None
    fluorescence: str | None
    certificate_lab: str | None      # GIA | IGI | GCAL | null
    certificate_number: str | None
    price_usd: float
    price_per_carat: float           # computed: price_usd / carat

    product_url: str

    @classmethod
    def build(
        cls,
        *,
        retailer: str,
        shape: str | None,
        carat: float,
        color: str | None,
        clarity: str | None,
        cut: str | None,
        polish: str | None,
        symmetry: str | None,
        fluorescence: str | None,
        certificate_lab: str | None,
        certificate_number: str | None,
        price_usd: float,
        product_url: str,
        date: str | None = None,
        scraped_at: str | None = None,
    ) -> "Diamond":
        now = datetime.utcnow()
        return cls(
            retailer=retailer,
            date=date or now.strftime("%Y-%m-%d"),
            scraped_at=scraped_at or now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            shape=normalize_shape(shape),
            carat=round(carat, 2),
            color=color.strip().upper() if color else None,
            clarity=clarity.strip() if clarity else None,
            cut=normalize_cut(cut),
            polish=normalize_cut(polish),     # same grade scale
            symmetry=normalize_cut(symmetry),
            fluorescence=normalize_fluorescence(fluorescence),
            certificate_lab=normalize_cert_lab(certificate_lab),
            certificate_number=certificate_number.strip() if certificate_number else None,
            price_usd=round(price_usd, 2),
            price_per_carat=round(price_usd / carat, 2) if carat > 0 else 0.0,
            product_url=product_url,
        )


# ---------------------------------------------------------------------------
# CSV schema
# ---------------------------------------------------------------------------

CSV_FIELDS = [
    "date", "retailer", "shape", "carat", "color", "clarity", "cut",
    "polish", "symmetry", "fluorescence", "certificate_lab", "certificate_number",
    "price_usd", "price_per_carat", "product_url", "scraped_at",
]


def diamond_to_row(d: Diamond) -> dict:
    return {
        "date": d.date,
        "retailer": d.retailer,
        "shape": d.shape,
        "carat": d.carat,
        "color": d.color,
        "clarity": d.clarity,
        "cut": d.cut,
        "polish": d.polish,
        "symmetry": d.symmetry,
        "fluorescence": d.fluorescence,
        "certificate_lab": d.certificate_lab,
        "certificate_number": d.certificate_number,
        "price_usd": d.price_usd,
        "price_per_carat": d.price_per_carat,
        "product_url": d.product_url,
        "scraped_at": d.scraped_at,
    }


# ---------------------------------------------------------------------------
# Validation cell definitions
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ValidationCell:
    label: str
    min_carat: float
    max_carat: float
    color: str
    clarity: str
    shape: str
    cut: str             # normalized grade expected
    cert_lab: str | None


VALIDATION_CELLS: list[ValidationCell] = [
    ValidationCell("1.0-1.09ct G VS1 Round Excellent IGI", 1.00, 1.09, "G", "VS1", "round", "Excellent", "IGI"),
    ValidationCell("1.4-1.59ct G VS1 Round Excellent IGI", 1.40, 1.59, "G", "VS1", "round", "Excellent", "IGI"),
    ValidationCell("1.9-2.09ct G VS1 Round Excellent IGI", 1.90, 2.09, "G", "VS1", "round", "Excellent", "IGI"),
    ValidationCell("1.9-2.09ct G VS1 Oval Excellent IGI",  1.90, 2.09, "G", "VS1", "oval",  "Excellent", "IGI"),
    ValidationCell("1.0-1.09ct G VS1 Round Excellent GIA", 1.00, 1.09, "G", "VS1", "round", "Excellent", "GIA"),
    ValidationCell("1.4-1.59ct G VS1 Round Excellent GIA", 1.40, 1.59, "G", "VS1", "round", "Excellent", "GIA"),
]


# ---------------------------------------------------------------------------
# Scrape target (shapes + carat range)
# ---------------------------------------------------------------------------

TARGET_SHAPES = ["round", "oval"]
MIN_CARAT = 0.90
MAX_CARAT = 2.50

# Legacy — kept so existing scraper stubs don't break during migration
@dataclass(frozen=True)
class Benchmark:
    label_carat: float
    min_carat: float
    max_carat: float


BENCHMARKS = [
    Benchmark(1.0, 1.00, 1.09),
    Benchmark(1.5, 1.50, 1.59),
    Benchmark(2.0, 2.00, 2.19),
]
