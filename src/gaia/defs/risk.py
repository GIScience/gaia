"""
Composite risk scores (vul, cop, exp_<hazard>, sus_<hazard>, risk_<hazard>,
ranking_<hazard>) from the indicator columns of a country's combined table.

Must stay identical to the Disaster Risk Composer dashboard
(GIScience/Hazard-Risk-Composer), which recomputes the same scores in the
browser from the published *_risk.parquet. All weights are 1.
"""

import re

import pandas as pd

from gaia.defs.constants import (
    COPING_HAZARD_PATTERNS,
    COPING_NON_INVERTED_PATTERNS,
    EXPOSURE_PREFIXES,
)
from gaia.defs.utils import normalize_indicators


def is_inverted_coping(col: str) -> bool:
    """True if a higher raw value of this cop_ column means better coping."""
    return not any(re.search(p, col) for p in COPING_NON_INVERTED_PATTERNS)


def coping_hazard(col: str) -> str | None:
    """The hazard a cop_ column belongs to, or None if it counts for all hazards."""
    for hazard, pattern in COPING_HAZARD_PATTERNS.items():
        if re.search(pattern, col):
            return hazard
    return None


def coping_contributions(normalized: pd.DataFrame) -> pd.DataFrame:
    """
    Per-column coping contributions, oriented so that higher = less coping
    capacity. Missing values take the worst case (contribution 1).
    """
    cop_cols = [c for c in normalized.columns if c.startswith("cop_")]
    contrib = pd.DataFrame(index=normalized.index)
    for c in cop_cols:
        if is_inverted_coping(c):
            contrib[c] = 1 - normalized[c].fillna(0)
        else:
            contrib[c] = normalized[c].fillna(1)
    return contrib


def geometric_mean(parts: list[pd.Series]) -> pd.Series:
    """Geometric mean of the given dimension scores (dimensions left out are skipped)."""
    return pd.concat(parts, axis=1).prod(axis=1) ** (1 / len(parts))


def compute_risk_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the composite columns for one country.

    `df` is indexed by admin PCODE and holds the raw indicator columns
    (cop_*, vul_*, exp_flo_*, exp_cyc_*). Returns a frame with the
    same index containing only the composite columns.
    """
    indicator_cols = [c for c in df.columns if c.startswith(("cop_", "vul_", "exp_"))]
    normalized = normalize_indicators(df[indicator_cols].astype(float))

    cop_contrib = coping_contributions(normalized)
    cop_shared = [c for c in cop_contrib.columns if coping_hazard(c) is None]

    vul_cols = [c for c in normalized.columns if c.startswith("vul_")]
    vul = normalized[vul_cols].fillna(1).mean(axis=1) if vul_cols else None

    results = pd.DataFrame(index=df.index)
    coping_by_hazard = {}

    for hazard, prefix in EXPOSURE_PREFIXES.items():
        exp_cols = [c for c in normalized.columns if c.startswith(prefix)]
        if not exp_cols:
            continue

        cop_cols = cop_shared + [
            c for c in cop_contrib.columns if coping_hazard(c) == hazard
        ]
        cop = cop_contrib[cop_cols].mean(axis=1) if cop_cols else None
        coping_by_hazard[hazard] = cop

        exp = normalized[exp_cols].fillna(0).mean(axis=1)
        sus_parts = [s for s in (vul, cop) if s is not None]
        sus = geometric_mean(sus_parts) if sus_parts else None
        risk = geometric_mean([exp, sus]) if sus is not None else exp

        results[f"exp_{hazard}"] = exp
        if cop is not None:
            results[f"coping_{hazard}"] = cop
        if sus is not None:
            results[f"sus_{hazard}"] = sus
        results[f"risk_{hazard}"] = risk
        results[f"ranking_{hazard}"] = risk.rank(ascending=False)

    # "cop" holds the flood coping score; coping_<hazard> holds each hazard's.
    # Fall back to another hazard's coping, then to the shared columns only,
    # when there is no flood exposure.
    # Every output column name must be on the dashboard's ignore list
    # (vul, cop, exp_<hazard>, coping_<hazard>, sus_*, risk_*, rank_*,
    # ranking*), otherwise it is read as an input indicator. Agree new names
    # with the dashboard first.
    cop_out = next(
        (coping_by_hazard[h] for h in EXPOSURE_PREFIXES if coping_by_hazard.get(h) is not None),
        cop_contrib[cop_shared].mean(axis=1) if cop_shared else None,
    )

    front = pd.DataFrame(index=df.index)
    if cop_out is not None:
        front["cop"] = cop_out
    if vul is not None:
        front["vul"] = vul
    return pd.concat([front, results], axis=1)
