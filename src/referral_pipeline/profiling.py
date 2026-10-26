"""Profile the seven source CSV extracts before any transformation.

For every expected file this reports availability, row count, column
names, pandas and inferred dtypes, missing values, distinct value
counts, and fully duplicated rows — the two metrics the Stage 0 brief
requires are the null count and the distinct value count per column.
Missing or unreadable files are reported, never fabricated — a file
that cannot be read simply has no profile beyond the reason why.

Joins, cleaning, business rules, and the reward report are added in
later milestones; this module only inspects raw inputs.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

LOGGER = logging.getLogger("referral_pipeline.profiling")

PROFILE_SUMMARY_NAME = "profile_summary.csv"

# The seven extracts documented in data/README.md (Stage 0 brief).
EXPECTED_CSVS: tuple[str, ...] = (
    "user_referrals.csv",
    "user_referral_logs.csv",
    "user_logs.csv",
    "user_referral_statuses.csv",
    "referral_rewards.csv",
    "paid_transactions.csv",
    "lead_logs.csv",
)


@dataclass
class ColumnProfile:
    """Profile of a single column in a source CSV."""

    name: str
    pandas_dtype: str
    inferred_type: str
    missing_count: int
    missing_pct: float
    distinct_count: int


@dataclass
class TableProfile:
    """Profile of a single source CSV (or the reason it has none)."""

    filename: str
    path: Path
    available: bool = False
    row_count: int | None = None
    duplicate_rows: int | None = None
    columns: list[ColumnProfile] = field(default_factory=list)
    error: str | None = None


def profile_csv(path: Path) -> TableProfile:
    """Profile one CSV file. Never raises: missing/unreadable inputs are
    recorded on the returned TableProfile instead of crashing."""
    profile = TableProfile(filename=path.name, path=path)

    if not path.is_file():
        return profile  # available=False, error=None -> simply absent

    try:
        frame = pd.read_csv(path)
    except Exception as exc:  # noqa: BLE001 - report any parse failure
        profile.error = f"{type(exc).__name__}: {exc}"
        return profile

    profile.available = True
    profile.row_count = int(len(frame))
    profile.duplicate_rows = int(frame.duplicated().sum())

    for column in frame.columns:
        series = frame[column]
        missing = int(series.isna().sum())
        profile.columns.append(
            ColumnProfile(
                name=str(column),
                pandas_dtype=str(series.dtype),
                inferred_type=pd.api.types.infer_dtype(series, skipna=True),
                missing_count=missing,
                missing_pct=(
                    round(100.0 * missing / len(frame), 2) if len(frame) else 0.0
                ),
                # Mirrors SQL COUNT(DISTINCT col): nulls are not counted.
                distinct_count=int(series.nunique(dropna=True)),
            )
        )
    return profile


def profile_csvs(
    data_dir: Path,
    filenames: tuple[str, ...] = EXPECTED_CSVS,
) -> list[TableProfile]:
    """Profile every expected CSV inside data_dir, in documented order."""
    return [profile_csv(data_dir / name) for name in filenames]


def log_profiles(profiles: list[TableProfile], logger: logging.Logger = LOGGER) -> None:
    """Emit a human-readable profile for each table via the given logger."""
    available_count = 0
    for profile in profiles:
        if profile.error is not None:
            logger.error(
                "%s: present but unreadable (%s) - no profile produced",
                profile.filename,
                profile.error,
            )
            continue
        if not profile.available:
            logger.warning(
                "%s: not found at %s - no profile produced",
                profile.filename,
                profile.path,
            )
            continue

        available_count += 1
        logger.info(
            "%s: %d rows, %d fully duplicated rows, %d columns",
            profile.filename,
            profile.row_count,
            profile.duplicate_rows,
            len(profile.columns),
        )
        for column in profile.columns:
            logger.info(
                "    %-32s dtype=%-10s inferred=%-15s missing=%d (%.2f%%) "
                "distinct=%d",
                column.name,
                column.pandas_dtype,
                column.inferred_type,
                column.missing_count,
                column.missing_pct,
                column.distinct_count,
            )

    logger.info(
        "Profiled %d of %d expected CSVs.", available_count, len(profiles)
    )


def profiles_to_frame(profiles: list[TableProfile]) -> pd.DataFrame:
    """Flatten profiles into a tidy summary frame (one row per column).

    Missing/unreadable files still get a single row with available=False
    (or the error text) so the summary documents every expected input.
    """
    rows: list[dict] = []
    for profile in profiles:
        base = {
            "table": profile.filename,
            "available": profile.available,
            "row_count": profile.row_count,
            "duplicate_rows": profile.duplicate_rows,
            "error": profile.error,
        }
        if not profile.columns:
            rows.append({**base, "column": None, "pandas_dtype": None,
                         "inferred_type": None, "missing_count": None,
                         "missing_pct": None, "distinct_count": None})
        else:
            for column in profile.columns:
                rows.append(
                    {
                        **base,
                        "column": column.name,
                        "pandas_dtype": column.pandas_dtype,
                        "inferred_type": column.inferred_type,
                        "missing_count": column.missing_count,
                        "missing_pct": column.missing_pct,
                        "distinct_count": column.distinct_count,
                    }
                )
    return pd.DataFrame(
        rows,
        columns=[
            "table",
            "available",
            "row_count",
            "duplicate_rows",
            "error",
            "column",
            "pandas_dtype",
            "inferred_type",
            "missing_count",
            "missing_pct",
            "distinct_count",
        ],
    )


def write_profile_summary(
    profiles: list[TableProfile],
    output_dir: Path,
    filename: str = PROFILE_SUMMARY_NAME,
) -> Path:
    """Write the flattened profile summary CSV into output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / filename
    profiles_to_frame(profiles).to_csv(summary_path, index=False)
    LOGGER.info("Wrote profile summary to %s", summary_path)
    return summary_path
