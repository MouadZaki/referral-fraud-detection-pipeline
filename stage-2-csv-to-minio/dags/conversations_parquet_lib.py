"""Pure helpers for the Stage 2 3K-Conversations CSV -> Parquet pipeline.

No Airflow imports and no heavy top-level imports (pandas / PyYAML /
pyarrow are imported lazily inside functions) so the module is cheap
during DAG parsing and unit-testable outside the Airflow image.

Validation contract: stage-2-csv-to-minio/3k-conversations/
schema_contract.yaml. Every failure message names the `fail_on` rule
that fired (e.g. `[header_mismatch] ...`) and reports counts/column
names only — never conversation text or other cell values.

Manifest status policy: only status "Done" is loadable; anything else
fails fast — datasets are loaded deliberately, never accidentally.

Partitioning: the dataset carries no date field, so Parquet output is
partitioned DETERMINISTICALLY on source_row_id ranges of 1,000 —
labels `id_range=0-999`, `id_range=1000-1999`, ... (fixed buckets, so
the last label reads `3000-3999` even though data ends at 3724).
Stable labels produce stable object keys, so reruns overwrite rather
than duplicate.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Any

ALLOWED_MANIFEST_STATUSES: frozenset[str] = frozenset({"Done"})
PARTITION_SIZE = 1000


class ContractViolation(ValueError):
    """Raised when input data violates schema_contract.yaml."""


# ---------------------------------------------------------------- YAML ---

def _load_yaml(path: Path | str, kind: str) -> dict[str, Any]:
    import yaml  # lazy: keeps DAG-parse cheap

    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"{kind} file not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ContractViolation(f"{kind} file is not a YAML mapping: {path}")
    return data


def load_manifest(path: Path | str) -> dict[str, Any]:
    return _load_yaml(path, "manifest")


def load_contract(path: Path | str) -> dict[str, Any]:
    return _load_yaml(path, "schema contract")


def require_allowed_status(manifest: dict[str, Any], source: str = "manifest.yaml") -> str:
    """Return the manifest status, or raise unless it is exactly "Done"."""
    status = manifest.get("status")
    if status not in ALLOWED_MANIFEST_STATUSES:
        raise ContractViolation(
            f"{source}: dataset status {status!r} is not loadable; "
            f"allowed statuses: {sorted(ALLOWED_MANIFEST_STATUSES)}. "
            "Promote the dataset deliberately instead of loading it."
        )
    return str(status)


# ----------------------------------------------------------------- CSV ---

def read_csv_raw(path: Path | str) -> "tuple[object, list[str]]":
    """Read a CSV keeping every value as a string and the TRUE header.

    Returns ``(DataFrame, raw_header)`` with ``df.columns`` set to the
    actual header cells (including the genuinely blank first cell)
    instead of pandas' synthetic "Unnamed: 0" names, so the positional
    header check can run against ``contract.raw_header`` exactly.

    Raises FileNotFoundError tagged with the missing_source_file rule.
    """
    import pandas as pd

    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"[missing_source_file] CSV not found: {path}")
    with path.open(newline="", encoding="utf-8-sig") as fh:
        raw_header = next(csv.reader(fh))
    frame = pd.read_csv(
        path, dtype=str, keep_default_na=False, encoding="utf-8-sig"
    )
    if len(raw_header) == frame.shape[1]:
        frame.columns = raw_header
    return frame, list(raw_header)


def read_cleaned_csv(
    path: Path | str, int_columns: tuple[str, ...] = ("source_row_id",)
):
    """Reread a staged cleaned frame with ZERO pandas coercion.

    Everything loads as ``str`` (keep_default_na=False) so values like
    "NA" or "001" survive verbatim, then the named integer columns are
    cast back. This is the read path the DAG's load task uses.
    """
    import pandas as pd

    frame = pd.read_csv(Path(path), dtype=str, keep_default_na=False)
    for col in int_columns:
        frame[col] = frame[col].astype("int64")
    return frame


def source_to_column_map(contract: dict[str, Any]) -> dict[str, str]:
    return {
        str(col.get("source_column", "")): str(col["name"])
        for col in contract.get("columns", [])
    }


# ----------------------------------------------------------- VALIDATE ---

def validate_against_contract(df, contract: dict[str, Any]) -> None:
    """Enforce schema_contract.yaml against a raw string-column frame.

    Every violation is reported as ``[rule_name] description`` where
    rule_name matches the contract's ``fail_on`` list. Messages carry
    counts and column names only — never cell values. Raises
    ContractViolation listing every problem found.
    """
    # header_mismatch: exact positional match catches missing, renamed,
    # extra, and reordered raw columns in one rule.
    expected_header = [str(h) for h in contract.get("raw_header", [])]
    actual_header = [str(c) for c in df.columns]
    if actual_header != expected_header:
        raise ContractViolation(
            f"[header_mismatch] expected header {expected_header}, "
            f"got {actual_header}"
        )

    mapping = source_to_column_map(contract)
    work = df.rename(columns=mapping)
    errors: list[str] = []

    expected_rows = contract.get("row_count")
    if expected_rows is not None and len(df) != int(expected_rows):
        errors.append(
            f"[unexpected_row_count] expected {int(expected_rows)} "
            f"rows, got {len(df)}"
        )

    pk_cols = [str(c) for c in contract.get("primary_key", [])]

    for col in contract.get("columns", []):
        name = str(col["name"])
        if name not in work.columns:
            errors.append(
                f"[missing_mapped_column] column {name!r} has no "
                "matching raw header cell"
            )
            continue
        series = work[name]

        if col.get("nullable") is False:
            blank = series.isna()
            if col.get("empty_is_null", True):
                blank = blank | series.astype(str).str.strip().eq("")
            n_blank = int(blank.sum())
            if n_blank:
                errors.append(
                    f"[blank_required_value] column {name!r}: "
                    f"{n_blank} blank/NULL value(s)"
                )

        if col.get("type") == "integer":
            stripped = series.astype(str).str.strip()
            bad = series.isna() | ~stripped.str.fullmatch(r"-?\d+")
            n_bad = int(bad.sum())
            if n_bad:
                errors.append(
                    f"[invalid_type] column {name!r}: {n_bad} "
                    "non-integer value(s)"
                )
            else:
                values = stripped.astype("int64")
                lo, hi = col.get("min"), col.get("max")
                if lo is not None or hi is not None:
                    out = values < lo if lo is not None else values != values
                    if hi is not None:
                        out = out | (values > hi)
                    n_out = int(out.sum())
                    if n_out:
                        errors.append(
                            f"[out_of_range] column {name!r}: {n_out} "
                            f"value(s) outside [{lo}, {hi}]"
                        )
                if col.get("unique") or name in pk_cols:
                    n_dup = int(values.duplicated().sum())
                    if n_dup:
                        errors.append(
                            f"[duplicate_primary_key] column {name!r}: "
                            f"{n_dup} duplicated value(s)"
                        )

    if errors:
        raise ContractViolation(
            "CSV failed schema contract: " + "; ".join(errors)
        )


# ----------------------------------------------------------- TRANSFORM ---

def transform_frame(df, contract: dict[str, Any]):
    """Clean a validated raw frame and add the partition column.

    Renames raw headers to column names, orders per contract, trims
    whitespace, casts integers, then adds ``id_range`` — a deterministic
    partition label of the form "start-end" covering 1,000 ids each
    (see module docstring for why ranges, not dates).
    """
    mapping = source_to_column_map(contract)
    order = [str(c["name"]) for c in contract.get("columns", [])]
    out = df.rename(columns=mapping).loc[:, order].copy()
    for col in contract.get("columns", []):
        name, ctype = str(col["name"]), col.get("type")
        if ctype == "integer":
            out[name] = out[name].astype(str).str.strip().astype("int64")
        else:
            out[name] = out[name].astype(str).str.strip()
    pk = str(contract.get("primary_key", ["source_row_id"])[0])
    starts = (out[pk] // PARTITION_SIZE) * PARTITION_SIZE
    out["id_range"] = (
        starts.astype(str) + "-" + (starts + PARTITION_SIZE - 1).astype(str)
    )
    return out


def partition_frames(df) -> "dict[str, object]":
    """Split a transformed frame into {id_range label: sub-frame}."""
    return {
        str(label): part for label, part in df.groupby("id_range", sort=True)
    }


# ----------------------------------------------------------------- I/O ---

def build_object_key(dataset: str, id_range_label: str) -> str:
    """Deterministic object key — overwriting it keeps reruns clean."""
    return f"{dataset}/id_range={id_range_label}/data.parquet"


def frame_to_parquet_bytes(df) -> bytes:
    """Serialize a partition frame to Parquet bytes (snappy default)."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()
