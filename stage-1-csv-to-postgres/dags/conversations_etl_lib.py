"""Pure helpers for the 3K Conversations CSV -> PostgreSQL pipeline.

Deliberately free of Airflow imports and heavy top-level imports so it
can be imported cheaply during DAG parsing and unit-tested outside the
Airflow image (pandas / PyYAML are imported lazily inside functions).

Manifest status policy: only status "Done" is loadable. Any other
status listed for a dataset aborts the run — datasets are loaded
deliberately, never accidentally.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

ALLOWED_MANIFEST_STATUSES: frozenset[str] = frozenset({"Done"})

_TARGET_TABLE = "conversations"


class SchemaValidationError(ValueError):
    """Raised when a CSV or manifest violates the declared contract."""


# ---------------------------------------------------------------- YAML ---

def _load_yaml(path: Path | str, kind: str) -> dict[str, Any]:
    import yaml  # lazy: keeps DAG-parse and non-yaml tests cheap

    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"{kind} file not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SchemaValidationError(f"{kind} file is not a YAML mapping: {path}")
    return data


def load_manifest(path: Path | str) -> dict[str, Any]:
    """Load manifest.yaml as a mapping."""
    return _load_yaml(path, "manifest")


def load_schema(path: Path | str) -> dict[str, Any]:
    """Load schema.yaml as a mapping."""
    return _load_yaml(path, "schema")


def require_allowed_status(manifest: dict[str, Any], source: str = "manifest.yaml") -> str:
    """Return the manifest status, or raise if it is not loadable.

    Only "Done" is accepted. Any other value (Pending, Completed,
    Done/pending, missing, ...) raises SchemaValidationError so the DAG
    fails at the first task with a clear reason.
    """
    status = manifest.get("status")
    if status not in ALLOWED_MANIFEST_STATUSES:
        raise SchemaValidationError(
            f"{source}: dataset status {status!r} is not loadable; "
            f"allowed statuses: {sorted(ALLOWED_MANIFEST_STATUSES)}. "
            "Update the dataset or promote it deliberately instead of "
            "loading it silently."
        )
    return str(status)


# ----------------------------------------------------------------- CSV ---

def read_csv_raw(path: Path | str) -> "tuple[object, list[str]]":
    """Read a CSV keeping every value as a string and the TRUE header.

    Returns ``(DataFrame, raw_header)``. ``df.columns`` is set to the
    actual header cells (including the genuinely blank first cell of
    Conversation.csv) instead of pandas' synthetic "Unnamed: 0" names,
    so validation can compare against ``schema.raw_header`` exactly.
    """
    import pandas as pd

    path = Path(path)
    with path.open(newline="", encoding="utf-8-sig") as fh:
        raw_header = next(csv.reader(fh))
    frame = pd.read_csv(
        path, dtype=str, keep_default_na=False, encoding="utf-8-sig"
    )
    if len(raw_header) == frame.shape[1]:
        frame.columns = raw_header
    return frame, list(raw_header)


def source_to_column_map(schema: dict[str, Any]) -> dict[str, str]:
    """{raw CSV header cell -> target column name} from schema columns."""
    return {
        str(col.get("source_column", "")): str(col["name"])
        for col in schema.get("columns", [])
    }


# ------------------------------------------------------------ VALIDATE ---

def validate_raw_frame(df, schema: dict[str, Any], source_file: str = "") -> None:
    """Enforce schema.yaml against a raw frame (string columns).

    Checks, in order: exact raw header, expected row count, blank/NULL
    required values, integer-coercible primary key, and PK uniqueness.
    Raises SchemaValidationError listing every problem found.
    """
    # 1. Header must match the raw contract exactly, positionally.
    expected_header = [str(h) for h in schema.get("raw_header", [])]
    actual_header = [str(c) for c in df.columns]
    if actual_header != expected_header:
        raise SchemaValidationError(
            f"wrong CSV header: expected {expected_header}, "
            f"got {actual_header}"
            + (f" (file: {source_file})" if source_file else "")
        )

    mapping = source_to_column_map(schema)
    work = df.rename(columns=mapping)
    errors: list[str] = []

    # 2. Row count must equal the manifest-verified expectation.
    expected_rows = schema.get("row_count")
    if expected_rows is not None and len(df) != int(expected_rows):
        errors.append(
            f"unexpected row count: expected {int(expected_rows)}, "
            f"got {len(df)}"
        )

    # 3. Required columns must not contain NULL/blank/whitespace.
    for col in schema.get("columns", []):
        name = str(col["name"])
        if name not in work.columns:
            errors.append(f"missing mapped column {name!r}")
            continue
        if col.get("nullable") is False:
            blank = work[name].isna() | work[name].astype(str).str.strip().eq("")
            n_blank = int(blank.sum())
            if n_blank:
                errors.append(
                    f"column {name!r}: {n_blank} blank/NULL value(s) "
                    "but nullable=false"
                )

    # 4. Primary key: integer-coercible and unique.
    pk_cols = [str(c) for c in schema.get("primary_key", [])]
    for pk in pk_cols:
        if pk not in work.columns:
            errors.append(f"missing primary-key column {pk!r}")
            continue
        stripped = work[pk].astype(str).str.strip()
        bad_int = ~stripped.str.fullmatch(r"\d+")
        n_bad = int(bad_int.sum())
        if n_bad:
            examples = stripped.loc[bad_int].unique()[:5].tolist()
            errors.append(
                f"primary key {pk!r}: {n_bad} non-integer value(s), "
                f"e.g. {examples}"
            )
        else:
            ids = stripped.astype("int64")
            dup = ids[ids.duplicated()].unique()
            if len(dup):
                errors.append(
                    f"primary key {pk!r}: {len(dup)} duplicated value(s), "
                    f"e.g. {dup[:5].tolist()}"
                )

    if errors:
        raise SchemaValidationError(
            "CSV failed schema validation: " + "; ".join(errors)
        )


# ----------------------------------------------------------- TRANSFORM ---

def transform_frame(df, schema: dict[str, Any]):
    """Clean a validated raw frame into the target table's shape.

    Renames raw headers to column names, orders columns per schema,
    trims whitespace, and casts integer columns.
    """
    mapping = source_to_column_map(schema)
    order = [str(c["name"]) for c in schema.get("columns", [])]
    out = df.rename(columns=mapping).loc[:, order].copy()
    for col in schema.get("columns", []):
        name, ctype = str(col["name"]), col.get("type")
        if ctype == "integer":
            out[name] = out[name].astype(str).str.strip().astype("int64")
        else:
            out[name] = out[name].astype(str).str.strip()
    return out


# ----------------------------------------------------------------- SQL ---

def build_upsert_sql(
    table: str, columns: list[str], primary_key: list[str]
) -> str:
    """Idempotent INSERT for psycopg2 ``execute_values`` (``VALUES %s``).

    Upserts on the primary key: a rerun refreshes rows instead of
    duplicating them.
    """
    cols = ", ".join(columns)
    pk = ", ".join(primary_key)
    updates = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in columns if c not in primary_key
    )
    conflict = f"DO UPDATE SET {updates}" if updates else "DO NOTHING"
    return (
        f"INSERT INTO {table} ({cols}) VALUES %s "
        f"ON CONFLICT ({pk}) {conflict}"
    )


TARGET_TABLE = _TARGET_TABLE
