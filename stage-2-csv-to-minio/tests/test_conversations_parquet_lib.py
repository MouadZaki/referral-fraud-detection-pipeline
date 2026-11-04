"""Contract/transform/Parquet tests for the Stage 2 CSV -> MinIO pipeline.

Exercises conversations_parquet_lib directly — no Airflow, no MinIO,
no real sample.csv needed. Run from repo root:

    python -m pytest stage-2-csv-to-minio/tests/ -v
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_DIR))

from conversations_parquet_lib import (  # noqa: E402
    ContractViolation,
    build_object_key,
    frame_to_parquet_bytes,
    partition_frames,
    read_cleaned_csv,
    read_csv_raw,
    require_allowed_status,
    transform_frame,
    validate_against_contract,
)


@pytest.fixture
def contract() -> dict:
    """Minimal copy of schema_contract.yaml (row_count=3, max id 2)."""
    return {
        "dataset": "3k-conversations",
        "source_file": "dags/extraction/sample.csv",
        "row_count": 3,
        "raw_header": ["", "question", "answer"],
        "primary_key": ["source_row_id"],
        "columns": [
            {
                "name": "source_row_id",
                "source_column": "",
                "type": "integer",
                "nullable": False,
                "unique": True,
                "min": 0,
                "max": 2,
            },
            {
                "name": "question",
                "source_column": "question",
                "type": "string",
                "nullable": False,
                "empty_is_null": True,
            },
            {
                "name": "answer",
                "source_column": "answer",
                "type": "string",
                "nullable": False,
                "empty_is_null": True,
            },
        ],
    }


def make_frame(rows, header=("", "question", "answer")):
    return pd.DataFrame(rows, columns=list(header))


SECRET_Q = "do not leak this question text"
GOOD_ROWS = [
    ("0", SECRET_Q, "fine"),
    ("1", "hi?", "okay"),
    ("2", "name?", "c"),
]


# ------------------------------------------------------------ valid ---

def test_valid_frame_passes(contract):
    validate_against_contract(make_frame(GOOD_ROWS), contract)


# ------------------------------------------------------------ header ---

def test_renamed_column_fails(contract):
    df = make_frame(GOOD_ROWS, header=("row_id", "question", "answer"))
    with pytest.raises(ContractViolation, match=r"\[header_mismatch\]"):
        validate_against_contract(df, contract)


def test_extra_column_fails(contract):
    df = make_frame(GOOD_ROWS, header=("", "question", "answer"))
    df["extra"] = "x"
    with pytest.raises(ContractViolation, match=r"\[header_mismatch\]"):
        validate_against_contract(df, contract)


def test_reordered_columns_fail(contract):
    df = make_frame(GOOD_ROWS, header=("question", "", "answer"))
    with pytest.raises(ContractViolation, match=r"\[header_mismatch\]"):
        validate_against_contract(df, contract)


def test_missing_column_fails(contract):
    df = make_frame([r[:2] for r in GOOD_ROWS], header=("", "question"))
    with pytest.raises(ContractViolation, match=r"\[header_mismatch\]"):
        validate_against_contract(df, contract)


# --------------------------------------------------------- row count ---

def test_unexpected_row_count_fails(contract):
    with pytest.raises(ContractViolation, match=r"\[unexpected_row_count\]"):
        validate_against_contract(make_frame(GOOD_ROWS[:2]), contract)


# ------------------------------------------------------------- blanks ---

def test_blank_question_fails(contract):
    rows = [list(r) for r in GOOD_ROWS]
    rows[1][1] = "   "
    with pytest.raises(ContractViolation, match=r"\[blank_required_value\].*question"):
        validate_against_contract(make_frame(rows), contract)


def test_null_answer_fails(contract):
    rows = [list(r) for r in GOOD_ROWS]
    rows[0][2] = None
    with pytest.raises(ContractViolation, match=r"\[blank_required_value\].*answer"):
        validate_against_contract(make_frame(rows), contract)


# ----------------------------------------------------------------- ids ---

def test_non_integer_id_fails(contract):
    rows = [list(r) for r in GOOD_ROWS]
    rows[0][0] = "abc"
    with pytest.raises(ContractViolation, match=r"\[invalid_type\]"):
        validate_against_contract(make_frame(rows), contract)


def test_out_of_range_id_fails(contract):
    rows = [list(r) for r in GOOD_ROWS]
    rows[0][0] = "9999"
    with pytest.raises(ContractViolation, match=r"\[out_of_range\]"):
        validate_against_contract(make_frame(rows), contract)


def test_duplicate_id_fails(contract):
    rows = [list(r) for r in GOOD_ROWS]
    rows[2][0] = "1"
    with pytest.raises(ContractViolation, match=r"\[duplicate_primary_key\]"):
        validate_against_contract(make_frame(rows), contract)


def test_errors_never_leak_conversation_text(contract):
    rows = [list(r) for r in GOOD_ROWS]
    rows[0][2] = ""  # trigger blank_required_value on answer
    with pytest.raises(ContractViolation) as excinfo:
        validate_against_contract(make_frame(rows), contract)
    assert SECRET_Q not in str(excinfo.value)


# ----------------------------------------------------------- transform ---

def test_transform_renames_casts_and_partitions(contract):
    cleaned = transform_frame(make_frame(GOOD_ROWS), contract)
    assert list(cleaned.columns) == [
        "source_row_id", "question", "answer", "id_range",
    ]
    assert cleaned["source_row_id"].tolist() == [0, 1, 2]
    assert cleaned["source_row_id"].dtype.kind == "i"
    assert cleaned["id_range"].unique().tolist() == ["0-999"]


def test_partition_labels_cover_thousands(contract):
    rows = [(str(i), "q", "a") for i in (0, 999, 1000, 2500, 3724)]
    wide = dict(contract, row_count=5)
    wide["columns"][0]["max"] = 3724
    cleaned = transform_frame(make_frame(rows), wide)
    assert cleaned["id_range"].tolist() == [
        "0-999", "0-999", "1000-1999", "2000-2999", "3000-3999",
    ]


# --------------------------------------------------------------- keys ---

def test_partition_frames_and_stable_keys(contract):
    cleaned = transform_frame(make_frame(GOOD_ROWS), contract)
    parts = partition_frames(cleaned)
    assert list(parts) == ["0-999"]
    key1 = build_object_key("3k-conversations", "0-999")
    key2 = build_object_key("3k-conversations", "0-999")
    assert key1 == key2 == "3k-conversations/id_range=0-999/data.parquet"


def test_loader_iteration_yields_label_and_frame_pairs(contract):
    """Regression for the DAG's `for label, part in ...items()` loop:
    unpacks each id_range label with its own frame (dict iteration
    alone raised ValueError at runtime)."""
    rows = [(str(i), "q", "a") for i in (0, 999, 1000, 2000, 3724)]
    wide = dict(contract, row_count=5)
    wide["columns"][0]["max"] = 3724
    cleaned = transform_frame(make_frame(rows), wide)

    seen = {}
    for label, part in partition_frames(cleaned).items():
        seen[label] = len(part)
    assert seen == {
        "0-999": 2,
        "1000-1999": 1,
        "2000-2999": 1,
        "3000-3999": 1,
    }


# ------------------------------------------------------------- parquet ---

def test_parquet_round_trip_schema_and_rows(contract):
    cleaned = transform_frame(make_frame(GOOD_ROWS), contract)
    blob = frame_to_parquet_bytes(cleaned)
    back = pd.read_parquet(io.BytesIO(blob))
    assert list(back.columns) == [
        "source_row_id", "question", "answer", "id_range",
    ]
    assert back["source_row_id"].tolist() == [0, 1, 2]
    assert back["question"].tolist() == [SECRET_Q, "hi?", "name?"]


def test_staged_cleaned_reread_preserves_na_like_strings(tmp_path, contract):
    """Regression: "NA"/"001" must survive the DAG's load-task reread.

    Stages a cleaned row exactly as transform does, rereads through the
    same read_cleaned_csv helper load_to_minio uses, then checks the
    Parquet payload — pandas must not coerce to NaN/1 anywhere.
    """
    rows = [("0", "NA", "001"), ("1", "q", "a"), ("2", "q2", "a2")]
    cleaned = transform_frame(make_frame(rows), contract)
    staged = tmp_path / "cleaned.csv"
    cleaned.to_csv(staged, index=False)

    back = read_cleaned_csv(staged)
    assert back.loc[0, "question"] == "NA"
    assert back.loc[0, "answer"] == "001"
    assert back["source_row_id"].tolist() == [0, 1, 2]
    assert back["source_row_id"].dtype.kind == "i"

    rt = pd.read_parquet(io.BytesIO(frame_to_parquet_bytes(back)))
    assert rt.loc[0, "question"] == "NA"
    assert rt.loc[0, "answer"] == "001"


# ------------------------------------------------------------- manifest ---

def test_status_done_allowed():
    assert require_allowed_status({"status": "Done"}) == "Done"


@pytest.mark.parametrize("status", ["Pending", "Completed", "Done/pending", None])
def test_other_statuses_rejected(status):
    with pytest.raises(ContractViolation, match="not loadable"):
        require_allowed_status({"status": status})


# -------------------------------------------------- raw read helpers ---

def test_read_csv_raw_preserves_blank_header(tmp_path):
    p = tmp_path / "mini.csv"
    p.write_text(",question,answer\n0,hi?,fine\n", encoding="utf-8")
    df, header = read_csv_raw(p)
    assert header == ["", "question", "answer"]
    assert list(df.columns) == ["", "question", "answer"]


def test_missing_file_raises_rule_named_error(tmp_path):
    with pytest.raises(FileNotFoundError, match=r"\[missing_source_file\]"):
        read_csv_raw(tmp_path / "nope.csv")


# ------------------------------------------------ real contract sanity ---

def test_real_contract_declares_expected_shape():
    path = DAGS_DIR.parent / "3k-conversations" / "schema_contract.yaml"
    contract = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert contract["raw_header"] == ["", "question", "answer"]
    assert contract["source_file"] == "dags/extraction/sample.csv"
    assert contract["row_count"] == 3725
    assert contract["primary_key"] == ["source_row_id"]
    blank = [c for c in contract["columns"] if c["source_column"] == ""]
    assert len(blank) == 1 and blank[0]["name"] == "source_row_id"
    assert blank[0]["min"] == 0 and blank[0]["max"] == 3724
    for rule in (
        "header_mismatch", "unexpected_row_count", "missing_mapped_column",
        "blank_required_value", "invalid_type", "out_of_range",
        "duplicate_primary_key", "missing_source_file",
    ):
        assert rule in contract["fail_on"]
