"""Validation/transform tests for the Stage 1 CSV pipeline helpers.

These tests exercise conversations_etl_lib directly — no Airflow and
no real Conversation.csv needed. Run with:

    python -m pytest stage-1-csv-to-postgres/tests/ -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_DIR))

from conversations_etl_lib import (  # noqa: E402
    SchemaValidationError,
    build_upsert_sql,
    read_csv_raw,
    require_allowed_status,
    transform_frame,
    validate_raw_frame,
)


@pytest.fixture
def schema() -> dict:
    """Minimal copy of schema.yaml's contract (row_count=3 for speed)."""
    return {
        "dataset": "3k-conversations",
        "source_file": "Conversation.csv",
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
            },
            {
                "name": "question",
                "source_column": "question",
                "type": "string",
                "nullable": False,
            },
            {
                "name": "answer",
                "source_column": "answer",
                "type": "string",
                "nullable": False,
            },
        ],
    }


def make_frame(rows, header=("", "question", "answer")):
    return pd.DataFrame(rows, columns=list(header))


GOOD_ROWS = [
    ("0", "hi, how are you doing?", "i'm fine. how about yourself?"),
    ("1", "hi, how are you doing?", "i'm fine. how about yourself?"),
    ("2", "what's your name?", "my friends call me c"),
]


# ------------------------------------------------------------- header ---

def test_valid_frame_passes(schema):
    validate_raw_frame(make_frame(GOOD_ROWS), schema)


def test_wrong_header_fails(schema):
    df = make_frame(GOOD_ROWS, header=("row_id", "question", "answer"))
    with pytest.raises(SchemaValidationError, match="wrong CSV header"):
        validate_raw_frame(df, schema)


def test_read_csv_raw_preserves_blank_header(tmp_path):
    csv_path = tmp_path / "mini.csv"
    csv_path.write_text(
        ",question,answer\n0,hi?,fine\n", encoding="utf-8"
    )
    df, raw_header = read_csv_raw(csv_path)
    assert raw_header == ["", "question", "answer"]
    assert list(df.columns) == ["", "question", "answer"]


# ---------------------------------------------------------- row count ---

def test_unexpected_row_count_fails(schema):
    df = make_frame(GOOD_ROWS[:2])  # 2 rows, schema expects 3
    with pytest.raises(SchemaValidationError, match="row count"):
        validate_raw_frame(df, schema)


# --------------------------------------------------------------- blanks ---

def test_blank_question_fails(schema):
    rows = [list(r) for r in GOOD_ROWS]
    rows[1][1] = "   "  # whitespace-only question
    with pytest.raises(SchemaValidationError, match="question"):
        validate_raw_frame(make_frame(rows), schema)


def test_null_answer_fails(schema):
    rows = [list(r) for r in GOOD_ROWS]
    rows[0][2] = None
    with pytest.raises(SchemaValidationError, match="answer"):
        validate_raw_frame(make_frame(rows), schema)


# ---------------------------------------------------------------- ids ---

def test_non_integer_id_fails(schema):
    rows = [list(r) for r in GOOD_ROWS]
    rows[0][0] = "abc"
    with pytest.raises(SchemaValidationError, match="non-integer"):
        validate_raw_frame(make_frame(rows), schema)


def test_duplicate_id_fails(schema):
    rows = [list(r) for r in GOOD_ROWS]
    rows[2][0] = "1"  # duplicate of second row's id
    with pytest.raises(SchemaValidationError, match="duplicated"):
        validate_raw_frame(make_frame(rows), schema)


def test_collects_multiple_errors(schema):
    rows = [list(r) for r in GOOD_ROWS[:2]]
    rows[0][1] = ""
    with pytest.raises(SchemaValidationError) as excinfo:
        validate_raw_frame(make_frame(rows), schema)
    assert "row count" in str(excinfo.value)
    assert "question" in str(excinfo.value)


# ----------------------------------------------------------- transform ---

def test_transform_renames_casts_and_strips(schema):
    rows = [(f" {i} ", f" q{i} ", f" a{i} ") for i in range(3)]
    cleaned = transform_frame(make_frame(rows), schema)
    assert list(cleaned.columns) == ["source_row_id", "question", "answer"]
    assert cleaned["source_row_id"].tolist() == [0, 1, 2]
    assert cleaned["source_row_id"].dtype.kind == "i"
    assert cleaned["question"].tolist() == ["q0", "q1", "q2"]


# ------------------------------------------------------------- upsert ---

def test_build_upsert_sql_is_idempotent():
    sql = build_upsert_sql(
        "conversations", ["source_row_id", "question", "answer"],
        ["source_row_id"],
    )
    assert "ON CONFLICT (source_row_id)" in sql
    assert "DO UPDATE" in sql
    assert "question = EXCLUDED.question" in sql
    assert "answer = EXCLUDED.answer" in sql
    assert "source_row_id = EXCLUDED" not in sql  # PK is not overwritten


# ------------------------------------------------------------- manifest ---

def test_status_done_allowed():
    assert require_allowed_status({"status": "Done"}) == "Done"


@pytest.mark.parametrize("status", ["Pending", "Completed", "Done/pending", None])
def test_other_statuses_rejected(status):
    with pytest.raises(SchemaValidationError, match="not loadable"):
        require_allowed_status({"status": status})


# ------------------------------------------------ real schema sanity ---

def test_real_schema_blank_header_mapping():
    """schema.yaml really declares the blank-header -> source_row_id map."""
    schema_path = DAGS_DIR.parent / "3k-conversations" / "schema.yaml"
    schema = yaml.safe_load(schema_path.read_text(encoding="utf-8"))
    assert schema["raw_header"][0] == ""
    blank = [c for c in schema["columns"] if c["source_column"] == ""]
    assert len(blank) == 1 and blank[0]["name"] == "source_row_id"
    assert schema["primary_key"] == ["source_row_id"]
