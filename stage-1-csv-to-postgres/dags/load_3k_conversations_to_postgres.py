"""Stage 1, Story 3 - load Conversation.csv into PostgreSQL.

Pipeline shape:  check_dataset_status >> read_csv >> validate_schema
                 >> transform >> load_to_postgres

Dataset status policy (manifest.yaml): only status "Done" is loadable.
Any other listed status, a missing status, or an unreadable manifest
fails the first task - datasets are promoted deliberately, not loaded
accidentally. Other statuses are not silently skipped or coerced.

Contract (schema.yaml, mounted read-only with the dataset): the raw
CSV header is exactly ["", "question", "answer"]; the blank first
header maps to source_row_id. Validation fails the run on a wrong
header, a row-count mismatch, blank required values, non-integer or
duplicate primary keys.

Loading is idempotent: the task applies ddl.sql (CREATE TABLE IF NOT
EXISTS) and then INSERT ... ON CONFLICT (source_row_id) DO UPDATE, so
a rerun refreshes rows instead of duplicating them.

Only small metadata (paths, row counts) travels via XCom - data moves
between tasks through staged CSV files under /tmp inside the same
container (LocalExecutor). Nothing is read at parse time.
"""

from __future__ import annotations

import os
import tempfile
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

from conversations_etl_lib import (
    TARGET_TABLE,
    build_upsert_sql,
    load_manifest,
    load_schema,
    read_csv_raw,
    require_allowed_status,
    transform_frame,
    validate_raw_frame,
)

DATASET_DIR = Path(os.environ.get("DATASET_DIR", "/opt/airflow/data/3k-conversations"))
CSV_FILENAME = "Conversation.csv"
TARGET_CONN_ID = "target_postgres"

with DAG(
    dag_id="load_3k_conversations_to_postgres",
    description="Validate and load the 3K Conversations CSV into PostgreSQL (idempotent).",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "data-engineering", "retries": 0},
    tags=["stage-1", "csv-to-postgres", "3k-conversations"],
) as dag:

    @task()
    def check_dataset_status() -> dict:
        """Gate on manifest.yaml status; only "Done" proceeds."""
        manifest_path = DATASET_DIR / "manifest.yaml"
        manifest = load_manifest(manifest_path)
        status = require_allowed_status(manifest, source=str(manifest_path))
        return {"dataset": manifest.get("dataset"), "status": status}

    @task()
    def read_csv() -> dict:
        """Read the raw CSV and stage it for downstream tasks."""
        csv_path = DATASET_DIR / CSV_FILENAME
        if not csv_path.is_file():
            raise AirflowException(
                f"Input CSV not found: {csv_path} "
                "(mount ./3k-conversations into the Airflow containers)."
            )
        frame, _raw_header = read_csv_raw(csv_path)
        staged = Path(tempfile.mkdtemp(prefix="conv_stage_")) / "raw_staged.csv"
        frame.to_csv(staged, index=False)
        return {"staged_path": str(staged), "rows_read": len(frame)}

    @task()
    def validate_schema(read: dict) -> dict:
        """Fail hard on any schema.yaml contract violation."""
        schema = load_schema(DATASET_DIR / "schema.yaml")
        source_file = str(schema.get("source_file") or CSV_FILENAME)
        if Path(source_file).name != CSV_FILENAME:
            raise AirflowException(
                f"schema.yaml expects file {source_file!r} but the DAG "
                f"loads {CSV_FILENAME!r}; update the schema or the dataset."
            )
        frame, _ = read_csv_raw(read["staged_path"])
        validate_raw_frame(frame, schema, source_file=CSV_FILENAME)
        return {
            "staged_path": read["staged_path"],
            "rows_validated": len(frame),
        }

    @task()
    def transform(validated: dict) -> dict:
        """Rename/clean into the target table shape; stage cleaned CSV."""
        schema = load_schema(DATASET_DIR / "schema.yaml")
        frame, _ = read_csv_raw(validated["staged_path"])
        cleaned = transform_frame(frame, schema)
        clean_path = Path(validated["staged_path"]).with_name("cleaned.csv")
        cleaned.to_csv(clean_path, index=False)
        return {"clean_path": str(clean_path), "rows": len(cleaned)}

    @task()
    def load_to_postgres(prepared: dict) -> dict:
        """Create the table if needed and upsert every row by PK."""
        import pandas as pd
        from psycopg2.extras import execute_values

        schema = load_schema(DATASET_DIR / "schema.yaml")
        columns = [str(c["name"]) for c in schema.get("columns", [])]
        primary_key = [str(c) for c in schema.get("primary_key", [])]

        frame = pd.read_csv(prepared["clean_path"])
        rows = list(frame[columns].itertuples(index=False, name=None))

        ddl = (DATASET_DIR / "ddl.sql").read_text(encoding="utf-8")
        upsert = build_upsert_sql(TARGET_TABLE, columns, primary_key)

        hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
        conn = hook.get_conn()
        try:
            with conn:  # single transaction: DDL + upsert + count
                with conn.cursor() as cur:
                    cur.execute(ddl)
                    execute_values(cur, upsert, rows, page_size=1000)
                    cur.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}")
                    total = cur.fetchone()[0]
        finally:
            conn.close()

        return {"rows_sent": len(rows), "table_rows": total}

    _status = check_dataset_status()
    _read = read_csv()
    _status >> _read
    _validated = validate_schema(_read)
    _prepared = transform(_validated)
    _load = load_to_postgres(_prepared)
