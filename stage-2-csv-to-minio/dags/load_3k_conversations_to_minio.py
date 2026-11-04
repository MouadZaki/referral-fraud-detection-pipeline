"""Stage 2 — load the 3K Conversations extract into MinIO as Parquet.

Pipeline shape:  check_dataset_status >> read_csv >> validate_contract
                 >> transform >> load_to_minio

Dataset status policy (manifest.yaml, mounted read-only at
CONTRACT_DIR): only status "Done" is loadable; any other status fails
the first task.

Contract enforcement (schema_contract.yaml): exact raw header
["", "question", "answer"], 3,725 rows, integer+unique source_row_id
in [0, 3724], nonblank question/answer. Missing, renamed, extra,
reordered, or mistyped columns fail `validate_contract` BEFORE any
Parquet is written. Errors name the broken fail_on rule and report
counts — never conversation text.

Partitioning: no date field exists, so output is partitioned
deterministically on source_row_id ranges of 1,000
(id_range=0-999 ... id_range=3000-3999) — stable keys
`3k-conversations/id_range=<label>/data.parquet` are overwritten on
reruns, so a rerun cannot create duplicate files.

MinIO access: boto3 client built from the plain MINIO_* env vars
supplied by Compose — no connection URI, and credentials are never
logged. Only paths/row counts travel via XCom; data moves through
staged files under /tmp (LocalExecutor, single container). Nothing is
read at parse time.
"""

from __future__ import annotations

import os
import tempfile
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException

from conversations_parquet_lib import (
    build_object_key,
    frame_to_parquet_bytes,
    load_contract,
    load_manifest,
    partition_frames,
    read_cleaned_csv,
    read_csv_raw,
    require_allowed_status,
    transform_frame,
    validate_against_contract,
)

CONTRACT_DIR = Path(os.environ.get("CONTRACT_DIR", "/opt/airflow/contracts"))
DATASET_NAME = "3k-conversations"
CSV_PATH = Path(
    os.environ.get("EXTRACTION_FILE", "/opt/airflow/dags/extraction/sample.csv")
)

with DAG(
    dag_id="load_3k_conversations_to_minio",
    description="Validate the 3K Conversations CSV and land it in MinIO as partitioned Parquet.",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,  # single-writer: no concurrent overwrites of the same partition keys
    default_args={"owner": "data-engineering", "retries": 0},
    tags=["stage-2", "csv-to-minio", "3k-conversations"],
) as dag:

    @task()
    def check_dataset_status() -> dict:
        """Gate on manifest.yaml status; only "Done" proceeds."""
        manifest_path = CONTRACT_DIR / "manifest.yaml"
        manifest = load_manifest(manifest_path)
        status = require_allowed_status(manifest, source=str(manifest_path))
        return {"dataset": manifest.get("dataset"), "status": status}

    @task()
    def read_csv() -> dict:
        """Read the extract and stage it for downstream tasks."""
        try:
            frame, _raw_header = read_csv_raw(CSV_PATH)
        except FileNotFoundError as exc:
            raise AirflowException(str(exc)) from exc
        staged = Path(tempfile.mkdtemp(prefix="conv_s2_")) / "raw_staged.csv"
        frame.to_csv(staged, index=False)
        return {"staged_path": str(staged), "rows_read": len(frame)}

    @task()
    def validate_contract(read: dict) -> dict:
        """Fail hard on any schema_contract.yaml rule before Parquet."""
        contract = load_contract(CONTRACT_DIR / "schema_contract.yaml")
        try:
            frame, _ = read_csv_raw(read["staged_path"])
        except FileNotFoundError as exc:
            raise AirflowException(str(exc)) from exc
        validate_against_contract(frame, contract)
        return {
            "staged_path": read["staged_path"],
            "rows_validated": len(frame),
        }

    @task()
    def transform(validated: dict) -> dict:
        """Rename/clean/partition-label; stage the cleaned frame."""
        contract = load_contract(CONTRACT_DIR / "schema_contract.yaml")
        try:
            frame, _ = read_csv_raw(validated["staged_path"])
        except FileNotFoundError as exc:
            raise AirflowException(str(exc)) from exc
        cleaned = transform_frame(frame, contract)
        clean_path = Path(validated["staged_path"]).with_name("cleaned.csv")
        cleaned.to_csv(clean_path, index=False)
        return {"clean_path": str(clean_path), "rows": len(cleaned)}

    @task()
    def load_to_minio(prepared: dict) -> dict:
        """Write one Parquet object per id_range partition to MinIO."""
        import boto3

        # Coercion-free reread: strings like "NA"/"001" stay verbatim.
        frame = read_cleaned_csv(prepared["clean_path"])
        dataset = DATASET_NAME
        bucket = os.environ["MINIO_BUCKET"]
        client = boto3.client(
            "s3",
            endpoint_url=f"http://{os.environ['MINIO_ENDPOINT']}",
            aws_access_key_id=os.environ["MINIO_ROOT_USER"],
            aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
            region_name="us-east-1",
        )

        written: list[str] = []
        for label, part in partition_frames(frame).items():
            key = build_object_key(dataset, label)
            client.put_object(
                Bucket=bucket, Key=key, Body=frame_to_parquet_bytes(part)
            )
            written.append(key)

        return {"bucket": bucket, "objects": written, "rows": len(frame)}

    _status = check_dataset_status()
    _read = read_csv()
    _status >> _read
    _validated = validate_contract(_read)
    _prepared = transform(_validated)
    _load = load_to_minio(_prepared)
