# Stage 2 — CSV → MinIO: 3K Conversations to Parquet

Validates the same 3K Conversations extract used in Stage 1 against a
strict schema contract, transforms it, and lands it in MinIO as
partitioned Parquet. Runs on a local Airflow + MinIO Docker stack.

- **DAG**: `load_3k_conversations_to_minio` — manual trigger only
  (`schedule=None`, `catchup=False`, `max_active_runs=1`).
- **Tasks**: `check_dataset_status >> read_csv >> validate_contract
  >> transform >> load_to_minio`.
- **Target**: MinIO bucket `stage2-data`, four stable Parquet objects.
- **Dataset contract**: `3k-conversations/manifest.yaml` +
  `3k-conversations/schema_contract.yaml` (mounted read-only into
  Airflow at `/opt/airflow/contracts`).

## Stack

| Service             | What it does                                        | Ports            |
|---------------------|-----------------------------------------------------|------------------|
| `airflow-webserver` | Airflow UI + API                                    | `8081`           |
| `airflow-scheduler` | Parses + runs DAGs (LocalExecutor)                  | —                |
| `airflow-postgres`  | Airflow's metadata DB (internal, not our data)      | —                |
| `airflow-init`      | One-shot: migrations + admin user                   | —                |
| `minio`             | S3-compatible object store (Chainguard, digest-pinned) | `9000`, `9001` |
| `minio-init`        | One-shot: creates `stage2-data` if absent (boto3, idempotent) | —   |

Port `8081` is deliberate — the Stage 1 stack keeps host port `8080`.

### Files

```text
stage-2-csv-to-minio/
├── Dockerfile                          # airflow:2.11.0-py3.11 + pandas/pyarrow/amazon-provider
├── docker-compose.yaml                 # the stack above
├── .env.example                        # template -> copy to .env (ignored)
├── .gitignore                          # logs/ + dags/extraction/*.csv
├── 3k-conversations/
│   ├── manifest.yaml                   # provenance; status must be "Done"
│   └── schema_contract.yaml            # header/row/type/range rules
├── dags/
│   ├── conversations_parquet_lib.py    # pure helpers (no Airflow imports)
│   ├── load_3k_conversations_to_minio.py  # the DAG
│   └── extraction/sample.csv           # <-- YOU place this (ignored)
├── minio-init/create_bucket.py         # boto3 bucket bootstrap
└── tests/test_conversations_parquet_lib.py
```

## Prerequisites

- Docker Desktop with Compose v2 (`docker compose version`).
- PowerShell (all commands below assume it).
- Python 3.10+ with pandas/pytest/PyYAML/pyarrow — only for the local
  unit tests (`pip install -r requirements-dev.txt` from repo root).

## Setup

### 1. Environment file

```powershell
cd stage-2-csv-to-minio
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
notepad .env
```

Fill the three required passwords — Compose refuses to start while any
of them is empty (`${VAR:?Set ... in .env}` interpolation):

- `AIRFLOW_DB_PASSWORD` — Airflow metadata DB (internal).
- `_AIRFLOW_WWW_USER_PASSWORD` — the Airflow UI login (user `admin`).
- `MINIO_ROOT_PASSWORD` — MinIO root login (user `minioadmin`;
  **at least 8 characters**).

`.env` is git-ignored and must never be committed. Do not put real
passwords in screenshots, commits, or terminal output.

### 2. Place the extract

The DAG reads `dags/extraction/sample.csv` — the same 3K Conversations
file used in Stage 1. Either download `Conversation.csv` from Kaggle
(<https://www.kaggle.com/datasets/kreeshrajani/3k-conversations-dataset-for-chatbot>)
and rename it, or reuse Stage 1's copy (still inside
`stage-2-csv-to-minio/` after step 1's `cd`):

```powershell
Copy-Item -LiteralPath "..\stage-1-csv-to-postgres\3k-conversations\Conversation.csv" `
          -Destination ".\dags\extraction\sample.csv"
```

The first header cell is genuinely blank (`,question,answer`) — that is
the contract, not a defect. `dags/extraction/*.csv` is git-ignored.

### 3. Build and start

```powershell
docker compose config --quiet      # resolves .env; errors if a password is blank
docker compose build
docker compose up -d
docker compose ps -a
docker compose logs minio-init     # "created bucket \"stage2-data\"" or "already exists"
```

Repeat `docker compose ps -a` until `airflow-webserver` and
`airflow-scheduler` show **healthy** — startup takes a minute while
`airflow-init` migrates. The one-shot `airflow-init` and `minio-init`
services should read **Exited (0)**.

### 4. UIs

- **Airflow**: <http://localhost:8081> — `admin` / your
  `_AIRFLOW_WWW_USER_PASSWORD`.
- **MinIO console**: <http://localhost:9001> — `minioadmin` / your
  `MINIO_ROOT_PASSWORD`. The `stage2-data` bucket is created by
  `minio-init`.

## Running the DAG

```powershell
docker compose exec airflow-scheduler airflow dags unpause load_3k_conversations_to_minio
docker compose exec airflow-scheduler airflow dags trigger load_3k_conversations_to_minio
docker compose exec airflow-scheduler airflow dags list-runs -d load_3k_conversations_to_minio
```

Triggering is asynchronous — repeat `dags list-runs` until the newest
run shows **success** before checking MinIO below.

Confirm the landed objects under the dataset prefix (reads the
container's own env vars — prints keys only, never credentials):

```powershell
docker compose exec airflow-scheduler python -c "import boto3,os; c=boto3.client('s3',endpoint_url='http://'+os.environ['MINIO_ENDPOINT'],aws_access_key_id=os.environ['MINIO_ROOT_USER'],aws_secret_access_key=os.environ['MINIO_ROOT_PASSWORD']); print('\n'.join(o['Key'] for o in c.list_objects_v2(Bucket=os.environ['MINIO_BUCKET'],Prefix='3k-conversations/').get('Contents',[])))"
```

Expected output — exactly four keys:

```text
3k-conversations/id_range=0-999/data.parquet
3k-conversations/id_range=1000-1999/data.parquet
3k-conversations/id_range=2000-2999/data.parquet
3k-conversations/id_range=3000-3999/data.parquet
```

Verify the row counts inside those Parquet objects (verified result:
**1,000 + 1,000 + 1,000 + 725 = 3,725 rows**):

```powershell
docker compose exec airflow-scheduler python -c "import boto3,os,io,pandas as pd; c=boto3.client('s3',endpoint_url='http://'+os.environ['MINIO_ENDPOINT'],aws_access_key_id=os.environ['MINIO_ROOT_USER'],aws_secret_access_key=os.environ['MINIO_ROOT_PASSWORD']); n=[(o['Key'],len(pd.read_parquet(io.BytesIO(c.get_object(Bucket=os.environ['MINIO_BUCKET'],Key=o['Key'])['Body'].read())))) for o in c.list_objects_v2(Bucket=os.environ['MINIO_BUCKET'],Prefix='3k-conversations/').get('Contents',[])]; [print(k,r) for k,r in n]; print('total',sum(r for _,r in n))"
```

## What gets enforced and written

### Contract (hard failures, not warnings)

`validate_contract` runs before anything reaches MinIO. Any violation
fails the task with the broken rule name — `[missing_source_file]`,
`[header_mismatch]` (missing/renamed/extra/reordered columns),
`[unexpected_row_count]`, `[missing_mapped_column]`,
`[blank_required_value]`, `[invalid_type]`, `[out_of_range]`,
`[duplicate_primary_key]`. Errors carry counts and column names only —
never conversation text.

| Rule                       | Enforced value                          |
|----------------------------|-----------------------------------------|
| `header_mismatch`          | Exact header `["", "question", "answer"]` |
| `unexpected_row_count`     | Exactly 3,725 data rows                 |
| `invalid_type`/`out_of_range`/`duplicate_primary_key` | `source_row_id` integer, unique, 0–3724 |
| `blank_required_value`     | `question`/`answer` nonblank            |
| `missing_source_file`      | `dags/extraction/sample.csv` exists     |

### Partitioning and reruns

The dataset has no date field, so output is partitioned
**deterministically** by `source_row_id` ranges of 1,000 — hence four
`id_range` partitions (`0-999` … `3000-3999`; the last label describes
its fixed bucket even though data ends at 3,724). Object keys are
stable and `put_object` overwrites them, so a rerun with this contract
always writes the same four keys — rerunning never creates extra
objects. `max_active_runs=1` additionally prevents two concurrent runs
from racing the same keys.

## Negative test — malformed CSV must fail red

Proves the contract gate works. Backup goes **outside** the repo so it
cannot be committed by accident.

```powershell
# 1. back up the good extract OUTSIDE the repo (unique name) and note its hash
$bak = "$env:TEMP\sample_$(Get-Date -Format yyyyMMddHHmmss).csv.bak"
Copy-Item .\dags\extraction\sample.csv $bak
$origHash = (Get-FileHash .\dags\extraction\sample.csv -Algorithm SHA256).Hash

# 2. replace it with a one-row file (3,725 expected -> 1)
",question,answer`n0,hi?,fine" | Out-File .\dags\extraction\sample.csv -Encoding utf8

# 3. trigger, then poll list-runs until the NEWEST run shows failed
#    (trigger is asynchronous — repeat until state = failed)
docker compose exec airflow-scheduler airflow dags trigger load_3k_conversations_to_minio
docker compose exec airflow-scheduler airflow dags list-runs -d load_3k_conversations_to_minio
# once red, open the run in http://localhost:8081 -> the
# validate_contract task log shows [unexpected_row_count] expected 3725 rows, got 1

# 4. restore the good file AFTER the failed run completes, then confirm
#    the restore byte-for-byte via SHA-256
Move-Item -Force $bak .\dags\extraction\sample.csv
(Get-FileHash .\dags\extraction\sample.csv -Algorithm SHA256).Hash -eq $origHash   # must print True
```

Deliberately failed runs **stay red** in the DAG run history — Airflow
keeps them as an audit trail. That is expected; a later green run does
not erase them.

## Unit tests

```powershell
# from repo root, with dev deps installed (pytest, pyyaml, pyarrow)
pip install -r requirements-dev.txt
python -m pytest stage-2-csv-to-minio/tests/ -v
```

Covers the `fail_on` rules (every rule except `missing_mapped_column`,
which has no dedicated test — the exact-positional `header_mismatch`
check catches that situation first), the `"NA"`/`"001"`
string-coercion regression, partition boundaries, Parquet round-trip,
stable keys, and the manifest status gate — all without Airflow or
MinIO.

## Stopping and cleanup

```powershell
docker compose down        # stops containers; KEEPS named volumes
docker compose down -v     # stops and DELETES named volumes
```

`down` alone preserves `minio-data-volume` (your Parquet objects) and
`airflow-postgres-db-volume` (DAG history/admin user) across restarts.
`down -v` wipes both — the next `up` re-creates the bucket and admin
user, and any previous DAG history is gone. Built images survive either
command; remove them explicitly if you want a cold rebuild.

## Troubleshooting

- **`docker compose config` errors about `Set ... in .env`** — a
  required password is still blank in `.env` (step 1).
- **Port already in use on 8081** — something else holds it; set
  `AIRFLOW_WEB_PORT=8082` in `.env` (or stop Stage 1 if `8080` was the
  conflict — Stage 2 does not use it).
- **minio-init: `rejected the credentials from .env`** — `minio` and
  `minio-init` read the *same* `.env` values, so the credentials being
  sent are not what the server expects. Check
  `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` in `.env`, then run
  `docker compose up -d --force-recreate` so every container picks up
  the current environment values.
- **minio-init retries then exits 1** — MinIO never answered in ~30 s;
  check `docker compose logs minio` (e.g. `/data` permission issues).
- **DAG not listed in the UI** — check `docker compose logs
  airflow-scheduler` for parse errors; `dags/` is bind-mounted, so
  edits appear without a rebuild.
- **`[missing_source_file]` failure** — `sample.csv` isn't at
  `dags/extraction/sample.csv` (step 2).
- **`[header_mismatch]` on a file you edited** — the contract requires
  the exact `,question,answer` header, positionally. Reordered or
  renamed columns are rejected by design.
- **Edits to code/compose not taking effect** — `dags/` changes are
  live; `docker-compose.yaml` env/volume changes need
  `docker compose up -d --force-recreate`; `Dockerfile` changes need
  `docker compose build` first.

## Security notes

- `.env`, `dags/extraction/*.csv`, and `logs/` are git-ignored; keep it
  that way.
- Credentials travel to MinIO only inside container env vars; neither
  the DAG nor `minio-init` logs them.
- `source_row_id` is a source row index, not a business identifier —
  the contract preserves it as the primary key and nothing more.
