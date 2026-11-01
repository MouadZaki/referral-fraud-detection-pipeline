# Stage 1 — CSV → PostgreSQL with Airflow

Local stack that validates `Conversation.csv` (Kaggle *3K Conversations
Dataset for ChatBot*) and loads it into a PostgreSQL `conversations`
table via the Airflow DAG `load_3k_conversations_to_postgres`.

Services (see `docker-compose.yaml`):

- `airflow-webserver` — UI at <http://localhost:8080>
- `airflow-scheduler` — runs the DAG (LocalExecutor)
- `airflow-postgres` — Airflow's own metadata DB (internal)
- `postgres-target` — the database the DAG loads into (`:5433` on the host)
- `airflow-init` — one-shot migrations + admin user, then exits

## Prerequisites

- Docker Desktop (with Compose v2) running.
- PowerShell (commands below work in it).
- The downloaded Kaggle extract `Conversation.csv`.
- Optional for unit tests: a Python venv with `pip install -r requirements-dev.txt`.

## 1. Place the CSV

Put the file in the dataset folder, next to `manifest.yaml`/`schema.yaml`/`ddl.sql`:

```
stage-1-csv-to-postgres\3k-conversations\Conversation.csv
```

It is mounted read-only at `/opt/airflow/data/3k-conversations` and is
git-ignored — never commit it.

## 2. Create `.env` (secrets stay local)

```powershell
cd stage-1-csv-to-postgres
Copy-Item .env.example .env
```

Edit `.env` and fill the three **required** passwords (blank by
default — pick your own, do not paste real values anywhere):

```
AIRFLOW_DB_PASSWORD=          # Airflow metadata DB
TARGET_POSTGRES_PASSWORD=     # target PostgreSQL (our data)
_AIRFLOW_WWW_USER_PASSWORD=   # Airflow UI admin password
```

`.env` is git-ignored; `.env.example` is the tracked template.

## 3. Build and start

```powershell
docker compose build          # builds stage-1-airflow:2.11.0-py3.11 (first run pulls base image)
docker compose up -d
docker compose ps -a          # wait for airflow-init to show Exited (0); webserver+scheduler healthy
                              # (`ps -a` is needed: plain `ps` hides one-shot containers once they exit)
```

If `up` reports `Set X in .env`, a required password is still blank.

## 4. Trigger the DAG

DAGs start **paused** (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION`).

UI: open <http://localhost:8080>, log in (`admin` + your
`_AIRFLOW_WWW_USER_PASSWORD`), unpause `load_3k_conversations_to_postgres`
and press the play button.

Or CLI:

```powershell
docker compose exec airflow-scheduler airflow dags unpause load_3k_conversations_to_postgres
docker compose exec airflow-scheduler airflow dags trigger load_3k_conversations_to_postgres
```

Task order: `check_dataset_status` → `read_csv` → `validate_schema` →
`transform` → `load_to_postgres`.

## 5. Check the newest run

```powershell
docker compose exec airflow-scheduler airflow dags list-runs -d load_3k_conversations_to_postgres
docker compose logs -f airflow-scheduler     # or follow live logs
```

All five tasks green / run state `success` means the load completed.

## 6. Verify the data landed

`postgres-target` exposes port **5433** on the host. Check inside the
container (credentials come from the container env — nothing hardcoded):

```powershell
docker compose exec postgres-target sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT COUNT(*) AS n, MIN(source_row_id) AS min_id, MAX(source_row_id) AS max_id FROM conversations;"'
```

Expected:

```
  n   | min_id | max_id
------+--------+--------
 3725 |      0 |   3724
```

## 7. Rerun safely

Loading is idempotent (`INSERT ... ON CONFLICT (source_row_id) DO UPDATE`
over the PK). Retrigger anytime — the count stays 3725 and rows are
refreshed, not duplicated.

```powershell
docker compose exec airflow-scheduler airflow dags trigger load_3k_conversations_to_postgres
```

## 8. Deliberate-failure check (malformed CSV)

Back up the real file **outside the repo** first, then drop in a
1-row file and queue a run:

```powershell
Copy-Item .\3k-conversations\Conversation.csv "$env:TEMP\Conversation.csv.bak"
",question,answer`n0,hi?,fine" | Out-File .\3k-conversations\Conversation.csv -Encoding utf8
docker compose exec airflow-scheduler airflow dags trigger load_3k_conversations_to_postgres
```

`dags trigger` only **queues** the run — it returns immediately and the
DAG then executes asynchronously. Wait until the new run's
`validate_schema` task has actually failed before touching the CSV:

```powershell
# poll until the NEWEST run shows state 'failed' (validate_schema also turns red in the UI grid)
docker compose exec airflow-scheduler airflow dags list-runs -d load_3k_conversations_to_postgres
```

Only after the run has failed, restore the original file:

```powershell
Move-Item -Force "$env:TEMP\Conversation.csv.bak" .\3k-conversations\Conversation.csv
```

Expected error in the `validate_schema` task log:
`unexpected row count: expected 3725, got 1`.

> **Failed runs stay red.** Airflow history is append-only — the failed
> run remains visible in `list-runs` and the UI grid forever, even after
> later green runs. That is intentional audit trail, not a stuck state.

## 9. Stop the stack (keep the data)

```powershell
docker compose down           # removes containers/networks; named volumes KEEP the DB data
```

`docker compose stop` also works. Only `docker compose down -v` deletes
the volumes (both databases' data) — avoid it unless you want a clean slate.

## Troubleshooting

- **`Set X in .env` / init fails instantly** — a required password is
  blank in `.env`. Fill it, then `docker compose up -d` again.
  `docker compose logs airflow-init` shows the real error.
- **CSV missing** — `read_csv` fails with `Input CSV not found: ...`.
  Check the file is named exactly `Conversation.csv` in
  `3k-conversations/` and the `docker-compose.yaml` mount line exists.
- **Validation failed** — `validate_schema` fails with a
  `SchemaValidationError` listing every violation (wrong header, bad
  row count, blanks, non-integer/duplicate ids). Open the task log in
  the UI grid (click the red box → Log). Fix the CSV, restore it, retrigger.
- **Scheduler/webserver not healthy** — `docker compose ps`, then
  `docker compose logs airflow-scheduler` / `airflow-webserver`.
  Confirm `airflow-init` exited 0 first and port 8080 isn't taken.
- **`target_postgres` connection errors in `load_to_postgres`** —
  check `postgres-target` is healthy (`docker compose ps -a`) and read
  the `load_to_postgres` task log in the Airflow UI for the real error.
  Then prove the target DB itself works, without printing secrets:
  `docker compose exec postgres-target sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT current_user, current_database();"'`
  — it should return your `.env` `TARGET_POSTGRES_USER` /
  `TARGET_POSTGRES_DB` values. Never print `AIRFLOW_CONN_TARGET_POSTGRES`
  or run `airflow connections get` — both expose the password in
  terminal/log output.
- **Stale `.env` changes not picked up** — recreate containers:
  `docker compose up -d --force-recreate`.

## Git hygiene

- `.env`, `*.csv` in `3k-conversations/`, and `logs/` are ignored —
  real secrets, data, and runtime logs never enter Git.
- Tracked: `Dockerfile`, `docker-compose.yaml`, `.env.example`, `dags/`,
  `tests/`, and the `3k-conversations/` contract files.

## Optional: unit tests (no Docker needed)

```powershell
python -m pytest stage-1-csv-to-postgres/tests/ -v   # run from repo root
```
