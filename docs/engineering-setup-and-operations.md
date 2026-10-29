# Engineering Setup & Operations — Referral Fraud Detection

How to run the pipeline locally and in Docker, what inputs it expects,
what it writes, and how to diagnose failures. Commands are written for
**Windows PowerShell** (the development environment); bash variants
are shown where they differ.

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Python | 3.10+ | Check with `python --version`. |
| pip | 21.3+ | Required for editable installs (`pip install -e .`). |
| Docker | any recent | Only needed for the container path (Docker Desktop on Windows). |
| pytest | 8.0+ | Dev dependency — only needed to run the test suite. |

The runtime dependency set is small: **pandas** (declared in
`pyproject.toml`). PySpark is installed **inside the Docker image**
per the assignment requirement but is not used by the pipeline code —
the implementation is pure Pandas and never starts a Spark session.

## Project layout (operational view)

```
Fraud Detection/
├── data/raw/          # place the seven source CSVs here (git-ignored)
├── output/            # generated CSVs land here (git-ignored)
├── src/referral_pipeline/   # package: main, profiling, preparation, rules
├── tests/             # pytest suite — fixture-based, no real CSVs
├── Dockerfile / .dockerignore
├── pyproject.toml     # deps + `referral-pipeline` console script
└── requirements*.txt  # thin wrappers around editable installs
```

## Local setup (PowerShell)

```powershell
cd "C:\Users\dell\Desktop\New folder (4)\Fraud Detection"

# 1. Create and activate a virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 2. Install the project (editable) with dev extras
pip install -r requirements-dev.txt
# runtime only: pip install -r requirements.txt
```

> If activation is blocked by policy:
> `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, then retry.

## Inputs

All seven documented extracts must exist in the data directory
(default `data/raw/`):

- `user_referrals.csv` — referral events (hub table)
- `user_referral_logs.csv` — referral details; one row per report row
- `user_logs.csv` — member records (log; latest per user kept)
- `user_referral_statuses.csv` — status lookup
- `referral_rewards.csv` — reward definitions
- `paid_transactions.csv` — transaction verification
- `lead_logs.csv` — lead records (joined for "Lead" referrals only)

Source CSVs are **not** version-controlled and never ship in the
Docker image. The repo has no example data — see `data/README.md`.

## Running the CLI

```powershell
referral-pipeline --data-dir data\raw --output-dir output
```

Equivalent module form:

```powershell
python -m referral_pipeline.main --data-dir data\raw --output-dir output
```

| Flag | Default | Purpose |
|---|---|---|
| `--data-dir` | `data/raw` | Directory containing the seven CSVs. |
| `--output-dir` | `output` | Where generated CSVs are written. |
| `--report-name` | `referral_report.csv` | Final report filename. |

**Exit codes:** `0` on success; `1` when inputs are missing or the
prepared dataset cannot be produced.

## Running tests

```powershell
python -m pytest tests/ -v
```

The suite builds synthetic seven-CSV fixtures in temp directories —
no real extracts needed.

## Docker

### Build

```powershell
docker build -t referral-fraud .
```

The image (`python:3.11-slim`) installs the package plus pandas and
pyspark. Source CSVs are **not** baked in — `.dockerignore` excludes
`data/`, `output/`, `*.csv`, virtual environments, git metadata and
credential files.

### Run (PowerShell)

```powershell
docker run --rm `
  -v "${PWD}/data/raw:/data:ro" `
  -v "${PWD}/output:/output" `
  referral-fraud
```

bash equivalent:

```bash
docker run --rm \
  -v "$PWD/data/raw:/data:ro" \
  -v "$PWD/output:/output" \
  referral-fraud
```

| Mount | Container path | Mode | Purpose |
|---|---|---|---|
| `data/raw` | `/data` | **read-only** | Seven input CSVs. |
| `output` | `/output` | read-write | All generated reports — written to the **host**, never stored in the container. |

The default `CMD` passes `--data-dir /data --output-dir /output`.
Append flags after the image name to override, e.g.
`referral-fraud --report-name alt_report.csv`. Forwarded flags replace
the whole `CMD`, so repeat `--data-dir`/`--output-dir` if overriding.

## Outputs

All files land in `--output-dir` (or the host's `output/` in Docker):

| File | Contents |
|---|---|
| `profile_summary.csv` | Flattened profiling: per-file, per-column row counts, dtypes, missing counts/%, distinct counts, duplicates. |
| `referral_details_prepared.csv` | Full cleaned + joined detail dataset, including traceability columns (`reward_value`, `reward_type`, timezone attribution, UTC instants). |
| `referral_report.csv` | The assignment's 22-column report including `num_reward_days` and `is_business_logic_valid`. |

## Expected behavior without inputs

- **Any of the seven CSVs absent** → the CLI logs
  `Missing required input CSV(s) in <dir>: <names>` and exits `1`
  **before writing anything**. In Docker the same message appears in
  `docker run` console output (e.g. when the `/data` mount is
  forgotten).
- **Required tables unreadable/structurally invalid**
  (`user_referrals`, `user_referral_logs`) → errors logged, no
  prepared dataset, exit `1`.
- **Referral-log id problems** (null or conflicting duplicate ids) or
  **join fan-out** → build aborts, exit `1`, no report written. Exact
  duplicate rows are dropped and logged instead.
- **Empty optional data** (e.g. no leads) → run succeeds; affected
  columns are null in the output.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Missing required input CSV(s)` | Wrong `--data-dir`, missing mount, or filename mismatch (names are case-sensitive) | Point at the directory containing all seven files; verify `/data` is mounted. |
| `referral-pipeline` not recognized | venv not activated or install skipped | `.\.venv\Scripts\Activate.ps1`, then `pip install -r requirements-dev.txt`. Use the `python -m` form as a fallback. |
| `ModuleNotFoundError: referral_pipeline` | Not installed / running outside the project | Install editable (`pip install -e .`) or set `$env:PYTHONPATH = "src"` before the `python -m` command. |
| Exit `1` with join-key or duplicate errors | Source data violates documented cardinalities | Inspect the logged table/key; fix the extract upstream — the pipeline aborts rather than guessing. |
| Docker run exits `1` instantly | `/data` mount missing or empty | Re-run with `-v "${PWD}/data/raw:/data:ro"`. |
| No files in host `output/` | `/output` not mounted or run failed | Add the mount; check console log for the error. |
| Weird timestamps in report | Missing/unrecognised timezone values in source rows | Check the log count of timezone fallbacks — affected rows keep UTC clock time. |
| `docker: permission denied` writing output | Host `output/` permissions | Ensure the directory exists and your user can write it. |

## Maintenance notes

- **Never put credentials or real CSVs** in the repo or image;
  `.gitignore` and `.dockerignore` both exclude them.
- **46-row expectation** comes from the real data — the pipeline
  preserves the actual log count and does not pad or trim rows.
- The scoring logic lives in `src/referral_pipeline/rules.py`;
  assumption documentation lives in `rules.py`'s docstring,
  `README.md`, and `docs/business-data-dictionary.md`.
