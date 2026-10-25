# Referral Fraud Detection

Python pipeline for the Stage 0 take-home brief: it profiles seven
referral-program CSV extracts, joins and cleans them, applies business
rules to flag valid or potentially fraudulent rewards, and produces a
CSV report containing an `is_business_logic_valid` flag.

> **Status: scaffold only.** Profiling, joins, business rules, the
> report, and Docker support are added in later milestones. The current
> entry point parses CLI arguments and reports which source CSVs are
> present or missing.

## Project layout

```
Fraud Detection/
├── data/
│   ├── README.md            # expected inputs (tracked)
│   └── raw/                 # source CSV extracts go here (ignored)
├── output/                  # generated reports land here (ignored)
├── src/
│   └── referral_pipeline/
│       ├── __init__.py
│       └── main.py          # CLI entry point
├── .gitignore
├── pyproject.toml
├── requirements.txt         # editable install + runtime deps
├── requirements-dev.txt     # editable install + dev deps
└── README.md
```

## Prerequisites

- Python 3.10 or newer
- pip 21.3+ (required for the editable install)

## Setup

Windows (PowerShell):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements-dev.txt
```

macOS / Linux:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements-dev.txt
```

For a runtime-only install, use `requirements.txt` instead of
`requirements-dev.txt`.

## Running

```bash
referral-pipeline --data-dir data/raw --output-dir output
```

or equivalently:

```bash
python -m referral_pipeline.main --data-dir data/raw --output-dir output
```

Both flags are optional and default to `data/raw` and `output`.
`--report-name` overrides the report filename (default
`referral_report.csv`).

## Source data

The seven CSV extracts are **not** included in version control. Place
them in `data/raw/` — see `data/README.md` for the expected filenames.
Generated reports under `output/` and credential files (`.env`, keys,
certificates) are likewise git-ignored by default.
