# Referral Fraud Detection

Python pipeline for the Stage 0 take-home brief: it profiles seven
referral-program CSV extracts, joins and cleans them, applies business
rules to flag valid or potentially fraudulent rewards, and produces a
CSV report containing an `is_business_logic_valid` flag.

> **Status: profiling + preparation + scoring implemented.** The entry
> point profiles the seven source CSVs, builds the prepared
> referral-detail dataset, scores each row with
> `is_business_logic_valid` (invalid conditions override valid ones),
> and writes the report. Docker support and the final documentation
> deliverables are added in later milestones.

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
│       ├── main.py          # CLI entry point
│       ├── profiling.py     # source CSV profiling
│       ├── preparation.py   # cleaning, joins, prepared referral details
│       └── rules.py         # is_business_logic_valid + report
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

Each run logs a per-table profile — availability, row count, duplicate
rows, per-column dtype/missing-value stats, and distinct value counts —
and writes a flattened summary to `output/profile_summary.csv`. Missing
or unreadable files are logged as warnings/errors and are never filled
with invented data.

It then builds the prepared referral-detail dataset and writes it to
`output/referral_details_prepared.csv`:

- **Grain:** one row per `user_referral_logs` record; its `id` becomes
  `referral_details_id`. One-to-many log rows are preserved — nothing
  is collapsed or dropped to reach the brief's 46-row report figure.
- **Joins:** `user_referral_logs.user_referral_id -> user_referrals.referral_id`,
  then `referrer_id -> user_logs.user_id`, `referee_id -> lead_logs.lead_id`
  (Lead referrals only), `referral_reward_id -> referral_rewards.id`,
  `transaction_id -> paid_transactions.transaction_id`, and
  `user_referral_status_id -> user_referral_statuses.id`.
- **Timestamps:** all timestamp columns are read as UTC and converted
  to local time per row using the documented timezone columns
  (`timezone_homeclub`, `timezone_transaction`, `timezone_location`);
  tables without a timezone borrow one via the joins (see assumptions).
- **Cleaning:** whitespace trimmed, documented columns coerced to
  datetime/Int64/boolean, initcap applied to descriptive strings except
  club/location names, remaining nulls reported rather than silently
  dropped.
- **Grain protection:** every many->1 join key (`referral_id`, status
  `id`, reward `id`, `transaction_id`) is validated unique before
  merging. Exact duplicate rows are dropped (logged); rows sharing a
  key but differing elsewhere **abort the build** rather than pick a
  row. In `user_referral_logs`, exact duplicate rows are dropped and a
  duplicated `id` on differing rows aborts; repeated `user_referral_id`
  values are legitimate 1->many log events and are preserved. Repeated
  `user_id`/`lead_id` records collapse to the latest per key **after
  logging the counts** — `lead_logs` by its documented `created_at`
  (`id` tiebreak), `user_logs` by greatest `id` (see assumptions).
- **`referral_source_category`:** `User Sign Up -> Online`,
  `Draft Transaction -> Offline`, `Lead -> lead_logs.source_category`.

It then scores every detail row with `is_business_logic_valid` per the
brief's valid/invalid conditions and writes `output/referral_report.csv`
with the brief's column list. Rules semantics:

- **Valid if** either valid condition holds AND no invalid condition
  holds — invalid conditions always override valid ones.
- **Neither-valid rows are invalid** (policy decision): the brief
  defines validity only through the two valid conditions.
- **Case-insensitive** status/transaction text comparisons
  (`berhasil`, `paid`, `new`, ...).
- **Chronology on UTC instants:** `referral_at`/`transaction_at` are
  local times under different timezones and are never compared
  directly; `referral_at_utc`/`transaction_at_utc` (preserved during
  preparation) drive "transaction after/before referral" and the
  same-month check.
- **Nulls never satisfy a condition**, except where null *is* the
  condition ("no reward value", "no transaction id").

If a required input (`user_referrals`, `user_referral_logs`) is absent
or structurally invalid, the dataset is not produced and the error is
logged — no partial or fabricated output. The CLI exits nonzero in
that case.

## Assumptions and open questions

Points where the Stage 0 brief is ambiguous; every one is also flagged
in `preparation.py`:

- **`reward_granted_at`:** no documented source column maps to it.
  Assumed to be the `user_referral_logs.created_at` (local time) of the
  log row where `is_reward_granted` is true; null otherwise. Needs
  confirmation.
- **`num_reward_days`:** populated from `referral_rewards.reward_value`
  — an adopted assumption, not stated by the brief (it's the only
  reward magnitude in the schema). `reward_value` and `reward_type`
  remain in the prepared dataset for traceability; `reward_type` stays
  internal-only and does not appear in the report. Needs confirmation.
- **Repeated log rows:** `user_referral_logs` is 1->many per referral.
  Each distinct log row stays one referral-detail row; only exact
  duplicate rows are dropped and conflicting duplicate `id`s abort the
  build. If a referral legitimately has several log rows the report
  will have more than one row for it; the 46-row figure only holds if
  the source data has exactly 46 distinct log rows.
- **"Latest" log record:** `lead_logs` orders repeated `lead_id`s by
  its documented `created_at` (`id` breaks ties). `user_logs` has no
  record timestamp (`membership_expired_date` is a business date), so
  the greatest `id` is *assumed* to be the most recent record. If ids
  are not issued in insertion order this picks the wrong record —
  needs confirmation.
- **Timezone attribution:** `user_referrals`, `user_referral_logs`,
  and the joined `user_referral_statuses`/`referral_rewards`
  `created_at` values have no timezone column; they are converted
  with the *referrer's* `timezone_homeclub` per the brief's "join
  with another table" instruction. `user_logs`, `lead_logs`, and
  `paid_transactions` use their own timezone columns. Rows whose
  timezone is missing or unrecognised keep UTC clock time and are
  counted in the log. Whether the referrer's — not the referee's —
  timezone is correct needs confirmation.
- **Transaction join:** `paid_transactions` joins via
  `user_referrals.transaction_id` per the ERD. The log table's
  `source_transaction_id` is kept verbatim in the prepared output;
  whether it is the same transaction needs confirmation.
- **Initcap exemption:** applied to descriptive strings except
  `referrer_homeclub`; `transaction_location` is treated as a
  club-style code and also left unchanged.
- **"No reward value":** read as `reward_value` null **or 0** —
  invalid condition 4 explicitly treats 0 as absent, and the same
  reading is applied to valid condition 2 / invalid condition 3.
- **"Transaction ID":** `user_referrals.transaction_id` — the column
  the ERD joins `paid_transactions` on.
- **Same-month rule:** compared on the **UTC** calendar year-month of
  the preserved instants; a transaction can share the referral's local
  month but fall in a different UTC month (or vice-versa) near
  boundaries — needs confirmation.
- **"Occurred after":** strict `>` on UTC instants; a transaction at
  the exact `referral_at` instant satisfies neither side of the
  chronology rules.
- **Membership expiry:** compared against local `referral_at`
  (same attributed timezone); a null expiry or referral timestamp
  fails the check as "cannot confirm unexpired". Whether expiry
  should be evaluated at referral time or report time needs
  confirmation.
- **Null status:** counts as "not Berhasil" for invalid condition 1
  (positive reward + unknown status is suspect) but never satisfies
  the valid conditions' status requirements.

## Source data

The seven CSV extracts are **not** included in version control. Place
them in `data/raw/` — see `data/README.md` for the expected filenames.
Generated reports under `output/` and credential files (`.env`, keys,
certificates) are likewise git-ignored by default.
