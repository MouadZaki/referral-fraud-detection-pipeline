# Business Data Dictionary — Referral Fraud Detection

Audience: anyone who needs to understand the data — no engineering
background required (e.g. a Marketing Manager reviewing the fraud
report).

This document describes:

1. The seven **source tables** the pipeline receives as CSV extracts.
2. How those tables **relate** to each other.
3. Every column in the **output report** the pipeline produces.
4. **Assumptions and open questions** — places where the assignment
   left a meaning undefined and a decision had to be made.

> The real source extracts have not been supplied to this project, so
> this dictionary intentionally contains **no observed values, row
> counts, or data-quality statistics**. Descriptions come from the
> Stage 0 assignment's own table definitions.

## Reading notes

- **Types** use business-friendly names: *whole number*, *text*,
  *date-time*, *yes/no*. The assignment's technical types are shown in
  brackets for reference.
- **Timestamps**: every timestamp in the source data is recorded in
  **UTC**. The pipeline converts report timestamps to **local time**
  using the timezone recorded for the relevant club, transaction or
  lead, as the assignment instructs.
- **"Referral detail"** vs "referral": one referral can produce more
  than one log row in `user_referral_logs` (e.g. a status change over
  time). Each log row is a *referral detail* and becomes exactly one
  report row — nothing is merged or dropped to reach a target count.

---

## Source tables

### 1. `user_referrals` — the hub table

One row per **referral event**. Every other table connects to this
one.

| Field | Type | Meaning |
|---|---|---|
| `referral_id` | text (string) | Unique identifier for the referral. |
| `referral_at` | date-time (timestamp) | When the referral was created (UTC). |
| `referrer_id` | text (string) | ID of the existing member who made the referral. |
| `referee_id` | text (string) | ID of the new user — only used when `referral_source` is "Lead". |
| `referee_name` | text (string) | Name of the new user. |
| `referee_phone` | text (string) | Phone number of the new user. |
| `referral_source` | text (string) | How the referral arrived. Documented values: "User Sign Up", "Draft Transaction", "Lead". |
| `referral_reward_id` | whole number (integer) | ID of the reward attached to the referral (links to `referral_rewards`). |
| `transaction_id` | text (string) | ID of the transaction linked to the referral (links to `paid_transactions`). |
| `updated_at` | date-time (timestamp) | When the referral record was last updated (UTC). |
| `user_referral_status_id` | whole number (integer) | ID of the referral's status (links to `user_referral_statuses`). |

### 2. `user_referral_logs` — referral details

Additional details about each referral — the transaction that resulted
and whether the reward was granted. This table may hold **several rows
per referral** (it is a log); each row is one *referral detail* and
becomes one report row.

| Field | Type | Meaning |
|---|---|---|
| `id` | whole number (integer) | Unique log ID — becomes `referral_details_id` in the report. |
| `user_referral_id` | text (string) | Links back to `user_referrals.referral_id`. |
| `source_transaction_id` | text (string) | ID of the transaction that resulted from the referral. |
| `created_at` | date-time (timestamp) | When this log entry was created (UTC). |
| `is_reward_granted` | yes/no (bool) | Whether the reward has been granted. |

### 3. `user_logs` — member information

Details about users — both referrers and referees. Also a log: **a
user can appear more than once**, so the pipeline keeps the most
recent record per user (see Assumptions).

| Field | Type | Meaning |
|---|---|---|
| `id` | whole number (integer) | Unique log ID. |
| `user_id` | text (string) | ID of the user. |
| `name` | text (string) | Name of the user. |
| `phone_number` | text (string) | Phone number of the user. |
| `homeclub` | text (string) | The user's home club. Kept in its original capitalisation. |
| `timezone_homeclub` | text (string) | The user's timezone, used to convert referral timestamps to local time. |
| `membership_expired_date` | date-time (timestamp) | When the user's membership expires. |
| `is_deleted` | yes/no (bool) | Whether the user's account has been deleted. |

### 4. `user_referral_statuses` — status lookup

The meaning of each referral status code.

| Field | Type | Meaning |
|---|---|---|
| `id` | whole number (integer) | Unique identifier for the status. |
| `description` | text (string) | Status text. Documented values: "Berhasil" (Successful), "Menunggu" (Pending), "Tidak Berhasil" (Failed). |
| `created_at` | date-time (timestamp) | When the status record was created (UTC). |

### 5. `referral_rewards` — reward definitions

The reward attached to a referral.

| Field | Type | Meaning |
|---|---|---|
| `id` | whole number (integer) | Unique identifier for the reward. |
| `reward_value` | whole number (integer) | Value of the reward. |
| `reward_type` | whole number (integer) | Type of reward (e.g. monetary, points). Kept internally for traceability; not in the report. |
| `created_at` | date-time (timestamp) | When the reward was created (UTC). |

### 6. `paid_transactions` — transaction verification

Payment transactions used to verify a referral actually produced
business.

| Field | Type | Meaning |
|---|---|---|
| `transaction_id` | text (string) | Unique ID of the transaction. |
| `transaction_status` | text (string) | Status of the transaction (e.g. "PAID"). |
| `transaction_at` | date-time (timestamp) | When the transaction occurred (UTC). |
| `transaction_location` | text (string) | Where the transaction happened. Kept in its original capitalisation. |
| `timezone_transaction` | text (string) | Timezone of the transaction, used for local-time conversion. |
| `transaction_type` | text (string) | Type of transaction (e.g. "NEW"). |

### 7. `lead_logs` — lead information

Marketing leads. Only joined to a referral when its
`referral_source` is "Lead".

| Field | Type | Meaning |
|---|---|---|
| `id` | whole number (integer) | Unique log ID. |
| `lead_id` | text (string) | ID of the lead. |
| `source_category` | text (string) | Category of the lead source (feeds `referral_source_category`). |
| `created_at` | date-time (timestamp) | When the lead record was created (UTC). |
| `preferred_location` | text (string) | The lead's preferred club/location. |
| `timezone_location` | text (string) | Timezone of the preferred location. |
| `current_status` | text (string) | Current status of the lead. |

## Relationships and grain

`user_referrals` is the hub — every other table joins to it (per the
assignment's reference ERD). The pipeline implements the documented
cardinalities and aborts rather than guessing when they are violated:

| From `user_referrals` | Joins to | Documented cardinality | Implemented behaviour |
|---|---|---|---|
| `referral_id` | `user_referral_logs.user_referral_id` | 1 → many | Every distinct log row is preserved as its own referral detail. |
| `referrer_id` | `user_logs.user_id` | 1 → many (log) | Latest record per user kept; see Assumptions for ordering. |
| `referee_id` | `lead_logs.lead_id` | 1 → many (log) | Joined **only** when `referral_source` = "Lead"; latest record kept. |
| `referral_reward_id` | `referral_rewards.id` | many → 1 | Key must be unique; conflicting rows sharing a key abort the run. |
| `transaction_id` | `paid_transactions.transaction_id` | many → 1 | Same uniqueness guarantee. |
| `user_referral_status_id` | `user_referral_statuses.id` | many → 1 | Same uniqueness guarantee. |

Additional guarantees:

- **One row per referral detail.** `user_referral_logs.id` must be
  non-null and unique after exact duplicates are removed; otherwise
  the run aborts rather than producing a corrupt report.
- **Null keys never match.** A missing join value never pairs with
  another missing value.
- **No fan-out.** If a join ever produced more or fewer rows than the
  de-duplicated log count, the run aborts instead of writing an
  inflated dataset.

## Output report fields

`referral_report.csv` — one row per referral detail. The assignment
states the correct report has **46 rows**; the pipeline preserves
whatever the real log data contains rather than padding to a number.

| # | Column | Type | Meaning and notes |
|---|---|---|---|
| 1 | `referral_details_id` | whole number (INTEGER) | Identifier of this referral-detail row — taken from `user_referral_logs.id`. Not the same as `referral_id`. |
| 2 | `referral_id` | text (STRING) | The referral this detail belongs to (`user_referrals.referral_id`). |
| 3 | `referral_source` | text (STRING) | How the referral arrived: "User Sign Up", "Draft Transaction" or "Lead". |
| 4 | `referral_source_category` | text (STRING) | Derived grouping: "Online" for User Sign Up, "Offline" for Draft Transaction, the lead's own `source_category` for Lead. |
| 5 | `referral_at` | date-time (DATETIME) | When the referral was created — **local time** (converted from UTC using the referrer's club timezone). |
| 6 | `referrer_id` | text (STRING) | Member ID of the person who referred. |
| 7 | `referrer_name` | text (STRING) | Referrer's name (from the latest `user_logs` record). |
| 8 | `referrer_phone_number` | text (STRING) | Referrer's phone number. |
| 9 | `referrer_homeclub` | text (STRING) | Referrer's home club, original capitalisation preserved. |
| 10 | `referee_id` | text (STRING) | New user's ID from the referral record — used to join `lead_logs` when the source is "Lead"; may be empty otherwise. |
| 11 | `referee_name` | text (STRING) | New user's name. |
| 12 | `referee_phone` | text (STRING) | New user's phone number. |
| 13 | `referral_status` | text (STRING) | Referral status text: "Berhasil", "Menunggu" or "Tidak Berhasil". |
| 14 | `num_reward_days` | whole number (INT) | Reward amount attached to the referral — **populated from `referral_rewards.reward_value`**, an adopted assumption (see Assumptions). |
| 15 | `transaction_id` | text (STRING) | Payment transaction linked to the referral; empty when none exists. |
| 16 | `transaction_status` | text (STRING) | Transaction status (e.g. "Paid"). |
| 17 | `transaction_at` | date-time (DATETIME) | When the transaction occurred — local time via `timezone_transaction`. |
| 18 | `transaction_location` | text (STRING) | Where the transaction happened, original capitalisation preserved. |
| 19 | `transaction_type` | text (STRING) | Transaction type (e.g. "New"). |
| 20 | `updated_at` | date-time (DATETIME) | When the referral record was last updated — local time. |
| 21 | `reward_granted_at` | date-time (DATETIME) | When the reward was granted — see Assumptions; empty when `is_reward_granted` is not true. |
| 22 | `is_business_logic_valid` | yes/no (BOOLEAN) | The fraud flag. TRUE when a documented *valid* condition holds and **no** invalid condition holds; FALSE otherwise. |

### How `is_business_logic_valid` is decided (plain language)

- **Valid — reward confirmed:** the reward value is greater than 0,
  the status is "Berhasil", a transaction exists, is PAID and NEW,
  happened after and in the same month as the referral, the referrer's
  membership is unexpired and not deleted, and the reward was granted.
- **Valid — pending or failed:** the status is "Menunggu" or
  "Tidak Berhasil" and no reward value is assigned.
- **Invalid overrides valid:** any invalid condition (reward attached
  but status not "Berhasil"; reward without a transaction; a paid
  transaction after referral with no reward; "Berhasil" with no or
  zero reward; a transaction dated before the referral) forces FALSE
  even if a valid condition also matched.
- **Anything else is invalid:** rows matching neither valid condition
  are flagged FALSE — the assignment only defines what *is* valid.

## Assumptions and open questions

The assignment leaves several points undefined. Each is also flagged
in `preparation.py` / `rules.py` and the README; all need
confirmation before sign-off.

- **`num_reward_days`** is populated from `referral_rewards.reward_value`.
  The report spec lists the column but never says where it comes from;
  `reward_value` is the only reward magnitude in the schema
  (sample value 30 fits "days"). `reward_type` stays internal-only.
- **`reward_granted_at`** has no documented source column. Assumed to
  be the referral-log `created_at` (local time) of the row where
  `is_reward_granted` is true; empty otherwise.
- **"Latest" log records:** `lead_logs` repeated `lead_id`s are
  ordered by `created_at` (`id` breaks ties) — the table has a
  timestamp. `user_logs` has no record timestamp
  (`membership_expired_date` is a business date), so the greatest `id`
  is assumed most recent.
- **Timezone attribution:** referral timestamps have no timezone of
  their own; the referrer's `timezone_homeclub` is used. Reward/status
  timestamps reuse the timezone obtained via the joins. Missing or
  unrecognised timezones keep UTC clock time and are counted in the
  log.
- **Chronology and "same month":** local clocks live in different
  timezones, so comparisons use preserved UTC instants; "same month"
  means same UTC calendar month. "Occurred after" is strict `>` — an
  identical timestamp satisfies neither direction.
- **"No reward value"** means `reward_value` empty **or 0** (invalid
  condition 4 explicitly treats 0 as absent; the same reading is
  applied everywhere).
- **Membership expiry** is evaluated at referral time (local
  `referral_at`); whether report-time was intended needs confirmation.
  Empty expiry fails the "unexpired" check.
- **Empty status** counts as "not Berhasil" for invalid condition 1
  but never satisfies a valid condition's status requirement.
- **`source_transaction_id`** (from the log) is kept internally for
  traceability; whether it equals `user_referrals.transaction_id`
  needs confirmation.
