"""Part 4: score each prepared referral detail with
``is_business_logic_valid`` per the Stage 0 brief.

A detail is valid when it satisfies *either* valid condition AND no
invalid condition — invalid conditions always override valid ones.
Rows satisfying neither valid condition are **invalid by policy** (the
brief defines validity through the two valid conditions; anything else
is not a known-good reward). This policy, and the ambiguities below,
are also documented in the README.

Null semantics: a null input can never satisfy a condition
(mask(False) applied throughout), with two deliberate exceptions —
"no reward value" and "no transaction_id" are defined *by* the null.
Notably a null ``referral_status`` counts as "not Berhasil" for
Invalid condition 1 (a positive reward attached to an unknown status
is suspect) but never satisfies the "Berhasil"/"Menunggu"/
"Tidak Berhasil" requirements of the valid conditions.

Chronology: ``referral_at``/``transaction_at`` in the prepared dataset
are tz-naive *local* times under potentially different timezones and
are never compared directly. "Transaction after referral" (valid 1.6 /
invalid 3.4 / invalid 5) compares the preserved UTC instants
``referral_at_utc``/``transaction_at_utc``. The "same month" rule
(valid 1.7) compares the UTC calendar year-month — a transaction may
fall in a different *local* month than the referral when timezones
differ near a boundary; needs confirmation.

Ambiguities:
- "no reward value" is read as ``reward_value`` null **or 0** —
  Invalid condition 4 explicitly treats 0 as absent, and the same
  reading is applied to valid condition 2 / invalid condition 3.
- "the referral has a transaction ID" uses
  ``user_referrals.transaction_id`` — the column the ERD joins
  ``paid_transactions`` on.
- "the referrer's membership has not expired" compares the (already
  localised) ``referrer_membership_expired_date`` against the local
  ``referral_at`` — same attributed timezone. A null expiry or null
  referral timestamp means "cannot confirm unexpired" and fails the
  check. Whether expiry should be evaluated at referral time or at
  report time needs confirmation.
- Boundary precision: "occurred after" is strict (``>``); an instant
  equal to ``referral_at`` satisfies neither side of the chronology
  rules.
- ``num_reward_days`` is populated from ``referral_rewards.reward_value``
  — adopted assumption, not stated by the brief (``reward_value`` is
  the only reward magnitude in the schema). ``reward_type`` remains
  internal-only for traceability. Needs confirmation.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

LOGGER = logging.getLogger("referral_pipeline.rules")

# Brief's report column list plus the computed flag. ``num_reward_days``
# is populated from ``reward_value`` (adopted assumption, see above).
REPORT_COLUMNS: tuple[str, ...] = (
    "referral_details_id",
    "referral_id",
    "referral_source",
    "referral_source_category",
    "referral_at",
    "referrer_id",
    "referrer_name",
    "referrer_phone_number",
    "referrer_homeclub",
    "referee_id",
    "referee_name",
    "referee_phone",
    "referral_status",
    "num_reward_days",
    "transaction_id",
    "transaction_status",
    "transaction_at",
    "transaction_location",
    "transaction_type",
    "updated_at",
    "reward_granted_at",
    "is_business_logic_valid",
)

_STATUS_BERHASIL = "berhasil"
_STATUS_PENDING_FAILED = ("menunggu", "tidak berhasil")


def _mask(condition: pd.Series) -> pd.Series:
    """Boolean mask where null comparisons count as False."""
    return condition.astype("boolean").fillna(False)


def apply_business_rules(prepared: pd.DataFrame) -> pd.DataFrame:
    """Add ``is_business_logic_valid`` (non-nullable bool) to a copy of
    the prepared dataset. Invalid conditions override valid ones; a row
    satisfying neither valid condition is invalid."""
    frame = prepared.copy()

    # Case-insensitive text comparisons (initcap already applied, but
    # the rules must not depend on input casing).
    status = frame["referral_status"].str.lower()
    txn_status = frame["transaction_status"].str.lower()
    txn_type = frame["transaction_type"].str.lower()

    # reward_value may arrive as object dtype when the rewards table
    # was absent; normalise to numeric so comparisons are NA-safe.
    reward = pd.to_numeric(frame["reward_value"], errors="coerce")
    reward_gt0 = _mask(reward > 0)
    no_reward = reward.isna() | _mask(reward == 0)

    has_txn_id = frame["transaction_id"].notna()
    txn_paid = _mask(txn_status == "paid")
    txn_new = _mask(txn_type == "new")

    # Chronology on preserved UTC instants (never on local clocks).
    # Normalise to UTC-aware datetimes with coercion: absent or
    # malformed values become NaT, so the masks below evaluate to
    # False instead of raising on dtype/awareness mismatches.
    ref_utc = pd.to_datetime(
        frame["referral_at_utc"], errors="coerce", utc=True
    )
    txn_utc = pd.to_datetime(
        frame["transaction_at_utc"], errors="coerce", utc=True
    )
    txn_after = _mask(txn_utc > ref_utc)
    txn_before = _mask(txn_utc < ref_utc)
    same_month = _mask(
        (txn_utc.dt.year == ref_utc.dt.year)
        & (txn_utc.dt.month == ref_utc.dt.month)
    )

    membership_ok = _mask(
        frame["referrer_membership_expired_date"] > frame["referral_at"]
    )
    not_deleted = _mask(frame["referrer_is_deleted"] == False)  # noqa: E712
    reward_granted = _mask(
        frame["is_reward_granted"] == True  # noqa: E712
    )

    # ---- Valid conditions -------------------------------------------
    valid1 = (
        reward_gt0
        & _mask(status == _STATUS_BERHASIL)
        & has_txn_id
        & txn_paid
        & txn_new
        & txn_after
        & same_month
        & membership_ok
        & not_deleted
        & reward_granted
    )
    valid2 = _mask(status.isin(_STATUS_PENDING_FAILED)) & no_reward

    # ---- Invalid conditions (override) -------------------------------
    invalid = {
        "reward_gt0_and_not_berhasil": reward_gt0
        & _mask(status != _STATUS_BERHASIL),
        "reward_gt0_and_no_transaction_id": reward_gt0 & ~has_txn_id,
        "no_reward_but_paid_transaction_after_referral": (
            no_reward & has_txn_id & txn_paid & txn_after
        ),
        "berhasil_without_reward_value": (
            _mask(status == _STATUS_BERHASIL) & no_reward
        ),
        "transaction_before_referral": txn_before,
    }
    for name, mask in invalid.items():
        count = int(mask.sum())
        if count:
            LOGGER.info("invalid condition %-45s hits %d row(s)", name, count)

    any_invalid = pd.concat(list(invalid.values()), axis=1).any(axis=1)
    any_valid = valid1 | valid2
    LOGGER.info(
        "business rules: %d valid-1, %d valid-2, %d invalid-any, "
        "%d neither-valid",
        int(valid1.sum()),
        int(valid2.sum()),
        int(any_invalid.sum()),
        int((~any_valid).sum()),
    )

    frame["is_business_logic_valid"] = (
        (any_valid & ~any_invalid).astype(bool)
    )

    # Report field assumption: num_reward_days is populated from
    # referral_rewards.reward_value (the only reward magnitude in the
    # schema); null propagates. reward_type stays internal-only.
    frame["num_reward_days"] = frame["reward_value"]
    return frame


def build_report(scored: pd.DataFrame) -> pd.DataFrame:
    """Project the scored dataset to the brief's report columns,
    including ``num_reward_days`` (populated from ``reward_value`` —
    adopted assumption documented in README)."""
    missing = [c for c in REPORT_COLUMNS if c not in scored.columns]
    if missing:
        raise KeyError(f"report columns missing from scored data: {missing}")
    return scored.loc[:, list(REPORT_COLUMNS)]


def write_report(
    report: pd.DataFrame, output_dir: Path, filename: str
) -> Path:
    """Write the final report CSV into output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / filename
    report.to_csv(out_path, index=False)
    LOGGER.info("Wrote referral report to %s", out_path)
    return out_path
