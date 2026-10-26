"""Focused tests for ``apply_business_rules`` — one per brief rule plus
the important overlaps.

Each test builds a single-row prepared frame directly (no CSVs) from a
valid-by-construction baseline, then flips one field. ``valid`` marks
the scenario the row is *designed* to satisfy when unmodified.
"""

from __future__ import annotations

import pandas as pd
import pytest

from referral_pipeline.rules import apply_business_rules, build_report

UTC = "UTC"


def _frame(**overrides) -> pd.DataFrame:
    """One prepared row satisfying Valid condition 1 by default."""
    row = {
        "referral_details_id": 101,
        "referral_id": "r1",
        # Report-only attributes (inert to scoring, required by
        # build_report's column validation).
        "referral_source": "User Sign Up",
        "referral_source_category": "Online",
        "referrer_id": "u1",
        "referrer_name": "John Doe",
        "referrer_phone_number": "123-456-7890",
        "referrer_homeclub": "PERMATA HIJAU",
        "referee_id": "f1",
        "referee_name": "Jhon Doe",
        "referee_phone": "987-654-3210",
        "transaction_location": "BENHIL",
        # Rule inputs.
        "referral_status": "Berhasil",
        "reward_value": 30,
        "transaction_id": "t1",
        "transaction_status": "Paid",
        "transaction_type": "New",
        "is_reward_granted": True,
        "referrer_is_deleted": False,
        "referral_at": pd.Timestamp("2023-06-15 14:35:00"),
        "referral_at_utc": pd.Timestamp("2023-06-15 07:35:00", tz=UTC),
        "transaction_at": pd.Timestamp("2023-06-20 10:00:00"),
        "transaction_at_utc": pd.Timestamp("2023-06-20 03:00:00", tz=UTC),
        "referrer_membership_expired_date": pd.Timestamp("2024-01-01"),
        "updated_at": pd.Timestamp("2023-06-21 12:00:00"),
        "reward_granted_at": pd.Timestamp("2023-06-30 14:00:00"),
    }
    row.update(overrides)
    return pd.DataFrame([row])


def _flag(frame: pd.DataFrame) -> bool:
    scored = apply_business_rules(frame)
    return bool(scored["is_business_logic_valid"].iloc[0])


# ---------------------------------------------------------------------
# Valid condition 1 — every sub-condition required
# ---------------------------------------------------------------------


def test_valid_condition_1_fully_satisfied():
    assert _flag(_frame()) is True


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("reward_value", 0, "reward not > 0"),
        ("referral_status", "Menunggu", "status not Berhasil"),
        ("transaction_id", None, "no transaction id"),
        ("transaction_status", "Failed", "not PAID"),
        ("transaction_type", "Renewal", "not NEW"),
        (
            "transaction_at_utc",
            pd.Timestamp("2023-06-14 00:00:00", tz=UTC),
            "transaction before referral",
        ),
        (
            "transaction_at_utc",
            pd.Timestamp("2023-07-01 03:00:00", tz=UTC),
            "different UTC month",
        ),
        (
            "referrer_membership_expired_date",
            pd.Timestamp("2023-01-01"),
            "membership expired before referral",
        ),
        ("referrer_is_deleted", True, "account deleted"),
        ("is_reward_granted", False, "reward not granted"),
    ],
)
def test_valid_condition_1_fails_when_any_part_missing(field, value, reason):
    assert _flag(_frame(**{field: value})) is False, reason


def test_case_insensitive_status_and_transaction_text():
    assert _flag(
        _frame(
            referral_status="BERHASIL",
            transaction_status="paid",
            transaction_type="NEW",
        )
    ) is True


def test_utc_chronology_used_not_naive_local_clocks():
    """Local clocks may appear inverted; the UTC instants decide.

    referral_at local 22:00 > transaction_at local 10:00 would wrongly
    read as 'transaction before referral'; in UTC the transaction is
    genuinely after the referral and the row stays valid.
    """
    assert _flag(
        _frame(
            referral_at=pd.Timestamp("2023-06-19 22:00:00"),
            referral_at_utc=pd.Timestamp("2023-06-19 15:00:00", tz=UTC),
            transaction_at=pd.Timestamp("2023-06-19 10:00:00"),
            transaction_at_utc=pd.Timestamp("2023-06-19 17:00:00", tz=UTC),
        )
    ) is True


def test_same_month_evaluated_on_utc_calendar():
    """UTC July vs referral's UTC June: different months -> not valid,
    even though both local dates display in June."""
    assert _flag(
        _frame(
            referral_at_utc=pd.Timestamp("2023-06-30 20:00:00", tz=UTC),
            transaction_at_utc=pd.Timestamp("2023-07-01 01:00:00", tz=UTC),
        )
    ) is False


def test_equal_timestamps_satisfy_neither_side():
    """txn_at == referral_at is neither 'after' (valid 1.6) nor
    'before' (invalid 5): valid1 fails at the boundary."""
    instant = pd.Timestamp("2023-06-15 07:35:00", tz=UTC)
    assert _flag(_frame(transaction_at_utc=instant)) is False


# ---------------------------------------------------------------------
# Valid condition 2
# ---------------------------------------------------------------------


@pytest.mark.parametrize("status", ["Menunggu", "Tidak Berhasil"])
def test_valid_condition_2_pending_or_failed_no_reward(status):
    assert _flag(
        _frame(
            referral_status=status,
            reward_value=None,
            transaction_id=None,
            transaction_status=None,
            transaction_type=None,
            transaction_at=None,
            transaction_at_utc=pd.NaT,
            is_reward_granted=False,
        )
    ) is True


def test_valid_condition_2_zero_reward_counts_as_no_reward():
    assert _flag(
        _frame(
            referral_status="Menunggu",
            reward_value=0,
            transaction_id=None,
            transaction_status=None,
            transaction_type=None,
            transaction_at_utc=pd.NaT,
            is_reward_granted=False,
        )
    ) is True


def test_valid_condition_2_fails_when_reward_assigned():
    """Menunggu with a positive reward is not valid-2 — and is caught
    by Invalid condition 1 (reward > 0 and not Berhasil)."""
    assert _flag(
        _frame(referral_status="Menunggu", transaction_status=None)
    ) is False


# ---------------------------------------------------------------------
# Invalid conditions — each must override on its own
# ---------------------------------------------------------------------


def test_invalid_1_positive_reward_non_berhasil():
    assert _flag(_frame(referral_status="Tidak Berhasil")) is False


def test_invalid_1_null_status_counts_as_not_berhasil():
    """A positive reward attached to an unknown status is suspect."""
    assert _flag(_frame(referral_status=None)) is False


def test_invalid_2_positive_reward_no_transaction_id():
    assert _flag(_frame(transaction_id=None)) is False


def test_invalid_3_no_reward_but_paid_transaction_after_referral():
    assert _flag(
        _frame(
            referral_status="Menunggu",
            reward_value=None,
            is_reward_granted=False,
        )
    ) is False


def test_invalid_4_berhasil_with_null_or_zero_reward():
    assert _flag(_frame(reward_value=None)) is False
    assert _flag(_frame(reward_value=0)) is False


def test_invalid_5_transaction_before_referral():
    assert _flag(
        _frame(
            transaction_at_utc=pd.Timestamp("2023-06-14 00:00:00", tz=UTC)
        )
    ) is False


def test_invalid_5_fires_even_on_valid2_shape():
    """A pending no-reward row still loses if the transaction ran
    before the referral — invalid overrides valid."""
    assert _flag(
        _frame(
            referral_status="Menunggu",
            reward_value=None,
            is_reward_granted=False,
            transaction_at_utc=pd.Timestamp("2023-06-14 00:00:00", tz=UTC),
        )
    ) is False


# ---------------------------------------------------------------------
# Neither-valid policy and overlaps
# ---------------------------------------------------------------------


def test_neither_valid_condition_is_invalid():
    """Berhasil + positive reward but a non-PAID transaction matches
    neither valid condition -> invalid by policy (and invalid-3 is
    not even needed since the reward exists)."""
    assert _flag(_frame(transaction_status="Pending")) is False


def test_null_inputs_never_satisfy_conditions():
    """Missing optional data cannot produce a valid flag."""
    assert _flag(
        _frame(
            referral_status=None,
            reward_value=None,
            transaction_id=None,
            transaction_status=None,
            transaction_type=None,
            transaction_at_utc=pd.NaT,
            referrer_is_deleted=None,
            is_reward_granted=None,
            referrer_membership_expired_date=pd.NaT,
        )
    ) is False


def test_flag_is_non_nullable_bool():
    scored = apply_business_rules(_frame())
    assert scored["is_business_logic_valid"].dtype == bool
    assert scored["is_business_logic_valid"].notna().all()


def test_report_includes_num_reward_days_from_reward_value():
    """num_reward_days is populated from referral_rewards.reward_value
    (adopted assumption) and appears in the report projection."""
    scored = apply_business_rules(_frame(reward_value=30))
    report = build_report(scored)

    assert "num_reward_days" in report.columns
    assert report["num_reward_days"].iloc[0] == 30
