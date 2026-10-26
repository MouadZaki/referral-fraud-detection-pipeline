"""Tests for the grain and key-integrity guarantees in
``referral_pipeline.preparation``.

Each test starts from the valid baseline fixture and corrupts exactly
one property, so a failure points at the guard that let it through.
"""

from __future__ import annotations

import pandas as pd

from referral_pipeline import preparation
from referral_pipeline.preparation import build_referral_details

LOGS_WITH_NULL_ID = (
    "id,user_referral_id,source_transaction_id,created_at,"
    "is_reward_granted\n"
    "101,r1,t1,2023-06-30 07:00:00,true\n"
    ",r2,t2,2023-07-01 07:00:00,false\n"
)

LOGS_WITH_CONFLICTING_ID = (
    "id,user_referral_id,source_transaction_id,created_at,"
    "is_reward_granted\n"
    "101,r1,t1,2023-06-30 07:00:00,true\n"
    "101,r1,t9,2023-07-02 07:00:00,false\n"
)

REFERRALS_WITH_NULL_REWARD_KEY = (
    "referral_id,referral_at,referrer_id,referee_id,referee_name,"
    "referee_phone,referral_source,referral_reward_id,transaction_id,"
    "updated_at,user_referral_status_id\n"
    "r1,2023-06-15 07:35:00,u1,l1,jhon doe,987-654-3210,Lead,1,t1,"
    "2023-06-21 05:00:00,1\n"
    "r2,2023-06-16 07:35:00,u2,,SUE SMITH,111,User Sign Up,,t2,"
    "2023-06-22 05:00:00,2\n"
)

REWARDS_WITH_NULL_ID = (
    "id,reward_value,reward_type,created_at\n"
    "1,30,1,2023-01-01 00:00:00\n"
    ",777,1,2023-01-01 00:00:00\n"
)


def test_null_referral_details_id_aborts(rewrite, dataset_dir):
    """A null user_referral_logs.id can never become a valid
    referral_details_id, so the build must abort."""
    rewrite("user_referral_logs.csv", LOGS_WITH_NULL_ID)

    assert build_referral_details(dataset_dir) is None


def test_conflicting_duplicate_id_aborts(rewrite, dataset_dir):
    """The same log id on differing rows is an integrity conflict."""
    rewrite("user_referral_logs.csv", LOGS_WITH_CONFLICTING_ID)

    assert build_referral_details(dataset_dir) is None


def test_null_join_keys_never_match(rewrite, dataset_dir):
    """Nulls on either side of a join key must not pair with each other.

    Referral r2 has a null referral_reward_id and a reward row exists
    with a null id carrying reward_value 777. If null keys were allowed
    to match, detail row 102 would pick up that bogus value.
    """
    rewrite("user_referrals.csv", REFERRALS_WITH_NULL_REWARD_KEY)
    rewrite("referral_rewards.csv", REWARDS_WITH_NULL_ID)

    prepared = build_referral_details(dataset_dir)
    assert prepared is not None
    assert len(prepared) == 2

    row = prepared.loc[prepared["referral_details_id"] == 102].iloc[0]
    assert pd.isna(row["reward_id"])
    assert pd.isna(row["reward_value"])
    # And the valid join still works: r1's reward_id=1 -> value 30.
    valid = prepared.loc[prepared["referral_details_id"] == 101].iloc[0]
    assert valid["reward_value"] == 30


def test_join_fanout_aborts(monkeypatch, dataset_dir, rewrite):
    """If a join ever fanned out despite validation, the grain check
    must abort rather than return an inflated dataset.

    Unique-key validation would normally stop this earlier; it is
    bypassed here via monkeypatch so the downstream grain check itself
    is exercised.
    """
    rewrite(
        "user_referrals.csv",
        "referral_id,referral_at,referrer_id,referee_id,referee_name,"
        "referee_phone,referral_source,referral_reward_id,transaction_id,"
        "updated_at,user_referral_status_id\n"
        "r1,2023-06-15 07:35:00,u1,l1,jhon doe,987,Lead,1,t1,"
        "2023-06-21 05:00:00,1\n"
        "r1,2023-06-15 07:35:00,u1,l1,jhon doe,988,Lead,1,t1,"
        "2023-06-21 05:00:00,1\n"
        "r2,2023-06-16 07:35:00,u2,,SUE SMITH,111,User Sign Up,2,t2,"
        "2023-06-22 05:00:00,2\n",
    )
    monkeypatch.setattr(
        preparation, "_validate_unique_key",
        lambda frame, key, label: frame,
    )

    assert build_referral_details(dataset_dir) is None
