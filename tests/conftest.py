"""Shared fixtures: a minimal, structurally valid copy of the seven
documented source CSVs, written into a pytest ``tmp_path``.

Two referral-detail rows (log ids 101, 102): one 'Lead' referral and
one 'User Sign Up' referral — enough to exercise every join, the
category mapping, and the grain checks. Tests overwrite individual
files via the ``rewrite`` fixture to create failure scenarios.
"""

from __future__ import annotations

from pathlib import Path

import pytest

BASELINE: dict[str, str] = {
    "user_referrals.csv": (
        "referral_id,referral_at,referrer_id,referee_id,referee_name,"
        "referee_phone,referral_source,referral_reward_id,transaction_id,"
        "updated_at,user_referral_status_id\n"
        "r1,2023-06-15 07:35:00,u1,l1,jhon doe,987-654-3210,Lead,1,t1,"
        "2023-06-21 05:00:00,1\n"
        "r2,2023-06-16 07:35:00,u2,,SUE SMITH,111,User Sign Up,2,t2,"
        "2023-06-22 05:00:00,2\n"
    ),
    "user_referral_logs.csv": (
        "id,user_referral_id,source_transaction_id,created_at,"
        "is_reward_granted\n"
        "101,r1,t1,2023-06-30 07:00:00,true\n"
        "102,r2,t2,2023-07-01 07:00:00,false\n"
    ),
    "user_logs.csv": (
        "id,user_id,name,phone_number,homeclub,timezone_homeclub,"
        "membership_expired_date,is_deleted\n"
        "6,u1,JOHN DOE,123-456-7890,PERMATA HIJAU,Asia/Jakarta,"
        "2026-01-01 00:00:00,false\n"
        "7,u2,sue smith,222,KUNINGAN,Asia/Jakarta,"
        "2025-01-01 00:00:00,false\n"
    ),
    "user_referral_statuses.csv": (
        "id,description,created_at\n"
        "1,berhasil,2023-01-01 00:00:00\n"
        "2,menunggu,2023-01-01 00:00:00\n"
    ),
    "referral_rewards.csv": (
        "id,reward_value,reward_type,created_at\n"
        "1,30,1,2023-01-01 00:00:00\n"
        "2,0,1,2023-01-01 00:00:00\n"
    ),
    "paid_transactions.csv": (
        "transaction_id,transaction_status,transaction_at,"
        "transaction_location,timezone_transaction,transaction_type\n"
        "t1,paid,2023-06-20 03:00:00,BENHIL,Asia/Jakarta,new\n"
    ),
    "lead_logs.csv": (
        "id,lead_id,source_category,created_at,preferred_location,"
        "timezone_location,current_status\n"
        "9,l1,offline,2023-06-10 00:00:00,BENHIL,Asia/Jakarta,contacted\n"
    ),
}


@pytest.fixture
def dataset_dir(tmp_path: Path) -> Path:
    """A temp directory containing all seven valid source CSVs."""
    for name, content in BASELINE.items():
        (tmp_path / name).write_text(content, encoding="utf-8")
    return tmp_path


@pytest.fixture
def rewrite(dataset_dir: Path):
    """Return a helper that overwrites one source CSV in dataset_dir."""

    def _rewrite(filename: str, content: str) -> None:
        (dataset_dir / filename).write_text(content, encoding="utf-8")

    return _rewrite
