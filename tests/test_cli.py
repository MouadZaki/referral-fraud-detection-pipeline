"""CLI exit-code contract: 0 on success, nonzero when required inputs
are missing or the prepared dataset cannot be produced — and no
prepared CSV is ever written on failure."""

from __future__ import annotations

from referral_pipeline import preparation
from referral_pipeline.main import main


def test_cli_success_returns_zero(dataset_dir, tmp_path):
    out_dir = tmp_path / "out"

    exit_code = main(
        ["--data-dir", str(dataset_dir), "--output-dir", str(out_dir)]
    )

    assert exit_code == 0
    assert (out_dir / "referral_details_prepared.csv").is_file()
    assert (out_dir / "profile_summary.csv").is_file()


def test_cli_missing_inputs_returns_nonzero(tmp_path):
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    out_dir = tmp_path / "out"

    exit_code = main(
        ["--data-dir", str(empty_dir), "--output-dir", str(out_dir)]
    )

    assert exit_code == 1
    # Required tables are absent: no prepared dataset may be produced.
    assert not (out_dir / "referral_details_prepared.csv").exists()


def test_cli_fanout_returns_nonzero_and_writes_nothing(
    monkeypatch, dataset_dir, tmp_path, rewrite
):
    """A grain violation must surface as a nonzero exit and must not
    leave a prepared CSV behind.

    Unique-key validation is bypassed via monkeypatch so the failure
    reaches the post-join grain check.
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
    out_dir = tmp_path / "out"

    exit_code = main(
        ["--data-dir", str(dataset_dir), "--output-dir", str(out_dir)]
    )

    assert exit_code == 1
    assert not (out_dir / "referral_details_prepared.csv").exists()
