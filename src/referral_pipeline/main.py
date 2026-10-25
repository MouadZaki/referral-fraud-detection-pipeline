"""Command-line entry point for the referral fraud-detection pipeline.

This is a scaffold only. Later milestones add CSV profiling, data
preparation and joins, business-rule validation, and the reward report
containing the ``is_business_logic_valid`` flag.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

LOGGER = logging.getLogger("referral_pipeline")

DEFAULT_DATA_DIR = Path("data") / "raw"
DEFAULT_OUTPUT_DIR = Path("output")
DEFAULT_REPORT_NAME = "referral_report.csv"

EXPECTED_CSVS = (
    "user_referrals.csv",
    "user_referral_logs.csv",
    "user_logs.csv",
    "user_referral_statuses.csv",
    "referral_rewards.csv",
    "paid_transactions.csv",
    "lead_logs.csv",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="referral-pipeline",
        description=(
            "Profile seven referral-program CSV extracts, join and clean "
            "them, apply business rules, and write a reward report."
        ),
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Directory containing the source CSV extracts (default: %(default)s).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where the report will be written (default: %(default)s).",
    )
    parser.add_argument(
        "--report-name",
        default=DEFAULT_REPORT_NAME,
        help="Filename of the generated report (default: %(default)s).",
    )
    return parser.parse_args(argv)


def check_source_files(data_dir: Path) -> tuple[list[str], list[str]]:
    """Split EXPECTED_CSVS into (found, missing) based on data_dir contents."""
    found = [name for name in EXPECTED_CSVS if (data_dir / name).is_file()]
    missing = [name for name in EXPECTED_CSVS if name not in found]
    return found, missing


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    LOGGER.info("Looking for source CSVs in %s", args.data_dir)
    found, missing = check_source_files(args.data_dir)
    for name in found:
        LOGGER.info("  found    %s", name)
    for name in missing:
        LOGGER.warning("  missing  %s", name)

    if missing:
        LOGGER.warning(
            "%d of %d expected CSVs are absent from %s "
            "(see data/README.md for the expected inputs).",
            len(missing),
            len(EXPECTED_CSVS),
            args.data_dir,
        )

    LOGGER.info(
        "Scaffold only: profiling, joins, business rules, and report "
        "generation (%s) are implemented in later milestones.",
        args.output_dir / args.report_name,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
