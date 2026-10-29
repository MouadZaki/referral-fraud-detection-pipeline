"""Command-line entry point for the referral fraud-detection pipeline.

Implemented stages:

- profiling of the seven source CSV extracts (availability, row counts,
  columns, dtypes, missing values, distinct counts, duplicates), logged
  to the console and written to ``<output-dir>/profile_summary.csv``;
- preparation of the referral-detail dataset: cleaning, type coercion,
  UTC -> local timestamps, documented joins, deduplication,
  ``referral_source_category`` - written to
  ``<output-dir>/referral_details_prepared.csv``;
- business-rule scoring: ``is_business_logic_valid`` computed per
  referral detail (invalid conditions override valid ones) and the
  report written to ``<output-dir>/<report-name>``.

Docker support is provided by the repository Dockerfile; final
documentation deliverables are added in later milestones.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from referral_pipeline.preparation import (
    build_referral_details,
    write_prepared_details,
)
from referral_pipeline.rules import (
    apply_business_rules,
    build_report,
    write_report,
)
from referral_pipeline.profiling import (
    EXPECTED_CSVS,
    log_profiles,
    profile_csvs,
    write_profile_summary,
)

LOGGER = logging.getLogger("referral_pipeline")

DEFAULT_DATA_DIR = Path("data") / "raw"
DEFAULT_OUTPUT_DIR = Path("output")
DEFAULT_REPORT_NAME = "referral_report.csv"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="referral-pipeline",
        description=(
            "Profile seven referral-program CSV extracts and build the "
            "prepared referral-detail dataset (joins, cleaning, UTC->local "
            "timestamps, referral_source_category)."
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


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    # Fail clearly before doing any work when documented inputs are
    # absent (e.g. a Docker run without the data bind mount).
    missing = [
        name
        for name in EXPECTED_CSVS
        if not (args.data_dir / name).is_file()
    ]
    if missing:
        LOGGER.error(
            "Missing required input CSV(s) in %s: %s. Place all seven "
            "documented extracts there (see data/README.md) or point "
            "--data-dir at the directory containing them.",
            args.data_dir,
            ", ".join(missing),
        )
        return 1

    LOGGER.info("Profiling source CSVs in %s", args.data_dir)
    profiles = profile_csvs(args.data_dir)
    log_profiles(profiles)
    write_profile_summary(profiles, args.output_dir)

    LOGGER.info("Building prepared referral-detail dataset")
    prepared = build_referral_details(args.data_dir)
    if prepared is None:
        LOGGER.error(
            "Prepared dataset not produced - see missing/unreadable "
            "input errors above."
        )
        return 1
    write_prepared_details(prepared, args.output_dir)

    LOGGER.info("Applying business rules")
    scored = apply_business_rules(prepared)
    write_report(build_report(scored), args.output_dir, args.report_name)
    LOGGER.info(
        "Done: %d referral-detail rows scored, report written to %s",
        len(scored),
        args.output_dir / args.report_name,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
