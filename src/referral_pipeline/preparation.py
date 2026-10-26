"""Part 3: load, clean, join, and prepare the referral-detail dataset.

Implements the Stage 0 brief's "Script development" steps 1 and 2:

- load the seven source CSVs into DataFrames,
- clean (type coercion, whitespace, missing-value normalisation),
- convert every timestamp from UTC to local time using the timezone
  column on each table — joining across tables when a table has no
  timezone of its own,
- join the tables on the documented keys,
- remove duplicates, apply initcap to descriptive strings (club names
  exempt), and derive ``referral_source_category``.

Grain contract: one output row per ``user_referral_logs`` row —
``user_referral_logs.id`` becomes ``referral_details_id``. Log tables
joined 1→many (``user_logs``, ``lead_logs``) are collapsed to their
most recent record per key **after logging how many records exist per
key**; one-to-many rows are never silently dropped, and no rows are
removed to force the brief's expected 46-row count. Every many→1 join
key (``referral_id``, status ``id``, reward ``id``, ``transaction_id``)
is validated unique before merging: exact duplicate rows are dropped
and logged, rows sharing a key but differing elsewhere abort the build
rather than corrupt the grain.

The brief's "null values should not exist" step is deferred to review:
this stage preserves rows and *reports* remaining nulls rather than
dropping whole referral-detail rows, so no referral detail is lost
before the business rules of Part 4 see it.

Known ambiguities flagged for confirmation (also in README):
- ``reward_granted_at``: no column in any documented table maps to it
  directly. Assumed to be the referral-log ``created_at`` on rows where
  ``is_reward_granted`` is true.
- ``num_reward_days``: no matching column exists; the only reward
  value in the schema is ``referral_rewards.reward_value`` (integer).
  The prepared dataset carries ``reward_value``/``reward_type``
  verbatim; whether ``num_reward_days`` is that same value renamed
  needs confirmation.
- ``user_referral_logs`` is 1→many per referral. Only *exact*
  duplicate rows are dropped; every distinct log row is kept and
  becomes one referral-detail row (a duplicated ``id`` on differing
  rows is an integrity conflict and aborts the build). The 46-row
  report figure only holds if the source data contains exactly 46
  distinct log rows.
- "Latest log record" ordering: ``lead_logs`` dedupes by its
  documented ``created_at`` (``id`` tiebreak). ``user_logs`` documents
  no record timestamp (``membership_expired_date`` is a business date,
  not a log timestamp), so the greatest ``id`` is *assumed* most
  recent — needs confirmation.
- Timezone attribution for tables without their own timezone column is
  an assumption (see ``_to_local`` calls): ``user_referrals``,
  ``user_referral_logs``, and the joined ``statuses``/``rewards``
  ``created_at`` values all use the *referrer's* ``timezone_homeclub``;
  ``user_logs``, ``lead_logs``, and ``paid_transactions`` use their own
  timezone columns. Rows whose timezone is missing or invalid keep UTC
  clock time and are counted in the log.
- ``paid_transactions`` is joined via ``user_referrals.transaction_id``
  per the ERD. ``user_referral_logs.source_transaction_id`` is kept
  under that name; whether it is the same transaction needs
  confirmation.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import zoneinfo

from referral_pipeline.profiling import EXPECTED_CSVS

LOGGER = logging.getLogger("referral_pipeline.preparation")

PREPARED_DETAILS_NAME = "referral_details_prepared.csv"

# ---------------------------------------------------------------------------
# Column-type contracts, taken from the Stage 0 brief's table structures.
# Files are read as text first and coerced below so a bad value in one
# field never aborts the load — coercion failures are logged instead.
# ---------------------------------------------------------------------------

TIMESTAMP_COLUMNS: dict[str, tuple[str, ...]] = {
    "user_referrals.csv": ("referral_at", "updated_at"),
    "user_referral_logs.csv": ("created_at",),
    "user_logs.csv": ("membership_expired_date",),
    "user_referral_statuses.csv": ("created_at",),
    "referral_rewards.csv": ("created_at",),
    "paid_transactions.csv": ("transaction_at",),
    "lead_logs.csv": ("created_at",),
}

INTEGER_COLUMNS: dict[str, tuple[str, ...]] = {
    "user_referrals.csv": ("referral_reward_id", "user_referral_status_id"),
    "user_referral_logs.csv": ("id",),
    "user_logs.csv": ("id",),
    "user_referral_statuses.csv": ("id",),
    "referral_rewards.csv": ("id", "reward_value", "reward_type"),
    "lead_logs.csv": ("id",),
}

BOOLEAN_COLUMNS: dict[str, tuple[str, ...]] = {
    "user_referral_logs.csv": ("is_reward_granted",),
    "user_logs.csv": ("is_deleted",),
}

# Columns on each table whose timezone column converts the timestamps.
# None = the table documents no timezone; conversion happens after the
# join via another table's timezone (see _to_local call sites).
LOCAL_TIMEZONE_COLUMNS: dict[str, str] = {
    "user_logs.csv": "timezone_homeclub",
    "paid_transactions.csv": "timezone_transaction",
    "lead_logs.csv": "timezone_location",
}

# Descriptive strings that get initcap. Club/location names are excluded
# per the brief ("except the club name") — transaction_location is a
# club-style location code so it is exempt too (assumption flagged).
INITCAP_COLUMNS: tuple[str, ...] = (
    "referee_name",
    "referral_source",
    "referrer_name",
    "referral_status",
    "transaction_status",
    "transaction_type",
    "lead_source_category",
    "referral_source_category",
)

# Output column order. All brief report fields are present except
# ``num_reward_days`` (no source column — see module docstring) and
# ``is_business_logic_valid`` (Part 4). Source columns kept for
# traceability follow the report columns.
PREPARED_COLUMNS: tuple[str, ...] = (
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
    "transaction_id",
    "transaction_status",
    "transaction_at",
    "transaction_location",
    "transaction_type",
    "updated_at",
    "reward_granted_at",
    # Traceability / Part-4 inputs (not in the report column list).
    "reward_id",
    "reward_value",
    "reward_type",
    "reward_created_at",
    "status_created_at",
    "is_reward_granted",
    "source_transaction_id",
    "referrer_timezone_homeclub",
    "transaction_timezone",
    "lead_timezone_location",
    # UTC instants preserved for Part 4 chronology checks: the local
    # timestamps above live in different timezones and must not be
    # compared directly.
    "referral_at_utc",
    "transaction_at_utc",
    "referrer_membership_expired_date",
    "referrer_is_deleted",
    "user_referral_status_id",
    "referral_reward_id",
)

_TRUE_VALUES = {"true", "1", "yes", "y", "t"}
_FALSE_VALUES = {"false", "0", "no", "n", "f"}


# ---------------------------------------------------------------------------
# Loading and cleaning
# ---------------------------------------------------------------------------


def _to_bool(series: pd.Series) -> pd.Series:
    """Map common truthy/falsey spellings to pandas boolean; rest -> NA."""
    lowered = series.str.strip().str.lower()
    mapped = lowered.map(
        {**{v: True for v in _TRUE_VALUES}, **{v: False for v in _FALSE_VALUES}}
    )
    unmapped = lowered.notna() & mapped.isna()
    if unmapped.any():
        LOGGER.warning(
            "Unrecognised boolean values left as null: %s",
            sorted(lowered[unmapped].unique().tolist()),
        )
    return mapped.astype("boolean")


def load_csv(path: Path) -> pd.DataFrame | None:
    """Load one CSV as text, strip whitespace, normalise empty strings
    to NA. Returns None when the file is absent or unreadable — the
    caller decides whether the table is required."""
    if not path.is_file():
        LOGGER.warning("%s: not found at %s - table skipped", path.name, path)
        return None
    try:
        frame = pd.read_csv(path, dtype="string")
    except Exception as exc:  # noqa: BLE001 - report, never fabricate
        LOGGER.error("%s: present but unreadable (%s) - table skipped",
                     path.name, exc)
        return None

    frame = frame.map(
        lambda value: value.strip() if pd.notna(value) else value
    ).replace({"": pd.NA})

    for column in INTEGER_COLUMNS.get(path.name, ()):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype(
                "Int64"
            )
    for column in BOOLEAN_COLUMNS.get(path.name, ()):
        if column in frame.columns:
            frame[column] = _to_bool(frame[column])
    for column in TIMESTAMP_COLUMNS.get(path.name, ()):
        if column in frame.columns:
            parsed = pd.to_datetime(
                frame[column], errors="coerce", utc=True, format="mixed"
            )
            unparseable = frame[column].notna() & parsed.isna()
            if unparseable.any():
                LOGGER.warning(
                    "%s: %d unparseable %s values coerced to null "
                    "(e.g. %s)",
                    path.name,
                    int(unparseable.sum()),
                    column,
                    frame.loc[unparseable, column].head(3).tolist(),
                )
            frame[column] = parsed
    return frame


def load_source_tables(data_dir: Path) -> dict[str, pd.DataFrame]:
    """Load all documented CSVs; returns {filename: frame} for those
    present and readable."""
    tables: dict[str, pd.DataFrame] = {}
    for filename in EXPECTED_CSVS:
        frame = load_csv(data_dir / filename)
        if frame is not None:
            tables[filename] = frame
    return tables


# ---------------------------------------------------------------------------
# UTC -> local time
# ---------------------------------------------------------------------------


def _to_local(
    utc_ts: pd.Series, timezone_names: pd.Series | str, label: str
) -> pd.Series:
    """Convert a tz-aware UTC series to tz-naive local time, either via a
    single timezone name or a per-row series of timezone names. Rows
    whose timezone is missing or invalid keep their UTC clock time (the
    timestamp is still recorded, not fabricated) and are counted in the
    log."""
    result = pd.Series(pd.NaT, index=utc_ts.index, dtype="datetime64[ns]")
    valid_rows = utc_ts.notna()
    if isinstance(timezone_names, pd.Series):
        zones = timezone_names
    elif isinstance(timezone_names, str):
        zones = pd.Series(timezone_names, index=utc_ts.index)
    else:
        # No timezone attributable at all (e.g. user_logs unavailable):
        # every row falls back to UTC clock time and is reported below.
        zones = pd.Series(pd.NA, index=utc_ts.index, dtype="object")
    untranslated: list[str] = []
    for zone_name in zones[valid_rows].dropna().unique():
        try:
            zone = zoneinfo.ZoneInfo(str(zone_name))
        except (zoneinfo.ZoneInfoNotFoundError, ValueError, TypeError):
            untranslated.append(str(zone_name))
            continue
        mask = valid_rows & (zones == zone_name)
        result.loc[mask] = utc_ts.loc[mask].dt.tz_convert(zone).dt.tz_localize(
            None
        )
    fallback = valid_rows & result.isna()
    if fallback.any():
        # No usable timezone: keep the UTC clock time so the value is
        # preserved and downstream comparisons still work.
        result.loc[fallback] = utc_ts.loc[fallback].dt.tz_localize(None)
        LOGGER.warning(
            "%s: %d rows kept at UTC clock time - timezone missing or "
            "unrecognised%s",
            label,
            int(fallback.sum()),
            f" ({sorted(set(untranslated))})" if untranslated else "",
        )
    return result


# ---------------------------------------------------------------------------
# Per-table preparation
# ---------------------------------------------------------------------------


def _dedupe_latest(
    frame: pd.DataFrame, key: str, order_by: tuple[str, ...], label: str
) -> pd.DataFrame:
    """Keep the most recent record per key in a log table.

    ``order_by`` columns are sorted ascending so the *last* record per
    key wins; later columns break ties. The number of collapsed records
    is logged, never silent."""
    per_key = frame.groupby(key, dropna=False).size()
    repeated = per_key[per_key > 1]
    if len(repeated):
        LOGGER.info(
            "%s: %d keys have >1 log record (%d records total); keeping "
            "the latest per key",
            label,
            len(repeated),
            int(per_key.sum()),
        )
    ordered = frame.sort_values(list(order_by), na_position="first")
    return ordered.drop_duplicates(subset=key, keep="last")


def _prepare_user_logs(frame: pd.DataFrame) -> pd.DataFrame:
    """One row per user_id, membership_expired_date localised with the
    row's own timezone_homeclub.

    ASSUMPTION (also flagged in README): user_logs documents no
    record/event timestamp — ``membership_expired_date`` is a business
    date, not when the log record was written — so the greatest ``id``
    is treated as the most recent record for a user. If ids are not
    issued in insertion order, this picks the wrong record."""
    frame = _dedupe_latest(frame, "user_id", ("id",), "user_logs")
    frame = frame.copy()
    frame["membership_expired_date"] = _to_local(
        frame["membership_expired_date"],
        frame["timezone_homeclub"],
        "user_logs.membership_expired_date",
    )
    return frame


def _prepare_lead_logs(frame: pd.DataFrame) -> pd.DataFrame:
    """One row per lead_id, created_at localised with the row's own
    timezone_location.

    "Latest" uses the documented ``created_at`` event timestamp (``id``
    only breaks ties). Dedupe runs on the UTC value before localisation
    so records under different timezones order correctly."""
    frame = _dedupe_latest(frame, "lead_id", ("created_at", "id"), "lead_logs")
    frame = frame.copy()
    frame["created_at"] = _to_local(
        frame["created_at"], frame["timezone_location"], "lead_logs.created_at"
    )
    return frame


def _prepare_referral_logs(frame: pd.DataFrame) -> pd.DataFrame | None:
    """Keep every legitimate log event, but enforce the documented
    uniqueness of ``id``.

    - Exact duplicate rows are redundant copies: dropped and logged.
    - A null ``id`` violates the documented key (it becomes
      ``referral_details_id``, so nulls are not allowed): reported,
      then the caller aborts.
    - The same ``id`` on differing rows is an integrity conflict that
      would corrupt the one-row-per-detail grain: reported, then the
      caller aborts rather than pick a row.
    Multiple rows per ``user_referral_id`` are legitimate (1→many per
    the ERD) and are preserved."""
    dupes = int(frame.duplicated().sum())
    if dupes:
        LOGGER.info(
            "user_referral_logs: dropped %d exact duplicate row(s)", dupes
        )
        frame = frame.drop_duplicates()
    null_ids = int(frame["id"].isna().sum())
    if null_ids:
        LOGGER.error(
            "user_referral_logs: %d rows have a null id - the documented "
            "unique log id is violated; aborting.",
            null_ids,
        )
        return None
    id_counts = frame["id"].value_counts()
    conflicts = id_counts[id_counts > 1]
    if len(conflicts):
        LOGGER.error(
            "user_referral_logs: id value(s) %s appear on differing rows "
            "- the documented unique log id is violated; aborting.",
            conflicts.index.tolist()[:5],
        )
        return None
    return frame


def _require_columns(
    frame: pd.DataFrame, columns: tuple[str, ...], label: str
) -> bool:
    """True when every documented column needed downstream is present.
    A structurally invalid table is an error to report, not data to
    guess around."""
    missing = [c for c in columns if c not in frame.columns]
    if missing:
        LOGGER.error("%s: missing required column(s) %s", label, missing)
        return False
    return True


def _drop_null_keys(
    frame: pd.DataFrame, key: str, label: str
) -> pd.DataFrame:
    """Drop rows whose join ``key`` is null on a right-hand table.

    A null key can never match a partner row, so removing it changes no
    output values — it only guarantees the merge cannot pair
    null-with-null regardless of join-backend semantics."""
    dropped = int(frame[key].isna().sum())
    if dropped:
        LOGGER.info(
            "%s: %d rows with null %s cannot join - ignored", label, dropped, key
        )
        frame = frame.loc[frame[key].notna()]
    return frame


def _validate_unique_key(
    frame: pd.DataFrame, key: str, label: str
) -> pd.DataFrame | None:
    """Enforce the documented uniqueness of a many→1 join ``key``.

    Exact duplicate rows are dropped and logged. Rows sharing a key
    value but differing in another column are an integrity conflict:
    there is no defensible row to pick, so this reports them and
    returns ``None`` for the caller to abort. Keys that are null can
    never match in a join and are ignored by this check."""
    dup_mask = frame.duplicated(subset=key, keep=False) & frame[key].notna()
    if not dup_mask.any():
        return frame.drop_duplicates()
    grouped = frame.loc[dup_mask].groupby(key)
    conflicting = [
        value for value, group in grouped if len(group.drop_duplicates()) > 1
    ]
    if conflicting:
        LOGGER.error(
            "%s: key %s has %d value(s) on differing rows (%s) - the "
            "documented uniqueness is violated; aborting rather than "
            "choosing a row arbitrarily.",
            label, key, len(conflicting), conflicting[:5],
        )
        return None
    dropped = int(frame.duplicated().sum())
    LOGGER.info(
        "%s: dropped %d exact duplicate row(s) sharing key %s",
        label, dropped, key,
    )
    return frame.drop_duplicates()


def _prepare_paid_transactions(frame: pd.DataFrame) -> pd.DataFrame:
    """transaction_at localised with the row's own timezone_transaction.
    ``transaction_id`` uniqueness is enforced upstream by
    ``_validate_unique_key``."""
    frame = frame.copy()
    # Keep the UTC instant for chronology comparisons (Part 4).
    frame["transaction_at_utc"] = frame["transaction_at"]
    frame["transaction_at"] = _to_local(
        frame["transaction_at"],
        frame["timezone_transaction"],
        "paid_transactions.transaction_at",
    )
    return frame


# ---------------------------------------------------------------------------
# Join + projection
# ---------------------------------------------------------------------------


def build_referral_details(data_dir: Path) -> pd.DataFrame | None:
    """Build the prepared referral-detail dataset.

    One row per ``user_referral_logs`` record (its ``id`` becomes
    ``referral_details_id``). Returns None when a required table is
    missing — never fabricates a dataset from partial inputs.
    """
    tables = load_source_tables(data_dir)

    required = ("user_referral_logs.csv", "user_referrals.csv")
    missing = [name for name in required if name not in tables]
    if missing:
        LOGGER.error(
            "Cannot build referral details: required input(s) missing: %s",
            ", ".join(missing),
        )
        return None

    logs = tables["user_referral_logs.csv"].copy()
    referrals = tables["user_referrals.csv"].copy()

    # Structural check: the required tables must carry every documented
    # column the joins and derivations rely on.
    structural_required = {
        "user_referral_logs.csv": (
            "id", "user_referral_id", "created_at", "is_reward_granted",
        ),
        "user_referrals.csv": (
            "referral_id", "referral_source", "referrer_id", "referee_id",
            "referral_at", "updated_at", "referee_name", "referee_phone",
            "transaction_id", "referral_reward_id", "user_referral_status_id",
        ),
    }
    checks = [
        _require_columns(tables[name], cols, name)
        for name, cols in structural_required.items()
    ]
    if not all(checks):
        LOGGER.error("Required table(s) failed structural checks - aborting.")
        return None

    # Keep legitimate repeated log events; drop exact copies; abort on
    # conflicting duplicate log ids.
    logs = _prepare_referral_logs(logs)
    if logs is None:
        return None

    LOGGER.info(
        "Grain: %d referral-detail rows from user_referral_logs", len(logs)
    )

    # -- Optional tables -------------------------------------------------
    # Missing documented columns in an optional table -> log the error
    # and treat the table as unavailable rather than crash mid-merge.
    optional_required: dict[str, tuple[str, ...]] = {
        "user_logs.csv": (
            "id", "user_id", "timezone_homeclub", "membership_expired_date",
        ),
        "user_referral_statuses.csv": ("id", "description"),
        "referral_rewards.csv": ("id", "reward_value", "reward_type"),
        "paid_transactions.csv": (
            "transaction_id", "transaction_at", "timezone_transaction",
        ),
        "lead_logs.csv": (
            "id", "lead_id", "created_at", "timezone_location",
            "source_category", "current_status",
        ),
    }

    def _optional(name: str) -> pd.DataFrame | None:
        frame = tables.get(name)
        if frame is not None and not _require_columns(
            frame, optional_required[name], name
        ):
            LOGGER.error("%s: structural check failed - table ignored", name)
            return None
        return frame

    statuses = _optional("user_referral_statuses.csv")
    rewards = _optional("referral_rewards.csv")
    users = _optional("user_logs.csv")
    transactions = _optional("paid_transactions.csv")
    leads = _optional("lead_logs.csv")

    # -- Unique-key enforcement ------------------------------------------
    # A duplicated key on a many→1 join target would fan the merge out
    # and silently break the one-row-per-detail grain. Exact copies are
    # dropped; conflicting rows sharing a key abort the build.
    unique_specs = (
        ("user_referrals.csv", referrals, "referral_id"),
        ("user_referral_statuses.csv", statuses, "id"),
        ("referral_rewards.csv", rewards, "id"),
        ("paid_transactions.csv", transactions, "transaction_id"),
    )
    checked: dict[str, pd.DataFrame] = {}
    for label, frame, key in unique_specs:
        if frame is None:
            continue
        result = _validate_unique_key(frame, key, label)
        if result is None:
            return None
        checked[label] = result
    referrals = checked["user_referrals.csv"]
    statuses = checked.get("user_referral_statuses.csv")
    rewards = checked.get("referral_rewards.csv")
    transactions = checked.get("paid_transactions.csv")

    # Null keys on the left side legitimately stay unmatched (joined
    # columns end up null); on the right side they are dropped so no
    # merge can ever pair null-with-null.
    referrals = _drop_null_keys(referrals, "referral_id", "user_referrals")
    if statuses is not None:
        statuses = _drop_null_keys(
            statuses, "id", "user_referral_statuses"
        )
    if rewards is not None:
        rewards = _drop_null_keys(rewards, "id", "referral_rewards")
    if transactions is not None:
        transactions = _drop_null_keys(
            transactions, "transaction_id", "paid_transactions"
        )

    if users is not None:
        users = _prepare_user_logs(users).rename(
            columns={
                "user_id": "referrer_id",
                "name": "referrer_name",
                "phone_number": "referrer_phone_number",
                "homeclub": "referrer_homeclub",
                "timezone_homeclub": "referrer_timezone_homeclub",
                "membership_expired_date": "referrer_membership_expired_date",
                "is_deleted": "referrer_is_deleted",
                "id": "referrer_user_log_id",
            }
        )
    if leads is not None:
        leads = _prepare_lead_logs(leads).rename(
            columns={
                "id": "lead_row_id",
                "source_category": "lead_source_category",
                "created_at": "lead_created_at",
                "timezone_location": "lead_timezone_location",
                "current_status": "lead_current_status",
            }
        )
        leads = _drop_null_keys(leads, "lead_id", "lead_logs")
    if transactions is not None:
        transactions = _prepare_paid_transactions(transactions).rename(
            columns={"timezone_transaction": "transaction_timezone"}
        )
    if statuses is not None:
        statuses = statuses.rename(
            columns={
                "id": "status_id",
                "description": "referral_status",
                "created_at": "status_created_at",
            }
        )
    if rewards is not None:
        rewards = rewards.rename(
            columns={"id": "reward_id", "created_at": "reward_created_at"}
        )

    # -- Joins on the documented keys ------------------------------------
    # referral_details -> user_referrals (many -> 1). The log row's own
    # id becomes the report's referral_details_id.
    details = logs.merge(
        referrals, how="left", left_on="user_referral_id", right_on="referral_id"
    ).rename(columns={"id": "referral_details_id"})
    # Preserve the UTC instant before referral_at is localised below.
    details["referral_at_utc"] = details["referral_at"]

    # -> referrer attributes via user_logs (latest record per user)
    if users is not None:
        users = _drop_null_keys(users, "referrer_id", "user_logs")
        details = details.merge(users, how="left", on="referrer_id")
        # Localise referral timestamps using the referrer's homeclub
        # timezone (assumption: referral events follow the referrer's
        # local time; user_referrals carries no timezone of its own).
        for column in ("referral_at", "updated_at"):
            details[column] = _to_local(
                details[column], details["referrer_timezone_homeclub"],
                f"user_referrals.{column}",
            )
        # Referral-log created_at uses the same attributed timezone.
        details["created_at_local"] = _to_local(
            details["created_at"], details["referrer_timezone_homeclub"],
            "user_referral_logs.created_at",
        )
    else:
        LOGGER.warning(
            "user_logs.csv unavailable: referrer attributes, timezone "
            "conversion of referral timestamps, and membership/deletion "
            "flags will be null."
        )
        for column in ("referral_at", "updated_at"):
            details[column] = _to_local(
                details[column], pd.NA, f"user_referrals.{column}"
            )
        details["created_at_local"] = _to_local(
            details["created_at"], pd.NA, "user_referral_logs.created_at"
        )

    # -> referral status description
    if statuses is not None:
        details = details.merge(
            statuses[["status_id", "referral_status", "status_created_at"]],
            how="left",
            left_on="user_referral_status_id",
            right_on="status_id",
        ).drop(columns="status_id")
    else:
        for column in ("referral_status", "status_created_at"):
            details[column] = pd.NA

    # -> reward attributes
    if rewards is not None:
        details = details.merge(
            rewards[
                ["reward_id", "reward_value", "reward_type",
                 "reward_created_at"]
            ],
            how="left",
            left_on="referral_reward_id",
            right_on="reward_id",
        )
    else:
        for column in (
            "reward_id", "reward_value", "reward_type", "reward_created_at",
        ):
            details[column] = pd.NA

    # -> transaction attributes via user_referrals.transaction_id
    if transactions is not None:
        details = details.merge(
            transactions, how="left", on="transaction_id"
        )
    else:
        for column in (
            "transaction_status", "transaction_at", "transaction_location",
            "transaction_type", "transaction_timezone", "transaction_at_utc",
        ):
            details[column] = pd.NA

    # -> lead attributes, ONLY when referral_source == 'Lead' per ERD.
    #    For other sources the lead fields stay null even if referee_id
    #    coincidentally matches a lead_id.
    if leads is not None:
        lead_rows = details["referral_source"].str.strip().str.lower() == "lead"
        joined = details.loc[lead_rows].merge(
            leads, how="left", left_on="referee_id", right_on="lead_id"
        )
        for column in (
            "lead_source_category", "lead_timezone_location",
            "lead_current_status", "lead_created_at", "lead_row_id",
        ):
            details[column] = pd.NA
        details.loc[lead_rows, [
            "lead_source_category", "lead_timezone_location",
            "lead_current_status", "lead_created_at", "lead_row_id",
        ]] = joined[[
            "lead_source_category", "lead_timezone_location",
            "lead_current_status", "lead_created_at", "lead_row_id",
        ]].to_numpy()
    else:
        for column in (
            "lead_source_category", "lead_timezone_location",
            "lead_current_status", "lead_created_at", "lead_row_id",
        ):
            details[column] = pd.NA

    # Dimension timestamps joined in via the referral context get the
    # same attributed timezone (referrer's homeclub) as the other
    # referral-level timestamps — same documented assumption.
    zones = (
        details["referrer_timezone_homeclub"]
        if "referrer_timezone_homeclub" in details.columns
        else pd.NA
    )
    for column in ("status_created_at", "reward_created_at"):
        if column in details.columns:
            details[column] = _to_local(details[column], zones, column)

    # -- Derived fields --------------------------------------------------
    # For 'Lead' referrals lead_logs carries no name/phone columns, so
    # referee_name/referee_phone come from user_referrals for all rows.

    # reward_granted_at assumption: log created_at where reward granted.
    granted = details["is_reward_granted"] == True  # noqa: E712 (nullable bool)
    details["reward_granted_at"] = details["created_at_local"].where(granted)

    # referral_source_category per the brief's CASE.
    source = details["referral_source"].str.strip().str.lower()
    details["referral_source_category"] = pd.Series(pd.NA, index=details.index, dtype="string")
    details.loc[source == "user sign up", "referral_source_category"] = "Online"
    details.loc[source == "draft transaction", "referral_source_category"] = "Offline"
    lead_mask = source == "lead"
    details.loc[lead_mask, "referral_source_category"] = details.loc[
        lead_mask, "lead_source_category"
    ]
    uncategorised = details["referral_source"].notna() & details[
        "referral_source_category"
    ].isna()
    if uncategorised.any():
        LOGGER.warning(
            "referral_source_category is null for %d rows: %s",
            int(uncategorised.sum()),
            details.loc[uncategorised, "referral_source"].unique().tolist(),
        )

    # -- initcap on descriptive strings (club/location names exempt) -----
    for column in INITCAP_COLUMNS:
        if column in details.columns:
            details[column] = (
                details[column]
                .astype("string")
                .str.title()
                .astype("string")
            )

    # -- Projection & integrity report -----------------------------------
    for column in PREPARED_COLUMNS:
        if column not in details.columns:
            details[column] = pd.NA
    prepared = details.loc[:, list(PREPARED_COLUMNS)].copy()
    prepared = prepared.astype({"referral_details_id": "Int64"})

    dup_ids = int(prepared["referral_details_id"].duplicated().sum())
    if dup_ids:
        LOGGER.warning(
            "%d duplicated referral_details_id values - the source log "
            "table does not enforce unique ids; rows preserved.",
            dup_ids,
        )
    # Grain integrity: with every join key validated unique, the row
    # count must equal the number of (de-duplicated) log rows. A
    # mismatch means a join fanned out — abort instead of writing a
    # dataset that violates the one-row-per-detail contract.
    if len(prepared) != len(logs):
        LOGGER.error(
            "Grain check failed: %d prepared rows vs %d source log rows "
            "- a join fanned out unexpectedly; aborting.",
            len(prepared),
            len(logs),
        )
        return None
    null_report = prepared.isna().sum()
    LOGGER.info("Prepared dataset: %d rows x %d columns", *prepared.shape)
    LOGGER.info("Remaining null counts per column (reported, not dropped):")
    for column, count in null_report.items():
        if count:
            LOGGER.info("    %-36s %d", column, int(count))
    return prepared


def write_prepared_details(
    prepared: pd.DataFrame, output_dir: Path, filename: str = PREPARED_DETAILS_NAME
) -> Path:
    """Write the prepared dataset CSV into output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / filename
    prepared.to_csv(out_path, index=False)
    LOGGER.info("Wrote prepared referral details to %s", out_path)
    return out_path
