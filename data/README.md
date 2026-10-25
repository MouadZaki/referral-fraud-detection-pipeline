# Data directory

Source data is **not** versioned. `.gitignore` excludes everything under
`data/` except this file and `data/raw/.gitkeep`, so the raw extracts
cannot be committed by accident.

## Expected inputs

Place the seven source CSV extracts in `data/raw/`:

- `user_referrals.csv`
- `user_referral_logs.csv`
- `user_logs.csv`
- `user_referral_statuses.csv`
- `referral_rewards.csv`
- `paid_transactions.csv`
- `lead_logs.csv`

Obtain them from the extract bundle supplied with the Stage 0 brief;
do not download copies from third-party mirrors.

The pipeline entry point logs which of these files are present and
warns about any that are missing.
