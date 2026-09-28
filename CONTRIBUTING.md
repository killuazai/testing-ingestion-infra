# Contributing

1. Pull `main` and create one small branch.
2. Link the change to a GitHub issue.
3. Run `ruff check .`, `ruff format --check .`, and `pytest -q`.
4. Run the affected pipeline stage in sample mode.
5. Update the data model, runbook, or decision log when behavior changes.
6. Open a pull request and ask one teammate to review it.

Never commit raw data, credentials, tokens, `.env`, or `.databrickscfg` files.
Do not change a source meaning, key, threshold, or deduplication rule without a
documented team decision and evidence.

