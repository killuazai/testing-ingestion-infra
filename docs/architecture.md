# Architecture and engineering decisions

## Flow

Every source follows the same small contract:

1. Inspect the live or landed source contract.
2. Preserve exact source bytes in a run-specific landing folder.
3. Validate page counts, fields, file layout, and row counts.
4. Build a source-shaped Bronze candidate with lineage.
5. Replace the current Bronze snapshot only after checks pass.
6. Conform, deduplicate, and join in Silver.
7. Persist quality results, then stop on key or reconciliation failures.

## Runtime boundary

`src/runtime.py` is the only module that knows whether the code is local or in
Databricks. Local tables are atomic Parquet snapshots. Databricks tables are Delta
snapshots. Ingestion and transformation code stays unchanged.

## Scale and cost choices

- The pipeline is batch-oriented because the sources publish snapshots, not event
  streams. Streaming infrastructure would add cost without improving freshness.
- API fetches use the largest verified page size and bounded retries.
- Spark performs bulk reads, transformations, and one publication per table.
- Bronze uses Python for APIs and Excel. Silver business transformations use Spark
  SQL, following team decision D-08. Python is retained only for orchestration,
  validation helpers, and the spatial-index operation SQL cannot provide portably.
- Project geocoding uses a Shapely spatial index built once per Spark partition.
  Only city and municipality boundaries are broadcast. Barangay geometries are not
  broadcast to every executor.
- The full pipeline runs in one process so Spark starts once.
- Full outputs use stable table names. Samples use separate `_sample` tables.

## Reliability choices

- Raw pages and their SHA-256 hashes provide provenance.
- Pagination totals cannot change silently during a run.
- Schema drift fails before publication.
- Local publication stages a complete Parquet folder, swaps it atomically, and
  restores the previous snapshot when the swap fails.
- Silver project keys and source-to-Silver row counts are fail-fast checks.
- Ambiguous business meanings stay in separate columns. DPWH reported budget,
  flood-control ABC, and flood contract cost are never silently combined.
- `amount_paid = 0` remains source data and receives a review flag; it is not
  converted to null without evidence.

## Deliberate limits

- BetterGov boundary codes are from an older PSGC release. Reviewed changes belong
  in `config/psgc_code_overrides.csv`; the code does not guess replacements.
- A project without coordinates or a unique boundary match remains in Silver with
  a `psgc_match_status` flag.
- The Philippines coordinate check is a broad bounding-box warning. The exact
  point-in-polygon result is the stronger place-match check.
- Silver is the stopping point. Gold and presentation logic belong in a later PR.
