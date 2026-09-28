# Implementation report

## Delivered scope

- Empty test repository scaffolded from setup through validated Silver.
- Dual local VS Code and Databricks runtime support.
- Five Bronze sources and four Silver tables.
- Persisted validation history with stop and flag severities.
- Low-cost sample mode isolated from full tables.
- Exact run order, source cards, data model, architecture, and recovery runbook.

## Source-of-truth alignment

- Catalog and schema names match D-13.
- Folder layout matches D-12.
- Python handles source ingestion; Spark SQL handles Silver business logic, matching
  D-08. Python remains only for orchestration and portable spatial matching.
- Cross-source project records combine by contract ID.
- DPWH district office names are not treated as PSGC provinces.
- Places are matched by PSGC code and point-in-polygon, not fuzzy name matching.
- Null keys, duplicate keys, and row-count reconciliation stop the run.
- Progress, coordinates, place match, and money checks flag rows for review.

## Cost and scale controls

- One Spark session for an end-to-end run.
- Maximum verified page sizes.
- Bounded retries, timeouts, and page guards.
- Page-at-a-time source handling.
- One bulk publication per table.
- Disk-only persistence only for reused candidates.
- Bounded city/municipality boundary broadcast with an executor-local spatial index.

## Human verification still required

- Download the official PSGC 2Q 2026 publication workbook and BetterGov boundary
  GeoJSON files.
- Review the PSGC Summary of Changes and populate only verified code overrides.
- Run the full pipeline twice with real files in the team workspace.
- Review all warning rows and attach the DQ output to the pull request.
- Confirm the observed source fields and counts before merging into the team repo.

