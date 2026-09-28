# Validation and stop conditions

## Stop the run

- A required API path, field, page, or file layout disappears.
- API totals change while pages are being downloaded.
- Source rows and Bronze candidate rows do not reconcile.
- A Silver primary key is null or duplicated.
- DPWH plus flood-only contract counts do not reconcile to Silver projects.
- The PSGC workbook does not have exactly one layout matching the reviewed aliases.
- Boundary publication contains missing geometry or duplicate current PSGC codes.

The final validation script writes the results before raising an error.

## Flag but keep the row

- Progress is outside 0 to 100.
- Coordinates are outside the broad Philippines bounding box.
- No unique city or municipality boundary contains the project point.
- A boundary code does not exist in the current PSGC place master.
- Amount paid is greater than the DPWH reported budget.
- Amount paid is zero and needs interpretation.
- Flood features for the same contract disagree on ABC or contract cost.

Flagged rows remain queryable. This avoids silently deleting unusual public records.

## Evidence to attach to a pull request

For each full run, attach:

1. The final `SUCCESS` lines and row counts.
2. The latest `04-validation.dq_results` rows.
3. A second run with unchanged inputs showing unchanged table counts.
4. Any populated PSGC override rows and their official Summary of Changes citation.

