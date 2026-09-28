# Source cards

Checked on 29 September 2026.

## DPWH projects

- Publisher: Department of Public Works and Highways; API copy by BetterGov.
- API: <https://api.dpwh.bettergov.ph/projects>
- Official portal: <https://transparency.dpwh.gov.ph>
- Grain: one API record per DPWH project.
- Observed total: 265,582 projects.
- Verified maximum request: 5,000 records per page.
- Warning: `location.province` is a district engineering office, not a PSGC
  province. Coordinates are top-level fields in the current live response.

## Flood-control projects

- Publisher: Department of Public Works and Highways.
- Portal: <https://sumbongsapangulo.ph>
- ArcGIS layer: the URL is centralized in `src/config.py`.
- Grain: one GeoJSON feature or project component.
- Observed total: 9,855 features.
- Verified maximum request: 1,000 records per page.
- Deduplication rule: combine cross-source records by normalized contract ID.

## PSGC

- Publisher: Philippine Statistics Authority.
- Official page: <https://psa.gov.ph/classification/psgc>
- Release: 2Q 2026, as of 30 June 2026, released 13 July 2026.
- Grain: one geographic area.
- Format: manually downloaded Excel publication workbook.
- License: PSA site content is CC BY 4.0 unless otherwise stated.
- Warning: inspect the official Summary of Changes before adding any code override.

## 2024 population

- Publisher: Philippine Statistics Authority.
- Official release page:
  <https://psa.gov.ph/content/2024-census-population-popcen-population-counts-declared-official-president>
- Official OpenSTAT table: URL and request body are centralized in `src/config.py`.
- Grain: one 10-digit geographic code.
- Reference date: 1 July 2024.
- The pipeline uses the code returned by OpenSTAT, not a place-name join.
- Warning: PSA endpoints may block Databricks; land the response from a local run.

## Boundary maps

- Publisher: BetterGov copy of PSA/NAMRIA-derived hierarchical boundaries.
- Dataset page: <https://data.bettergov.ph/datasets/23>
- Grain: one geographic boundary feature.
- Use: assign project points to cities or municipalities.
- Warning: the source uses an older PSGC release. Apply only reviewed overrides.

