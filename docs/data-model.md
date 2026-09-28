# Data model

## Bronze

| Table | Grain | Key |
| --- | --- | --- |
| `01-bronze.dpwh_projects` | One DPWH API project record | `contract_id` |
| `01-bronze.flood_control_projects` | One ArcGIS feature or project component | `object_id` |
| `01-bronze.psgc` | One place in the official PSGC publication workbook | `psgc_code` |
| `01-bronze.population_2024` | One PSA OpenSTAT geographic code | `psgc_code` |
| `01-bronze.boundary_bettergov` | One BetterGov boundary feature | `psgc_code` |

Every Bronze table includes source identifiers, source file or URL, `run_id`, and
`load_ts`. API and file records retain a JSON representation where useful.

## Silver

| Table | Grain | Key |
| --- | --- | --- |
| `02-silver.places` | One current PSGC place | `psgc_code` |
| `02-silver.population` | One 2024 population count per geographic code | `psgc_code` |
| `02-silver.boundaries` | One conformed boundary feature | `psgc_code` |
| `02-silver.projects` | One contract across DPWH and flood-control sources | `contract_id` |

Important `projects` fields:

| Field | Meaning |
| --- | --- |
| `contract_id` | Shared project key used to combine sources |
| `reported_budget` | Budget value reported by the DPWH projects API |
| `flood_abc` | Approved budget for contract from the flood layer |
| `flood_contract_cost` | Contract cost from the flood layer |
| `amount_paid` | Amount paid reported by the DPWH API |
| `project_source` | `DPWH`, `FLOOD`, or `BOTH` |
| `psgc_code` | Unique city or municipality boundary match, when available |
| `psgc_match_status` | Match result; unmatched projects remain visible |
| `flood_record_count` | Number of flood features combined into the contract |
| `flood_value_conflict_flag` | Multiple ABC or contract-cost values need review |

The pipeline does not call `location.province` a province. It stores that DPWH
district-office value as `deo`, matching the source warning.

## Validation history

`04-validation.dq_results` has one row per check per run:

`run_id`, `run_mode`, `table_schema`, `table_name`, `column`, `check_name`,
`failed_rows`, `total_rows`, `percentage`, `severity`, `status`, `checked_at`.

