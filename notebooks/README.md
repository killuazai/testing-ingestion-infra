# Notebook order

Use `00_run_pipeline.py` for the official run. It executes these stages in order:

| Order | File | Output |
| --- | --- | --- |
| 0 | `00_setup/00_setup_workspace.py` | Catalog, schemas, Volume, or local folders |
| 0 | `00_setup/01_check_sources.py` | Reachability report |
| 1 | `01_bronze/01_dpwh_projects.py` | `01-bronze.dpwh_projects` |
| 2 | `01_bronze/02_flood_control.py` | `01-bronze.flood_control_projects` |
| 3 | `01_bronze/03_psgc.py` | `01-bronze.psgc` |
| 4 | `01_bronze/04_population.py` | `01-bronze.population_2024` |
| 5 | `01_bronze/05_boundaries.py` | `01-bronze.boundary_bettergov` |
| 6 | `02_silver/01_places.py` | `02-silver.places` |
| 7 | `02_silver/02_population.py` | `02-silver.population` |
| 8 | `02_silver/03_boundaries.py` | `02-silver.boundaries` |
| 9 | `02_silver/04_projects.py` | `02-silver.projects` |
| 10 | `02_silver/99_validate.py` | `04-validation.dq_results` |

Sample mode appends `_sample` to every managed data table except the validation
history. Never rename a sample table to a production table.

