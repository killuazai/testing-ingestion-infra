# Runbook

## Official run path

Run from the repository root. `notebooks/00_run_pipeline.py` is the official path
because it keeps one Spark session alive and records the order in code.

## Prepare manual sources

### Local VS Code

1. Download `PSGC 2Q 2026 Publication Datafile` from the
   [official PSGC page](https://psa.gov.ph/classification/psgc).
2. Put it in `data/landing/psgc`.
3. Download the BetterGov hierarchical GeoJSON resources from
   [dataset 23](https://data.bettergov.ph/datasets/23).
4. Put the GeoJSON files in `data/landing/boundaries/bettergov`.

### Databricks

Upload the same files to:

```text
/Volumes/buildabida-capstone/00-source/landing/psgc/
/Volumes/buildabida-capstone/00-source/landing/boundaries/bettergov/
```

## Run order

The orchestrator runs:

1. Workspace setup and source reachability.
2. DPWH Bronze.
3. Flood-control Bronze.
4. PSGC Bronze.
5. Population Bronze.
6. Boundary Bronze.
7. Silver places.
8. Silver population.
9. Silver boundaries.
10. Silver projects.
11. Silver validation and persisted DQ results.

## Resume from a stage

Completed snapshots can be reused. For example:

```bash
BUILDABIDA_START_AT=silver_places \
BUILDABIDA_STOP_AFTER=validate_silver \
python notebooks/00_run_pipeline.py
```

Valid stage names are listed near the top of `notebooks/00_run_pipeline.py`.

## Configuration

All optional environment variables use the `BUILDABIDA_` prefix:

| Variable | Default | Purpose |
| --- | --- | --- |
| `RUN_MODE` | `full` | Use `sample` for separate low-cost tables |
| `SAMPLE_PAGES` | `1` | API pages per source in sample mode |
| `DATA_ROOT` | repository `data` | Local landing and table root |
| `CATALOG` | `buildabida-capstone` | Unity Catalog catalog |
| `PSGC_FILE` | auto-detect | Exact publication workbook file name |
| `POPULATION_RAW_FILE` | empty | Pre-landed OpenSTAT JSON path |
| `BOUNDARY_DIRECTORY` | standard landing path | BetterGov GeoJSON folder |
| `START_AT` | `setup` | First orchestrator stage |
| `STOP_AFTER` | `validate_silver` | Last orchestrator stage |

Source URLs, page sizes, timeout defaults, aliases, and safety limits are also
centralized. Do not copy them into notebooks.

## Failure recovery

- A failed API run leaves its batch folder for inspection but does not replace
  Bronze.
- A failed local table swap restores the last completed Parquet snapshot.
- Fix the source, configuration, or code, then rerun the failed stage.
- Never edit a landed raw response to make a run pass. Land a new source artifact.

