# Databricks notebook source
# MAGIC %md
# MAGIC # Run setup through Silver
# MAGIC Reuses one Spark session. Set `BUILDABIDA_RUN_MODE=sample` for low-cost testing.

# COMMAND ----------

import os
import runpy
from pathlib import Path


def repository_root() -> Path:
    starts = [Path.cwd()]
    if "__file__" in globals():
        starts.insert(0, Path(__file__).resolve().parent)
    for start in starts:
        for candidate in (start, *start.parents):
            if (candidate / "src").is_dir():
                return candidate
    raise RuntimeError("Could not find repository root")


ROOT = repository_root()
steps = [
    ("setup", "notebooks/00_setup/00_setup_workspace.py"),
    ("source_check", "notebooks/00_setup/01_check_sources.py"),
    ("bronze_dpwh", "notebooks/01_bronze/01_dpwh_projects.py"),
    ("bronze_flood", "notebooks/01_bronze/02_flood_control.py"),
    ("bronze_psgc", "notebooks/01_bronze/03_psgc.py"),
    ("bronze_population", "notebooks/01_bronze/04_population.py"),
    ("bronze_boundaries", "notebooks/01_bronze/05_boundaries.py"),
    ("silver_places", "notebooks/02_silver/01_places.py"),
    ("silver_population", "notebooks/02_silver/02_population.py"),
    ("silver_boundaries", "notebooks/02_silver/03_boundaries.py"),
    ("silver_projects", "notebooks/02_silver/04_projects.py"),
    ("validate_silver", "notebooks/02_silver/99_validate.py"),
]
step_names = [name for name, _ in steps]
start_at = os.environ.get("BUILDABIDA_START_AT", step_names[0])
stop_after = os.environ.get("BUILDABIDA_STOP_AFTER", step_names[-1])
if start_at not in step_names or stop_after not in step_names:
    raise ValueError(
        f"START_AT and STOP_AFTER must be in this order: {', '.join(step_names)}"
    )
start_index = step_names.index(start_at)
stop_index = step_names.index(stop_after)
if start_index > stop_index:
    raise ValueError("BUILDABIDA_START_AT comes after BUILDABIDA_STOP_AFTER")

for name, relative_path in steps[start_index : stop_index + 1]:
    print(f"\n===== START {name} =====")
    runpy.run_path(str(ROOT / relative_path), run_name=f"__buildabida_{name}__")
    print(f"===== DONE {name} =====")

print(f"PIPELINE SUCCESS: {start_at} through {stop_after}")
