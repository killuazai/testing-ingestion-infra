# Databricks notebook source
# MAGIC %md
# MAGIC # Check source reachability
# MAGIC Makes only small requests. A blocked PSA result means land the official
# MAGIC file locally.

# COMMAND ----------

import sys
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
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import requests

from src import config, ingestion, runtime

# COMMAND ----------

session = ingestion.retry_session(config.USER_AGENT, retries=1)
checks = [
    (
        "DPWH projects API",
        "GET",
        config.DPWH_PROJECTS_URL,
        {"page": 1, "limit": 1},
    ),
    (
        "Flood-control ArcGIS layer",
        "GET",
        config.FLOOD_LAYER_URL,
        {"f": "json"},
    ),
    ("PSA PSGC page", "GET", config.PSGC_PAGE_URL, None),
    ("PSA population page", "GET", config.POPULATION_PAGE_URL, None),
    ("PSA OpenSTAT population API", "GET", config.POPULATION_OPENSTAT_URL, None),
    ("BetterGov boundary page", "GET", config.BOUNDARY_PAGE_URL, None),
]

rows = []
for source, method, url, params in checks:
    try:
        response = session.request(method, url, params=params, timeout=(10, 30))
        status = "OK" if response.ok else f"HTTP {response.status_code}"
    except requests.RequestException as error:
        status = f"BLOCKED: {type(error).__name__}"
    rows.append((source, status, url))

if runtime.is_databricks():
    spark = runtime.get_spark()
    runtime.show(
        spark.createDataFrame(rows, "source string, status string, url string")
    )
else:
    for source, status, url in rows:
        print(f"{status:24} {source}: {url}")
