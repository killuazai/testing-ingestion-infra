"""Central configuration for every pipeline stage."""

from __future__ import annotations

import json
import os
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def env(name: str, default: str) -> str:
    """Read one Buildabida environment setting."""
    return os.environ.get(f"BUILDABIDA_{name}", default)


CATALOG = env("CATALOG", "buildabida-capstone")
SOURCE_SCHEMA = env("SOURCE_SCHEMA", "00-source")
BRONZE_SCHEMA = env("BRONZE_SCHEMA", "01-bronze")
SILVER_SCHEMA = env("SILVER_SCHEMA", "02-silver")
VALIDATION_SCHEMA = env("VALIDATION_SCHEMA", "04-validation")

DPWH_PROJECTS_URL = env("DPWH_PROJECTS_URL", "https://api.dpwh.bettergov.ph/projects")
FLOOD_LAYER_URL = env(
    "FLOOD_LAYER_URL",
    "https://services1.arcgis.com/IwZZTMxZCmAmFYvF/arcgis/rest/services/"
    "FloodControl_Data_20250802_v6_corrected_coordinates_for_uploading/"
    "FeatureServer/0",
)
FLOOD_QUERY_URL = f"{FLOOD_LAYER_URL}/query"
POPULATION_OPENSTAT_URL = env(
    "POPULATION_OPENSTAT_URL",
    "https://openstat.psa.gov.ph/PXWeb/api/v1/en/DB/1A/PO_2024/0231A6DPUP0.px",
)
PSGC_PAGE_URL = "https://psa.gov.ph/classification/psgc"
POPULATION_PAGE_URL = (
    "https://psa.gov.ph/content/2024-census-population-popcen-population-"
    "counts-declared-official-president"
)
BOUNDARY_PAGE_URL = "https://data.bettergov.ph/datasets/23"

USER_AGENT = env(
    "USER_AGENT",
    "Buildabida testing-ingestion-infra "
    "(https://github.com/killuazai/testing-ingestion-infra)",
)

DPWH_PAGE_SIZE = int(env("DPWH_PAGE_SIZE", "5000"))
FLOOD_PAGE_SIZE = int(env("FLOOD_PAGE_SIZE", "1000"))
MAX_API_PAGES = int(env("MAX_API_PAGES", "10000"))
REQUEST_CONNECT_TIMEOUT = int(env("REQUEST_CONNECT_TIMEOUT", "10"))
REQUEST_READ_TIMEOUT = int(env("REQUEST_READ_TIMEOUT", "90"))

RUN_MODE = env("RUN_MODE", "full").casefold()
if RUN_MODE not in {"full", "sample"}:
    raise ValueError("BUILDABIDA_RUN_MODE must be full or sample")
SAMPLE_PAGES = int(env("SAMPLE_PAGES", "1"))


def managed_name(base_name: str) -> str:
    """Keep low-cost sample outputs separate from full pipeline tables."""
    return f"{base_name}_sample" if RUN_MODE == "sample" else base_name


TABLE_DPWH = managed_name("dpwh_projects")
TABLE_FLOOD = managed_name("flood_control_projects")
TABLE_PSGC = managed_name("psgc")
TABLE_PSGC_CHANGES = managed_name("psgc_changes")
TABLE_POPULATION = managed_name("population_2024")
TABLE_BOUNDARIES = managed_name("boundary_bettergov")
TABLE_PLACES = managed_name("places")
TABLE_SILVER_POPULATION = managed_name("population")
TABLE_SILVER_BOUNDARIES = managed_name("boundaries")
TABLE_PROJECTS = managed_name("projects")
TABLE_DQ_RESULTS = "dq_results"

SOURCE_CONTRACT_PATH = REPOSITORY_ROOT / "config" / "source_contracts.json"
PSGC_OVERRIDE_PATH = REPOSITORY_ROOT / "config" / "psgc_code_overrides.csv"


def source_contracts() -> dict:
    """Load reviewed source contracts from version-controlled JSON."""
    with SOURCE_CONTRACT_PATH.open(encoding="utf-8") as stream:
        return json.load(stream)


def population_query() -> dict:
    """Return the official OpenSTAT query for 2024 total population."""
    return {
        "query": [
            {
                "code": "Geographic Location",
                "selection": {"filter": "all", "values": ["*"]},
            },
            {
                "code": "Parameter",
                "selection": {"filter": "item", "values": ["0"]},
            },
        ],
        "response": {"format": "json"},
    }
