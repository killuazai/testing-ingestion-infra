# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: flood-control projects
# MAGIC Verified ArcGIS pages -> immutable landing batch -> source-shaped Bronze snapshot.

# COMMAND ----------

import hashlib
import math
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


from pyspark import StorageLevel
from pyspark.sql import functions as F

from src import config, ingestion, runtime


spark = runtime.get_spark()

# COMMAND ----------

# ---------------------------------------------------------
# ingestion configuration
# ---------------------------------------------------------

source_system = "DPWH Sumbong sa Pangulo ArcGIS layer"

load_ts = ingestion.utc_now()
run_id = ingestion.make_run_id(load_ts)

batch_path = ingestion.ensure_directory(
    runtime.landing_root()
    / "flood_control"
    / run_id
)

timeout = (
    config.REQUEST_CONNECT_TIMEOUT,
    config.REQUEST_READ_TIMEOUT,
)

session = ingestion.retry_session(
    config.USER_AGENT
)

print("run_id:", run_id)
print("run_mode:", config.RUN_MODE)
print("landing:", batch_path)
print("source:", config.FLOOD_LAYER_URL)

# COMMAND ----------

# ---------------------------------------------------------
# inspect and verify ArcGIS layer metadata
# ---------------------------------------------------------

metadata, metadata_raw = ingestion.request_json(
    session,
    "GET",
    config.FLOOD_LAYER_URL,
    params={"f": "json"},
    timeout=timeout,
)

if (
    not isinstance(metadata, dict)
    or metadata.get("error")
):
    raise RuntimeError(
        "Invalid ArcGIS layer metadata"
    )


capabilities = (
    metadata.get("advancedQueryCapabilities")
    or {}
)

if capabilities.get("supportsPagination") is not True:
    raise RuntimeError(
        "ArcGIS source no longer reports pagination support"
    )


max_record_count = int(
    metadata.get("maxRecordCount", 0)
)

if max_record_count < config.FLOOD_PAGE_SIZE:
    raise RuntimeError(
        "Configured flood page size exceeds "
        f"the source maximum of {max_record_count}"
    )


object_id_field = metadata.get("objectIdField")
fields = metadata.get("fields")

if (
    not isinstance(object_id_field, str)
    or not isinstance(fields, list)
):
    raise RuntimeError(
        "ArcGIS metadata is missing "
        "fields or objectIdField"
    )


expected_fields = {
    field["name"]
    for field in fields
    if (
        isinstance(field, dict)
        and isinstance(field.get("name"), str)
    )
}


required_fields = [
    object_id_field,
    *config.source_contracts()["flood"]["required_fields"],
]


missing_source_fields = [
    field
    for field in required_fields
    if field not in expected_fields
]

if missing_source_fields:
    raise RuntimeError(
        "ArcGIS source is missing required fields: "
        f"{sorted(missing_source_fields)}"
    )


ingestion.write_bytes(
    batch_path / "layer_metadata.json",
    metadata_raw,
)


field_contract_hash = hashlib.sha256(
    "|".join(
        sorted(expected_fields)
    ).encode()
).hexdigest()


print(
    f"ArcGIS metadata verified: "
    f"{len(expected_fields)} fields"
)

# COMMAND ----------

# ---------------------------------------------------------
# get source row count before downloading
# ---------------------------------------------------------

count_payload, _ = ingestion.request_json(
    session,
    "GET",
    config.FLOOD_QUERY_URL,
    params={
        "where": "1=1",
        "returnCountOnly": "true",
        "f": "json",
    },
    timeout=timeout,
)


if (
    not isinstance(count_payload, dict)
    or not isinstance(
        count_payload.get("count"),
        int,
    )
):
    raise RuntimeError(
        f"Invalid ArcGIS count response: "
        f"{count_payload!r}"
    )


source_total = count_payload["count"]

if source_total <= 0:
    raise RuntimeError(
        "Flood-control source returned zero rows"
    )


source_pages = math.ceil(
    source_total
    / config.FLOOD_PAGE_SIZE
)


pages_to_download = (
    min(
        source_pages,
        config.SAMPLE_PAGES,
    )
    if config.RUN_MODE == "sample"
    else source_pages
)


print("source_total:", source_total)
print("source_pages:", source_pages)
print("pages_to_download:", pages_to_download)

# COMMAND ----------

# ---------------------------------------------------------
# download paginated GeoJSON
# ---------------------------------------------------------

records_received = 0

page_fingerprints: set[str] = set()
page_summaries: list[dict] = []


for page_number in range(
    1,
    pages_to_download + 1,
):

    if page_number > config.MAX_API_PAGES:
        raise RuntimeError(
            "Stopped at page safety limit "
            f"{config.MAX_API_PAGES}"
        )


    offset = (
        page_number - 1
    ) * config.FLOOD_PAGE_SIZE


    payload, raw = ingestion.request_json(
        session,
        "GET",
        config.FLOOD_QUERY_URL,
        params={
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "resultOffset": offset,
            "resultRecordCount": config.FLOOD_PAGE_SIZE,
            "orderByFields": (
                f"{object_id_field} ASC"
            ),
            "f": "geojson",
        },
        timeout=timeout,
    )


    if (
        not isinstance(payload, dict)
        or payload.get("type")
        != "FeatureCollection"
    ):
        raise RuntimeError(
            f"Flood page {page_number} "
            "is not valid GeoJSON"
        )


    features = ingestion.record_array(
        payload,
        ("features",),
    )


    if not features:
        raise RuntimeError(
            f"Flood page {page_number} "
            "returned no features"
        )


    if len(features) > config.FLOOD_PAGE_SIZE:
        raise RuntimeError(
            f"Flood page {page_number} "
            "exceeded configured page size"
        )


    object_ids = []


    for feature in features:

        properties = feature.get("properties")
        geometry = feature.get("geometry")


        if not isinstance(properties, dict):
            raise RuntimeError(
                f"Flood page {page_number} "
                "contains invalid properties"
            )


        if not isinstance(geometry, dict):
            raise RuntimeError(
                f"Flood page {page_number} "
                "contains invalid geometry"
            )


        missing_fields = (
            set(required_fields)
            - set(properties)
        )

        if missing_fields:
            raise RuntimeError(
                f"Flood page {page_number} "
                "is missing required fields: "
                f"{sorted(missing_fields)}"
            )


        object_ids.append(
            str(
                properties[
                    object_id_field
                ]
            )
        )


    fingerprint = hashlib.sha256(
        "\0".join(
            object_ids
        ).encode()
    ).hexdigest()


    if fingerprint in page_fingerprints:
        raise RuntimeError(
            f"Flood page {page_number} "
            "repeated an earlier page"
        )


    page_fingerprints.add(
        fingerprint
    )


    raw_file = (
        batch_path
        / f"page_{page_number:05d}.geojson"
    )


    ingestion.write_bytes(
        raw_file,
        raw,
    )


    records_received += len(features)


    page_summaries.append(
        {
            "page": page_number,
            "records": len(features),
            "sha256": ingestion.sha256(raw),
            "source_file": raw_file.name,
        }
    )


    print(
        f"page {page_number}/{pages_to_download}: "
        f"{len(features):,} records | "
        f"total received: {records_received:,}"
    )


# full-run reconciliation
if (
    config.RUN_MODE == "full"
    and records_received != source_total
):
    raise RuntimeError(
        f"Downloaded {records_received} "
        "flood records; "
        f"expected {source_total}"
    )


print(
    f"download complete: "
    f"{records_received:,} records"
)

# COMMAND ----------

# ---------------------------------------------------------
# write ingestion manifest
# ---------------------------------------------------------

ingestion.write_json(
    batch_path / "manifest.json",
    {
        "run_id": run_id,
        "run_mode": config.RUN_MODE,
        "source_system": source_system,
        "source_url": config.FLOOD_LAYER_URL,
        "source_total": source_total,
        "records_received": records_received,
        "field_contract_sha256": field_contract_hash,
        "pages": page_summaries,
        "completed_at": ingestion.utc_now(),
    },
)

# COMMAND ----------

# ---------------------------------------------------------
# read landed GeoJSON with Spark
# ---------------------------------------------------------

raw_pages = (
    spark.read
    .option(
        "multiLine",
        True,
    )
    .json(
        f"{batch_path}/page_*.geojson"
    )
)


features = raw_pages.select(
    F.explode(
        "features"
    ).alias("feature"),

    F.input_file_name()
    .alias("source_path"),
)

# COMMAND ----------

# ---------------------------------------------------------
# create source-shaped Bronze candidate
#
# date handling:
# - trim removes source whitespace such as "11/26/2021 "
# - try_to_timestamp prevents one malformed source date
#   from crashing the entire ingestion
# - original source values remain preserved in source_record_json
# ---------------------------------------------------------

candidate = features.select(

    F.col("feature.properties")
    .getField(object_id_field)
    .cast("long")
    .alias("object_id"),

    F.trim(
        F.col(
            "feature.properties.ContractID"
        ).cast("string")
    ).alias("contract_id"),

    F.col(
        "feature.properties.ProjectID"
    )
    .cast("string")
    .alias("project_id"),

    F.col(
        "feature.properties.ProjectComponentID"
    )
    .cast("string")
    .alias("project_component_id"),

    F.col(
        "feature.properties.ProjectDescription"
    )
    .cast("string")
    .alias("description"),

    F.col(
        "feature.properties.infra_type"
    )
    .cast("string")
    .alias("infra_type"),

    F.col(
        "feature.properties.TypeofWork"
    )
    .cast("string")
    .alias("type_of_work"),

    F.col(
        "feature.properties.Region"
    )
    .cast("string")
    .alias("reported_region"),

    F.col(
        "feature.properties.Province"
    )
    .cast("string")
    .alias("reported_province"),

    F.col(
        "feature.properties.Municipality"
    )
    .cast("string")
    .alias("reported_municipality"),

    F.col(
        "feature.properties."
        "DistrictEngineeringOffice"
    )
    .cast("string")
    .alias("deo"),

    F.col(
        "feature.properties.ABC"
    )
    .cast("decimal(18,2)")
    .alias("abc"),

    F.col(
        "feature.properties.ContractCost"
    )
    .cast("decimal(18,2)")
    .alias("contract_cost"),

    F.col(
        "feature.properties.InfraYear"
    )
    .cast("int")
    .alias("infra_year"),

    F.expr(
        """
        CAST(
            try_to_timestamp(
                trim(
                    feature.properties.StartDate
                ),
                'MM/dd/yyyy'
            )
            AS DATE
        )
        """
    ).alias("start_date"),

    F.expr(
        """
        CAST(
            try_to_timestamp(
                trim(
                    feature.properties.CompletionDateActual
                ),
                'MM/dd/yyyy'
            )
            AS DATE
        )
        """
    ).alias("completion_date"),

    F.col(
        "feature.properties.Contractor"
    )
    .cast("string")
    .alias("contractor"),

    F.coalesce(
        F.col(
            "feature.properties.Latitude"
        ).cast("double"),

        F.col(
            "feature.geometry.coordinates"
        )[1].cast("double"),
    ).alias("latitude"),

    F.coalesce(
        F.col(
            "feature.properties.Longitude"
        ).cast("double"),

        F.col(
            "feature.geometry.coordinates"
        )[0].cast("double"),
    ).alias("longitude"),

    F.to_json(
        "feature.properties"
    ).alias(
        "source_record_json"
    ),

    F.to_json(
        "feature.geometry"
    ).alias(
        "geometry_json"
    ),

    F.lit(
        load_ts
    )
    .cast("timestamp")
    .alias("load_ts"),

    F.lit(
        run_id
    ).alias(
        "run_id"
    ),

    F.lit(
        source_system
    ).alias(
        "source_system"
    ),

    F.lit(
        config.FLOOD_LAYER_URL
    ).alias(
        "source_url"
    ),

    F.regexp_extract(
        "source_path",
        r"([^/]+)$",
        1,
    ).alias(
        "source_file"
    ),
)

# COMMAND ----------

# ---------------------------------------------------------
# minimal ingestion reconciliation
# ---------------------------------------------------------

candidate.persist(
    StorageLevel.DISK_ONLY
)


stats = candidate.agg(

    F.count("*")
    .alias("rows"),

    F.sum(
        F.when(
            F.col("object_id").isNull(),
            1,
        ).otherwise(0)
    ).alias("null_keys"),

).first()


if (
    stats.rows != records_received
    or stats.null_keys
):

    candidate.unpersist()

    raise RuntimeError(
        "Flood candidate failed reconciliation: "
        f"{stats.asDict()} "
        f"from {records_received} records"
    )


print(
    "candidate rows:",
    stats.rows,
)

# COMMAND ----------

# ---------------------------------------------------------
# publish current Bronze snapshot
# ---------------------------------------------------------

try:

    runtime.publish_table(
        candidate,
        config.BRONZE_SCHEMA,
        config.TABLE_FLOOD,
    )

finally:

    candidate.unpersist()

# COMMAND ----------

# ---------------------------------------------------------
# confirm Bronze write
# ---------------------------------------------------------

written = runtime.read_table(
    spark,
    config.BRONZE_SCHEMA,
    config.TABLE_FLOOD,
).count()


if written != records_received:
    raise RuntimeError(
        f"Flood Bronze wrote {written}; "
        f"expected {records_received}"
    )


print(
    f"SUCCESS {config.TABLE_FLOOD}: "
    f"{written:,} rows -> "
    f"{runtime.table_label(config.BRONZE_SCHEMA, config.TABLE_FLOOD)}"
)