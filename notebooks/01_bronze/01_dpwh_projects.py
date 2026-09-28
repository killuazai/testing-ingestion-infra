# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: DPWH projects
# MAGIC API pages -> immutable landing batch -> source-shaped Bronze snapshot.

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

source_system = "DPWH Transparency Portal via BetterGov"
load_ts = ingestion.utc_now()
run_id = ingestion.make_run_id(load_ts)
batch_path = ingestion.ensure_directory(
    runtime.landing_root() / "dpwh_projects" / run_id
)
timeout = (config.REQUEST_CONNECT_TIMEOUT, config.REQUEST_READ_TIMEOUT)
source_contract = config.source_contracts()["dpwh"]
required_fields = set(source_contract["required_fields"])
required_location_fields = set(source_contract["required_location_fields"])

session = ingestion.retry_session(config.USER_AGENT)
page_number = 1
source_total = None
source_pages = None
pages_to_download = None
records_received = 0
page_fingerprints: set[str] = set()
page_summaries: list[dict] = []

while pages_to_download is None or page_number <= pages_to_download:
    if page_number > config.MAX_API_PAGES:
        raise RuntimeError(f"Stopped at page safety limit {config.MAX_API_PAGES}")
    payload, raw = ingestion.request_json(
        session,
        "GET",
        config.DPWH_PROJECTS_URL,
        params={"page": page_number, "limit": config.DPWH_PAGE_SIZE},
        timeout=timeout,
    )
    records = ingestion.record_array(payload, ("data", "data"))
    pagination = ingestion.value_at_path(payload, ("data", "pagination"))
    if not isinstance(pagination, dict):
        raise TypeError("data.pagination must be an object")
    if pagination.get("page") != page_number:
        raise RuntimeError(f"Source returned the wrong page: {pagination}")
    if pagination.get("limit") != config.DPWH_PAGE_SIZE:
        raise RuntimeError(f"Source changed the requested page size: {pagination}")

    if page_number == 1:
        source_total = pagination.get("totalCount")
        source_pages = pagination.get("totalPages")
        if not isinstance(source_total, int) or source_total <= 0:
            raise RuntimeError(f"Invalid source total: {source_total!r}")
        expected_pages = math.ceil(source_total / config.DPWH_PAGE_SIZE)
        if source_pages != expected_pages:
            raise RuntimeError(
                f"Source reports {source_pages} pages; expected {expected_pages}"
            )
        pages_to_download = (
            min(source_pages, config.SAMPLE_PAGES)
            if config.RUN_MODE == "sample"
            else source_pages
        )
    elif (
        pagination.get("totalCount") != source_total
        or pagination.get("totalPages") != source_pages
    ):
        raise RuntimeError("DPWH source total changed during the run")

    if not records:
        raise RuntimeError(f"DPWH page {page_number} returned no records")
    for record in records:
        missing = required_fields - set(record)
        if missing:
            raise RuntimeError(
                f"DPWH page {page_number} is missing fields: {sorted(missing)}"
            )
        if not isinstance(record["location"], dict):
            raise TypeError("DPWH location must be an object")
        missing_location = required_location_fields - set(record["location"])
        if missing_location:
            raise RuntimeError(
                f"DPWH page {page_number} has location objects missing: "
                f"{sorted(missing_location)}"
            )

    fingerprint = hashlib.sha256(
        "\0".join(str(record["contractId"]) for record in records).encode()
    ).hexdigest()
    if fingerprint in page_fingerprints:
        raise RuntimeError(f"DPWH page {page_number} repeated an earlier page")
    page_fingerprints.add(fingerprint)

    raw_file = batch_path / f"page_{page_number:05d}.json"
    ingestion.write_bytes(raw_file, raw)
    records_received += len(records)
    page_summaries.append(
        {
            "page": page_number,
            "records": len(records),
            "sha256": ingestion.sha256(raw),
            "source_file": raw_file.name,
        }
    )
    page_number += 1

if config.RUN_MODE == "full" and records_received != source_total:
    raise RuntimeError(
        f"Downloaded {records_received} DPWH records; expected {source_total}"
    )

ingestion.write_json(
    batch_path / "manifest.json",
    {
        "run_id": run_id,
        "run_mode": config.RUN_MODE,
        "source_system": source_system,
        "source_url": config.DPWH_PROJECTS_URL,
        "source_total": source_total,
        "records_received": records_received,
        "pages": page_summaries,
        "completed_at": ingestion.utc_now(),
    },
)

# COMMAND ----------

raw_pages = spark.read.option("multiLine", True).json(f"{batch_path}/page_*.json")
projects = raw_pages.select(
    F.explode("data.data").alias("project"),
    F.input_file_name().alias("source_path"),
)
candidate = projects.select(
    F.trim(F.col("project.contractId").cast("string")).alias("contract_id"),
    F.col("project.description").cast("string").alias("description"),
    F.col("project.category").cast("string").alias("category"),
    F.col("project.status").cast("string").alias("status"),
    F.col("project.budget").cast("decimal(18,2)").alias("reported_budget"),
    F.col("project.amountPaid").cast("decimal(18,2)").alias("amount_paid"),
    F.col("project.progress").cast("decimal(7,3)").alias("progress"),
    F.col("project.infraYear").cast("int").alias("infra_year"),
    F.col("project.programName").cast("string").alias("program_name"),
    F.col("project.sourceOfFunds").cast("string").alias("source_of_funds"),
    F.col("project.contractor").cast("string").alias("contractor"),
    F.to_date("project.startDate").alias("start_date"),
    F.to_date("project.completionDate").alias("completion_date"),
    F.col("project.location.region").cast("string").alias("reported_region"),
    F.col("project.location.province").cast("string").alias("deo"),
    F.col("project.latitude").cast("double").alias("latitude"),
    F.col("project.longitude").cast("double").alias("longitude"),
    F.to_json("project").alias("source_record_json"),
    F.lit(load_ts).cast("timestamp").alias("load_ts"),
    F.lit(run_id).alias("run_id"),
    F.lit(source_system).alias("source_system"),
    F.lit(config.DPWH_PROJECTS_URL).alias("source_url"),
    F.regexp_extract("source_path", r"([^/]+)$", 1).alias("source_file"),
)
candidate.persist(StorageLevel.DISK_ONLY)
stats = candidate.agg(
    F.count("*").alias("rows"),
    F.sum(F.when(F.col("contract_id").isNull(), 1).otherwise(0)).alias("null_keys"),
).first()
if stats.rows != records_received or stats.null_keys:
    candidate.unpersist()
    raise RuntimeError(
        f"DPWH candidate failed reconciliation: {stats.asDict()} "
        f"from {records_received} records"
    )

try:
    runtime.publish_table(candidate, config.BRONZE_SCHEMA, config.TABLE_DPWH)
finally:
    candidate.unpersist()

written = runtime.read_table(spark, config.BRONZE_SCHEMA, config.TABLE_DPWH).count()
if written != records_received:
    raise RuntimeError(f"DPWH Bronze wrote {written}; expected {records_received}")

print(
    f"SUCCESS {config.TABLE_DPWH}: {written} rows -> "
    f"{runtime.table_label(config.BRONZE_SCHEMA, config.TABLE_DPWH)}"
)
