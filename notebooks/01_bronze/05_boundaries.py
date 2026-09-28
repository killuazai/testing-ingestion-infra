# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: BetterGov boundary maps
# MAGIC Reads manually landed hierarchical GeoJSON and preserves geometry and
# MAGIC properties.

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

from pyspark import StorageLevel
from pyspark.sql import functions as F

from src import config, ingestion, runtime

spark = runtime.get_spark()

# COMMAND ----------

source_directory = Path(
    runtime.parameter(
        "boundary_directory",
        str(runtime.landing_root() / "boundaries" / "bettergov"),
        "BetterGov GeoJSON directory",
    )
)
source_files = ingestion.discover_files(source_directory, {".geojson", ".json"})
source_paths = [str(path) for path in source_files]
load_ts = ingestion.utc_now()
run_id = ingestion.make_run_id(load_ts)

raw_files = (
    spark.read.option("multiLine", True)
    .json(source_paths)
    .withColumn("source_path", F.input_file_name())
)
raw_files.persist(StorageLevel.DISK_ONLY)
contracts = raw_files.select(
    F.regexp_extract("source_path", r"([^/]+)$", 1).alias("source_file"),
    F.col("type").alias("geojson_type"),
    F.col("metadata.psgc_type").alias("administrative_level"),
    F.col("metadata.psgc_version").alias("psgc_version"),
    F.col("metadata.namria_version").alias("boundary_version"),
    F.col("metadata.feature_count").cast("long").alias("declared_features"),
    F.size("features").cast("long").alias("parsed_features"),
).collect()

if len(contracts) != len(source_files):
    raw_files.unpersist()
    raise RuntimeError(
        f"Read {len(contracts)} GeoJSON documents from {len(source_files)} files"
    )
records_received = 0
for contract in contracts:
    if contract.geojson_type != "FeatureCollection":
        raw_files.unpersist()
        raise RuntimeError(f"{contract.source_file} is not a FeatureCollection")
    if not contract.administrative_level or not contract.psgc_version:
        raw_files.unpersist()
        raise RuntimeError(f"{contract.source_file} lacks BetterGov metadata")
    if contract.declared_features != contract.parsed_features:
        raw_files.unpersist()
        raise RuntimeError(
            f"{contract.source_file} declares {contract.declared_features} features "
            f"but contains {contract.parsed_features}"
        )
    records_received += contract.parsed_features
if records_received <= 0:
    raw_files.unpersist()
    raise RuntimeError("Boundary files contain no features")

# COMMAND ----------

features = raw_files.select(
    F.explode("features").alias("feature"),
    F.col("metadata.psgc_type").alias("metadata_administrative_level"),
    F.col("metadata.psgc_version").alias("psgc_version"),
    F.col("metadata.namria_version").alias("boundary_version"),
    F.col("source_path"),
)
candidate = features.select(
    F.col("feature.properties.psgc_id").cast("string").alias("source_feature_id"),
    F.lpad(F.col("feature.properties.psgc_code").cast("string"), 10, "0").alias(
        "psgc_code"
    ),
    F.coalesce(
        F.col("feature.properties.psgc_type"),
        F.col("metadata_administrative_level"),
    )
    .cast("string")
    .alias("administrative_level"),
    F.to_json("feature.properties").alias("properties_json"),
    F.to_json("feature.geometry").alias("geometry_json"),
    F.col("psgc_version").cast("string").alias("source_psgc_version"),
    F.col("boundary_version").cast("string").alias("source_boundary_version"),
    F.regexp_extract("source_path", r"([^/]+)$", 1).alias("source_file"),
    F.col("source_path"),
    F.lit(load_ts).cast("timestamp").alias("load_ts"),
    F.lit(run_id).alias("run_id"),
    F.lit("BetterGov hierarchical Philippine boundaries").alias("source_system"),
    F.lit(config.BOUNDARY_PAGE_URL).alias("source_url"),
)
candidate.persist(StorageLevel.DISK_ONLY)
stats = candidate.agg(
    F.count("*").alias("rows"),
    F.sum(
        F.when(
            ~F.col("psgc_code").rlike(r"^[0-9]{10}$")
            | F.col("administrative_level").isNull()
            | F.col("geometry_json").isNull(),
            1,
        ).otherwise(0)
    ).alias("invalid_required"),
).first()
if stats.rows != records_received or stats.invalid_required:
    candidate.unpersist()
    raw_files.unpersist()
    raise RuntimeError(f"Boundary Bronze contract failed: {stats.asDict()}")

manifest = [
    {
        "source_file": path.name,
        "bytes": path.stat().st_size,
        "sha256": ingestion.sha256_file(path),
    }
    for path in source_files
]
ingestion.write_json(
    source_directory / f".manifest_{run_id}.json",
    {
        "run_id": run_id,
        "records_received": records_received,
        "files": manifest,
        "completed_at": ingestion.utc_now(),
    },
)

try:
    runtime.publish_table(candidate, config.BRONZE_SCHEMA, config.TABLE_BOUNDARIES)
finally:
    candidate.unpersist()
    raw_files.unpersist()

written = runtime.read_table(
    spark, config.BRONZE_SCHEMA, config.TABLE_BOUNDARIES
).count()
if written != records_received:
    raise RuntimeError(f"Boundary Bronze wrote {written}; expected {records_received}")
print(
    f"SUCCESS {config.TABLE_BOUNDARIES}: {written} rows -> "
    f"{runtime.table_label(config.BRONZE_SCHEMA, config.TABLE_BOUNDARIES)}"
)
