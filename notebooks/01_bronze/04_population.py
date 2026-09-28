# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: 2024 population
# MAGIC Uses the official PSA OpenSTAT table, whose rows carry 10-digit
# MAGIC geographic codes.

# COMMAND ----------

import json
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
from pyspark import StorageLevel
from pyspark.sql import functions as F

from src import config, ingestion, runtime

spark = runtime.get_spark()

# COMMAND ----------

load_ts = ingestion.utc_now()
run_id = ingestion.make_run_id(load_ts)
landing_directory = ingestion.ensure_directory(
    runtime.landing_root() / "population_2024" / run_id
)
requested_file = runtime.parameter(
    "population_raw_file",
    "",
    "Previously landed OpenSTAT JSON response",
).strip()

payload = None
raw = None
source_file = None
if requested_file:
    source_file = Path(requested_file)
    raw = source_file.read_bytes()
    payload = json.loads(raw.decode("utf-8-sig"))
else:
    session = ingestion.retry_session(config.USER_AGENT)
    try:
        payload, raw = ingestion.request_json(
            session,
            "POST",
            config.POPULATION_OPENSTAT_URL,
            body=config.population_query(),
            timeout=(config.REQUEST_CONNECT_TIMEOUT, config.REQUEST_READ_TIMEOUT),
        )
    except (requests.RequestException, ValueError) as error:
        available = sorted(
            (runtime.landing_root() / "population_2024").glob("**/*.json")
        )
        raise RuntimeError(
            "PSA OpenSTAT is unavailable from this runtime. Run this notebook locally "
            "once, upload its response.json, then set population_raw_file. "
            f"Available JSON files: {[str(path) for path in available]}"
        ) from error
    source_file = landing_directory / "response.json"
    ingestion.write_bytes(source_file, raw)

if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
    raise RuntimeError("OpenSTAT response is missing the data array")
columns = payload.get("columns")
if not isinstance(columns, list) or len(columns) < 3:
    raise RuntimeError("OpenSTAT response is missing its column contract")
rows = payload["data"]
population_contract = config.source_contracts()["population"]
minimum_rows = int(population_contract["minimum_expected_rows"])
if len(rows) < minimum_rows:
    raise RuntimeError(
        f"OpenSTAT returned {len(rows)} rows; expected at least {minimum_rows}"
    )

records = []
seen_codes = set()
for row in rows:
    if not isinstance(row, dict):
        raise TypeError("OpenSTAT data rows must be objects")
    key = row.get("key")
    values = row.get("values")
    if not isinstance(key, list) or len(key) != 2:
        raise RuntimeError(f"Invalid OpenSTAT key: {key!r}")
    if not isinstance(values, list) or len(values) != 1:
        raise RuntimeError(f"Invalid OpenSTAT value: {values!r}")
    psgc_code = str(key[0]).zfill(10)
    if len(psgc_code) != 10 or not psgc_code.isdigit():
        raise RuntimeError(f"Invalid OpenSTAT geographic code: {psgc_code!r}")
    if psgc_code in seen_codes:
        raise RuntimeError(f"Duplicate OpenSTAT geographic code: {psgc_code}")
    seen_codes.add(psgc_code)
    try:
        population = int(str(values[0]).replace(",", ""))
    except ValueError as error:
        raise RuntimeError(
            f"Invalid population for {psgc_code}: {values[0]!r}"
        ) from error
    records.append(
        (
            psgc_code,
            str(key[1]),
            population,
            json.dumps(row, ensure_ascii=False, separators=(",", ":")),
        )
    )

source_sha256 = ingestion.sha256(raw)
source_df = spark.createDataFrame(
    records,
    "psgc_code string, parameter_code string, population long, "
    "source_record_json string",
)
candidate = source_df.select(
    "psgc_code",
    "parameter_code",
    "population",
    "source_record_json",
    F.lit(int(population_contract["release_year"]))
    .cast("int")
    .alias("population_year"),
    F.lit(source_file.name).alias("source_file"),
    F.lit(source_sha256).alias("source_sha256"),
    F.lit(load_ts).cast("timestamp").alias("load_ts"),
    F.lit(run_id).alias("run_id"),
    F.lit("Philippine Statistics Authority OpenSTAT").alias("source_system"),
    F.lit(config.POPULATION_OPENSTAT_URL).alias("source_url"),
)
candidate.persist(StorageLevel.DISK_ONLY)
stats = candidate.agg(
    F.count("*").alias("rows"),
    F.sum(F.when(F.col("population") < 0, 1).otherwise(0)).alias("negative_values"),
).first()
if stats.rows != len(records) or stats.negative_values:
    candidate.unpersist()
    raise RuntimeError(f"Population Bronze contract failed: {stats.asDict()}")

try:
    runtime.publish_table(candidate, config.BRONZE_SCHEMA, config.TABLE_POPULATION)
finally:
    candidate.unpersist()

written = runtime.read_table(
    spark, config.BRONZE_SCHEMA, config.TABLE_POPULATION
).count()
if written != len(records):
    raise RuntimeError(f"Population Bronze wrote {written}; expected {len(records)}")
print(
    f"SUCCESS {config.TABLE_POPULATION}: {written} rows -> "
    f"{runtime.table_label(config.BRONZE_SCHEMA, config.TABLE_POPULATION)}"
)
