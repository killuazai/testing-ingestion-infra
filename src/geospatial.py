"""Cost-bounded point-to-boundary matching for Silver projects."""

from __future__ import annotations


def assign_psgc_codes(spark, projects, boundary_rows: list, maximum_features: int):
    """Match project points using one executor-local STRtree per partition."""
    from pyspark.sql import types as T

    if not boundary_rows:
        raise RuntimeError("No city or municipality boundaries are available")
    if len(boundary_rows) > maximum_features:
        raise RuntimeError(
            f"Refusing to broadcast {len(boundary_rows)} boundaries; "
            f"configured maximum is {maximum_features}"
        )

    payload = [
        (row["psgc_code"], row["geometry_json"])
        for row in boundary_rows
        if row["psgc_code"] and row["geometry_json"]
    ]
    broadcast_boundaries = spark.sparkContext.broadcast(payload)
    output_schema = T.StructType(
        [
            *projects.schema.fields,
            T.StructField("psgc_code", T.StringType(), True),
            T.StructField("place_match_count", T.IntegerType(), False),
        ]
    )

    def match_partition(rows):
        import json

        from shapely import make_valid
        from shapely.geometry import Point, shape
        from shapely.strtree import STRtree

        codes = []
        geometries = []
        for psgc_code, geometry_json in broadcast_boundaries.value:
            geometry = shape(json.loads(geometry_json))
            if not geometry.is_valid:
                geometry = make_valid(geometry)
            if not geometry.is_empty:
                codes.append(psgc_code)
                geometries.append(geometry)
        tree = STRtree(geometries)

        for row in rows:
            values = tuple(row)
            longitude = row["longitude"]
            latitude = row["latitude"]
            if longitude is None or latitude is None:
                yield (*values, None, 0)
                continue
            point = Point(float(longitude), float(latitude))
            indexes = tree.query(point, predicate="intersects")
            matches = sorted({codes[int(index)] for index in indexes})
            matched_code = matches[0] if len(matches) == 1 else None
            yield (*values, matched_code, len(matches))

    return spark.createDataFrame(
        projects.rdd.mapPartitions(match_partition), output_schema
    )
