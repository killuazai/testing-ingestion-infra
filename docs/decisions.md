# Decision log

The capstone source document remains authoritative for D-01 through D-13.

## D-14: One code path for local and Databricks testing

- Status: Decided
- Decision: Source and transformation files detect the active runtime. Local runs
  publish Parquet; Databricks runs publish Delta.
- Why: The team can test without consuming workspace quota, then run the same code
  in the final shared workspace.

## D-15: Separate sample tables

- Status: Decided
- Decision: Sample mode writes `_sample` tables.
- Why: A low-cost run must not replace full Bronze or Silver snapshots.

## D-16: City and municipality spatial matching

- Status: Decided for Silver v1
- Decision: Match points against city and municipality boundaries using a Shapely
  spatial index built per Spark partition.
- Why: This answers the region/province allocation questions without broadcasting
  all barangay geometry. Barangay matching can be added only if the dashboard needs
  it and performance is measured.

## D-17: Preserve ambiguous financial fields

- Status: Decided
- Decision: Keep DPWH reported budget, flood ABC, and flood contract cost separate.
- Why: The source document warns that the values may represent different concepts.
  Combining them would create an unsupported business rule.

