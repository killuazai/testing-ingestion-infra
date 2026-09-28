"""Small data-quality helpers with explicit stop and warning severities."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Check:
    """One aggregate data-quality check."""

    name: str
    column: str
    failure_expression: str
    severity: str


def evaluate(dataframe, checks: list[Check]) -> list[dict]:
    """Evaluate all row checks in one Spark scan."""
    from pyspark.sql import functions as F

    aggregations = [F.count(F.lit(1)).alias("total_rows")]
    for index, check in enumerate(checks):
        aggregations.append(
            F.sum(F.when(F.expr(check.failure_expression), 1).otherwise(0)).alias(
                f"failed_{index}"
            )
        )
    result = dataframe.agg(*aggregations).first()
    total_rows = int(result.total_rows)
    rows = []
    for index, check in enumerate(checks):
        failures = int(result[f"failed_{index}"] or 0)
        if failures == 0:
            status = "PASS"
        elif check.severity == "stop":
            status = "FAIL"
        else:
            status = "WARN"
        rows.append(
            {
                "column": check.column,
                "check_name": check.name,
                "failed_rows": failures,
                "total_rows": total_rows,
                "percentage": 0.0 if total_rows == 0 else failures / total_rows * 100,
                "severity": check.severity,
                "status": status,
            }
        )
    return rows


def require_no_failures(results: list[dict]) -> None:
    """Stop after results have been persisted when any stop check failed."""
    failed = [result["check_name"] for result in results if result["status"] == "FAIL"]
    if failed:
        raise RuntimeError(f"Stop checks failed: {failed}")
