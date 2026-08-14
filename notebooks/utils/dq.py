"""
Data Quality utilities.

Usage in any notebook (after %run 00_config.py):
    import sys; sys.path.insert(0, "/home/jovyan/work")
    from utils.dq import dq_check, write_dq_log
"""
import uuid
from datetime import datetime
from pyspark.sql import Row


def dq_check(run_id: str, layer: str, table: str,
             check: str, expected, actual) -> Row:
    """
    Builds one DQ check result Row.
    A PASS is when str(actual) == str(expected).
    """
    status = "PASS" if str(actual) == str(expected) else "FAIL"
    print(f"  [{status}] {layer}.{table} | {check} | expected={expected} actual={actual}")
    return Row(
        run_id=run_id,
        layer=layer,
        table_name=table,
        check_name=check,
        expected_value=str(expected),
        actual_value=str(actual),
        status=status,
        run_ts=datetime.utcnow(),
    )


def write_dq_log(spark, checks: list, gold_path: str) -> None:
    """
    Appends a list of dq_check() Rows to the DQ log Delta table.
    Raises if any check has status FAIL (fails fast on critical violations).
    """
    df = spark.createDataFrame(checks)
    df.write.format("delta").mode("append").save(f"{gold_path}/dq_log")

    failures = [c for c in checks if c.status == "FAIL"]
    if failures:
        names = ", ".join(c.check_name for c in failures)
        raise AssertionError(f"DQ FAILED — {len(failures)} check(s): {names}")

    print(f"  DQ log: {len(checks)} check(s) written, all PASS")
