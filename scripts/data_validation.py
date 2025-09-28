"""Data validation script for marketing campaign data."""
from __future__ import annotations

import csv
import sqlite3
import sys
from datetime import datetime, date, timezone
from pathlib import Path
from statistics import pstdev, mean
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

RUN_TIMESTAMP = datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_fact_campaigns(db_path: str) -> List[Dict[str, Any]]:
    """Load the fact_campaigns table from the SQLite database."""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM fact_campaigns")
        rows = cursor.fetchall()
    return [dict(row) for row in rows]


def is_missing_value(value: Any) -> bool:
    """Return True if the value should be treated as missing taxonomy."""
    if value is None:
        return True
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return True
        if stripped.lower() == "unknown":
            return True
    return False


def build_sample_rows(records: Iterable[Dict[str, Any]], limit: int = 5) -> str:
    """Return a formatted sample string of up to ``limit`` records."""
    preferred_id_cols = ["campaign_id", "ad_id", "ad_group_id", "id"]
    samples: List[str] = []

    for record in list(records)[:limit]:
        parts: List[str] = []
        for column in preferred_id_cols:
            if column in record and record[column] is not None:
                parts.append(f"{column}={record[column]}")
        if "date" in record and record["date"] is not None:
            parts.append(f"date={record['date']}")
        if not parts:
            parts.append(str({k: record[k] for k in sorted(record.keys())}))
        samples.append("|".join(parts))

    return "; ".join(samples)


def taxonomy_validation(data: Sequence[Dict[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    """Check for missing taxonomy in source or medium columns."""
    if not data:
        return 0, []

    sample_record = data[0]
    if "source" not in sample_record or "medium" not in sample_record:
        return len(data), list(data[:5])

    flagged: List[Dict[str, Any]] = []
    for record in data:
        if is_missing_value(record.get("source")) or is_missing_value(record.get("medium")):
            flagged.append(record)

    return len(flagged), flagged


def coerce_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if text == "":
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def outlier_validation(data: Sequence[Dict[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    """Detect outliers based on Z-score for key numeric metrics."""
    metrics = ["spend", "impressions", "clicks", "conversions", "revenue"]
    if not data:
        return 0, []

    flagged_indices: set[int] = set()
    for metric in metrics:
        values_with_index: List[Tuple[int, float]] = []
        for idx, record in enumerate(data):
            numeric_value = coerce_float(record.get(metric))
            if numeric_value is not None:
                values_with_index.append((idx, numeric_value))

        if len(values_with_index) < 2:
            continue

        values = [val for _, val in values_with_index]
        avg = mean(values)
        std_dev = pstdev(values)
        if std_dev == 0:
            continue

        for idx, value in values_with_index:
            z_score = (value - avg) / std_dev
            if abs(z_score) >= 3:
                flagged_indices.add(idx)

    flagged_records = [data[idx] for idx in sorted(flagged_indices)]
    return len(flagged_records), flagged_records


def parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    text = str(value).strip()
    if text == "":
        return None

    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return None


def latency_validation(data: Sequence[Dict[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    """Check if the latest date in the data is older than three days."""
    if not data:
        return 1, []

    if "date" not in data[0]:
        return 1, list(data[:1])

    latest_date: Optional[date] = None
    latest_index: Optional[int] = None
    for idx, record in enumerate(data):
        parsed = parse_date(record.get("date"))
        if parsed is None:
            continue
        if latest_date is None or parsed > latest_date:
            latest_date = parsed
            latest_index = idx

    if latest_date is None:
        return 1, list(data[:1])

    today = datetime.now(timezone.utc).date()
    if (today - latest_date).days > 3:
        if latest_index is not None:
            return 1, [data[latest_index]]
        return 1, []

    return 0, []


def write_results(output_path: str, results: Sequence[Dict[str, Any]]) -> None:
    fieldnames = ["rule", "severity", "count", "sample_rows", "run_timestamp"]
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow(result)


def run_validations(db_path: str, output_path: str) -> bool:
    """Run all validations and write findings to CSV.

    Returns ``True`` if any critical findings were detected.
    """
    results: List[Dict[str, Any]] = []
    critical_found = False

    try:
        data = load_fact_campaigns(db_path)
    except sqlite3.OperationalError as exc:
        results.append(
            {
                "rule": "fact_campaigns_table_access",
                "severity": "critical",
                "count": 1,
                "sample_rows": f"Error: {exc}",
                "run_timestamp": RUN_TIMESTAMP,
            }
        )
        critical_found = True
    except Exception as exc:
        results.append(
            {
                "rule": "fact_campaigns_load_failure",
                "severity": "critical",
                "count": 1,
                "sample_rows": f"Error: {exc}",
                "run_timestamp": RUN_TIMESTAMP,
            }
        )
        critical_found = True
    else:
        tax_count, tax_records = taxonomy_validation(data)
        if tax_count > 0:
            results.append(
                {
                    "rule": "missing_taxonomy",
                    "severity": "critical",
                    "count": tax_count,
                    "sample_rows": build_sample_rows(tax_records),
                    "run_timestamp": RUN_TIMESTAMP,
                }
            )
            critical_found = True

        outlier_count, outlier_records = outlier_validation(data)
        if outlier_count > 0:
            results.append(
                {
                    "rule": "zscore_outliers",
                    "severity": "warning",
                    "count": outlier_count,
                    "sample_rows": build_sample_rows(outlier_records),
                    "run_timestamp": RUN_TIMESTAMP,
                }
            )

        latency_count, latency_records = latency_validation(data)
        if latency_count > 0:
            sample_text = (
                build_sample_rows(latency_records)
                if latency_records
                else "No recent data available"
            )
            results.append(
                {
                    "rule": "data_latency",
                    "severity": "critical",
                    "count": latency_count,
                    "sample_rows": sample_text,
                    "run_timestamp": RUN_TIMESTAMP,
                }
            )
            critical_found = True

    write_results(output_path, results)
    return critical_found


def main() -> None:
    """Entrypoint for command-line execution."""
    db_path = "data/marketing.db"
    output_path = "data/validation_results.csv"

    try:
        critical_found = run_validations(db_path, output_path)
    except Exception as exc:  # safeguard to capture unexpected errors
        print(f"Validation failed: {exc}", file=sys.stderr)
        sys.exit(1)

    sys.exit(1 if critical_found else 0)


if __name__ == "__main__":
    main()
