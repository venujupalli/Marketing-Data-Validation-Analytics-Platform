"""ETL pipeline for marketing campaign data."""
from __future__ import annotations

import sys
from pathlib import Path
import re

import pandas as pd
from sqlalchemy import create_engine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_CSV_PATH = DATA_DIR / "sample_marketing_data.csv"
CLEAN_CSV_PATH = DATA_DIR / "sample_marketing_data_clean.csv"
SQLITE_DB_PATH = DATA_DIR / "marketing.db"
TABLE_NAME = "fact_campaigns"


def to_snake_case(name: str) -> str:
    """Convert a column name to snake_case."""
    name = name.strip()
    name = re.sub(r"[\s\-]+", "_", name)
    name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"[^0-9a-zA-Z_]+", "", name)
    return name.lower()


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names to snake_case."""
    rename_map = {col: to_snake_case(col) for col in df.columns}
    return df.rename(columns=rename_map)


def trim_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace from string-like columns."""
    string_cols = df.select_dtypes(include=["object", "string"]).columns
    for col in string_cols:
        df[col] = df[col].astype("string").str.strip()
        df[col] = df[col].replace({"": pd.NA})
    return df


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce column types where possible."""
    return df.convert_dtypes()


def ensure_non_negative(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure numeric columns contain non-negative values."""
    numeric_cols = df.select_dtypes(include=["number"]).columns
    if not numeric_cols.empty:
        df[numeric_cols] = df[numeric_cols].clip(lower=0)
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all cleaning steps to the dataframe."""
    df = standardize_columns(df)
    df = trim_string_columns(df)
    df = coerce_types(df)

    # Fill missing categorical values
    for col in ("medium", "source"):
        if col in df.columns:
            df[col] = df[col].fillna("unknown")

    # Cast date column to datetime when present
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df = ensure_non_negative(df)

    # Drop duplicate records
    df = df.drop_duplicates()

    return df


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    """Generate a summary by source and medium."""
    required_cols = {"source", "medium"}
    if not required_cols.issubset(df.columns):
        return pd.DataFrame()
    summary = (
        df.groupby(["source", "medium"], dropna=False)
        .size()
        .reset_index(name="row_count")
        .sort_values(["row_count", "source", "medium"], ascending=[False, True, True])
    )
    return summary


def load_data(csv_path: Path) -> pd.DataFrame:
    """Load the raw CSV file."""
    if not csv_path.exists():
        raise FileNotFoundError(f"Input CSV not found at {csv_path}")
    try:
        df = pd.read_csv(csv_path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    return df


def write_outputs(df: pd.DataFrame) -> None:
    """Persist cleaned data to SQLite and CSV."""
    CLEAN_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(CLEAN_CSV_PATH, index=False)

    if df.empty and not df.columns.size:
        # Skip database write when there is no schema information.
        return

    engine = create_engine(f"sqlite:///{SQLITE_DB_PATH}")
    with engine.begin() as connection:
        df.to_sql(TABLE_NAME, connection, if_exists="replace", index=False)


def print_summary(raw_count: int, cleaned_df: pd.DataFrame) -> None:
    """Print row counts and a summary by source/medium."""
    cleaned_count = len(cleaned_df)
    print(f"Raw rows: {raw_count}")
    print(f"Cleaned rows: {cleaned_count}")

    summary_df = summarize(cleaned_df)
    if summary_df.empty:
        print("No source/medium summary available (required columns missing).")
    else:
        print("Summary by source and medium:")
        print(summary_df.to_string(index=False))


def main() -> None:
    """Run the ETL pipeline."""
    try:
        raw_df = load_data(RAW_CSV_PATH)
    except FileNotFoundError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)

    raw_count = len(raw_df)

    if raw_df.empty:
        cleaned_df = raw_df
    else:
        cleaned_df = clean_dataframe(raw_df)

    write_outputs(cleaned_df)
    print_summary(raw_count, cleaned_df)


if __name__ == "__main__":
    main()
