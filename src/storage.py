"""
Storage utilities for Databricks Delta tables.

Responsible for:
    - Saving metadata
    - Saving transactional data
    - Checking table existence
    - Listing tables
"""

from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.settings import CATALOG, SCHEMA

spark = SparkSession.builder.getOrCreate()


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def qualified_table(table_name: str) -> str:
    """
    Returns the fully qualified Unity Catalog table name.

    Example:
        programsdev.malawi.patient
    """
    return f"{CATALOG}.{SCHEMA}.{table_name}"


def get_table_names():
    """
    Return all tables within the configured schema.
    """
    tables = spark.sql(
        f"SHOW TABLES IN {CATALOG}.{SCHEMA}"
    )

    return [row.tableName for row in tables.collect()]


def table_exists(table_name: str) -> bool:
    """
    Check whether a table exists.
    """
    return spark.catalog.tableExists(
        qualified_table(table_name)
    )


# ---------------------------------------------------------------------
# Schema inference
# ---------------------------------------------------------------------

def _infer_spark_type(value):
    """
    Infer a Spark SQL type from a Python value.
    """

    if isinstance(value, bool):
        return BooleanType()

    if isinstance(value, int):
        return LongType()

    if isinstance(value, float):
        return DoubleType()

    if isinstance(value, datetime):
        return TimestampType()

    if isinstance(value, date):
        return DateType()

    return StringType()


def _build_schema(records):
    """
    Build schema from first non-null values.
    """

    fields = {}

    for record in records:

        for key, value in record.items():

            if key in fields:
                continue

            if value is not None:
                fields[key] = _infer_spark_type(value)

    schema = StructType()

    for key in records[0].keys():

        schema.add(
            StructField(
                key,
                fields.get(key, StringType()),
                True
            )
        )

    return schema


# ---------------------------------------------------------------------
# Save helper
# ---------------------------------------------------------------------

def _save_table(table_name: str, records: list, mode: str):
    """
    Save records to Delta.
    """

    if not records:
        print(f"No records returned for '{table_name}'.")
        return

    full_table_name = qualified_table(table_name)

    # ----------------------------------------------------------
    # Reuse existing schema if table already exists
    # ----------------------------------------------------------

    if spark.catalog.tableExists(full_table_name):
        schema = spark.table(full_table_name).schema
    else:
        schema = _build_schema(records)

    df = spark.createDataFrame(
        records,
        schema=schema
    )

    print("\nSchema being written:")
    df.printSchema()

    (
        df.write
          .format("delta")
          .mode(mode)
          .saveAsTable(full_table_name)
    )

    print(
        f"Successfully saved {len(records):,} records "
        f"to {full_table_name}"
    )


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------

def save_metadata(metadata_type: str, records: list):
    """
    Save metadata.

    First page creates the table.
    Remaining pages append using the existing schema.
    """

    mode = (
        "append"
        if table_exists(metadata_type)
        else "overwrite"
    )

    _save_table(
        table_name=metadata_type,
        records=records,
        mode=mode
    )


def save_transactions(table_name: str, records: list):
    """
    Save transactional data.

    Always append.
    """

    _save_table(
        table_name=table_name,
        records=records,
        mode="append"
    )