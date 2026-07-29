"""
Storage utilities for Databricks Delta tables.

Responsible for:
    - Saving metadata
    - Saving transactional data
    - Merge (upsert) support
    - Table existence
    - Schema inference
"""

from datetime import date, datetime

from delta.tables import DeltaTable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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

from src.settings import (
    CATALOG,
    SCHEMA,
    TARGET_SCHEMA,
    TRANSACTION_TABLES,
    ENABLE_SCHEMA_MERGE,
)

spark = SparkSession.builder.getOrCreate()


# ============================================================
# Helpers
# ============================================================

def qualified_table(table_name: str) -> str:
    """Return fully-qualified Unity Catalog table name."""
    return f"{TARGET_SCHEMA}.{table_name}"


def table_exists(table_name: str) -> bool:
    """Return True if the table exists."""
    return spark.catalog.tableExists(
        qualified_table(table_name)
    )


def get_table_names():
    """Return all tables in the configured schema."""
    tables = spark.sql(
        f"SHOW TABLES IN {CATALOG}.{SCHEMA}"
    )

    return [t.tableName for t in tables.collect()]


def get_primary_key(table_name: str) -> str:
    """
    Return the configured primary key.
    """

    for table in TRANSACTION_TABLES:

        if table["table"] == table_name:
            return table["primary_key"]

    raise ValueError(
        f"No primary key configured for '{table_name}'."
    )


# ============================================================
# Schema inference
# ============================================================

def _infer_spark_type(value):

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

    fields = {}

    for record in records:

        for key, value in record.items():

            if key not in fields and value is not None:
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


# ============================================================
# DataFrame creation
# ============================================================

def _create_dataframe(table_name, records):

    full_table = qualified_table(table_name)

    if table_exists(table_name):

        schema = spark.table(full_table).schema

    else:

        schema = _build_schema(records)

    return spark.createDataFrame(
        records,
        schema=schema
    )


# ============================================================
# Append
# ============================================================

def append_records(table_name, records):

    if not records:
        return 0

    df = _create_dataframe(
        table_name,
        records
    )

    writer = (
        df.write
          .format("delta")
          .mode("append")
    )

    if ENABLE_SCHEMA_MERGE:
        writer = writer.option(
            "mergeSchema",
            "true"
        )

    writer.saveAsTable(
        qualified_table(table_name)
    )

    print(
        f"✓ {table_name}: appended {len(records):,} records"
    )

    return len(records)


# ============================================================
# Merge (Upsert)
# ============================================================

def merge_records(table_name, records):

    if not records:
        return 0

    full_table = qualified_table(table_name)

    if not table_exists(table_name):

        append_records(
            table_name,
            records
        )

        return len(records)

    primary_key = get_primary_key(
        table_name
    )

    source_df = _create_dataframe(
        table_name,
        records
    )

    delta_table = DeltaTable.forName(
        spark,
        full_table
    )

    (
        delta_table.alias("target")

        .merge(
            source_df.alias("source"),
            f"target.{primary_key} = source.{primary_key}"
        )

        .whenMatchedUpdateAll()

        .whenNotMatchedInsertAll()

        .execute()
    )

    print(
        f"✓ {table_name}: merged {len(records):,} records"
    )

    return len(records)


# ============================================================
# Public API
# ============================================================

def save_metadata(metadata_type, records):
    """
    Metadata is append-only.
    """

    return append_records(
        metadata_type,
        records
    )


def save_transactions(table_name, records):
    """
    Transactional data uses UPSERT
    for idempotent synchronization.
    """

    return merge_records(
        table_name,
        records
    )