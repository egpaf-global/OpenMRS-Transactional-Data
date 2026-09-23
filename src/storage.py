"""
Storage utilities for Databricks Delta tables.

Responsible for:
    - Saving metadata
    - Saving transactional data
    - Merge (upsert) support
    - Table existence
    - Schema inference
    - Single-column and compound merge keys
"""

from datetime import date, datetime

from delta.tables import DeltaTable

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
    """
    Return the fully-qualified Unity Catalog table name.

    Example:
        programsdev.malawi.encounter
    """

    return f"{TARGET_SCHEMA}.{table_name}"


def table_exists(table_name: str) -> bool:
    """
    Return True if the Delta table exists.
    """

    return spark.catalog.tableExists(
        qualified_table(table_name)
    )


def get_table_names():
    """
    Return all tables in the configured catalog/schema.
    """

    tables = spark.sql(
        f"SHOW TABLES IN {CATALOG}.{SCHEMA}"
    )

    return [
        table.tableName
        for table in tables.collect()
    ]


def get_merge_keys(table_name: str) -> list[str]:
    """
    Return the configured merge keys for a transactional table.

    Supports both:

        "keys": ["encounter_id"]

    and:

        "keys": ["encounter_id", "site_id"]

    The latter represents a compound logical key.
    """

    for table in TRANSACTION_TABLES:

        if table["table"] == table_name:

            keys = table.get("keys")

            if not keys:
                raise ValueError(
                    f"No merge keys configured for '{table_name}'."
                )

            if isinstance(keys, str):
                keys = [keys]

            return keys

    raise ValueError(
        f"No merge keys configured for '{table_name}'."
    )


def build_merge_condition(
    table_name: str,
    target_alias: str = "target",
    source_alias: str = "source",
) -> str:
    """
    Build the Delta MERGE condition.

    For a single key:

        target.patient_id = source.patient_id

    For compound keys:

        target.encounter_id = source.encounter_id
        AND target.site_id = source.site_id
    """

    keys = get_merge_keys(table_name)

    conditions = [
        f"{target_alias}.{key} = {source_alias}.{key}"
        for key in keys
    ]

    return " AND ".join(conditions)


# ============================================================
# Schema inference
# ============================================================

def _infer_spark_type(value):
    """
    Infer the Spark data type from a Python value.
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
    Build a Spark schema from incoming records.
    """

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
                True,
            )
        )

    return schema


# ============================================================
# DataFrame creation
# ============================================================

def _create_dataframe(table_name, records):
    """
    Create a Spark DataFrame using the existing Delta table schema
    where available.

    If the table does not yet exist, infer the schema from the
    incoming records.
    """

    full_table = qualified_table(table_name)

    if table_exists(table_name):

        schema = spark.table(
            full_table
        ).schema

    else:

        schema = _build_schema(
            records
        )

    return spark.createDataFrame(
        records,
        schema=schema,
    )


# ============================================================
# Append
# ============================================================

def append_records(table_name, records):
    """
    Append records to a Delta table.

    Used primarily for metadata tables.

    No merge/upsert logic is performed here.
    """

    if not records:
        return 0

    df = _create_dataframe(
        table_name,
        records,
    )

    writer = (
        df.write
        .format("delta")
        .mode("append")
    )

    if ENABLE_SCHEMA_MERGE:

        writer = writer.option(
            "mergeSchema",
            "true",
        )

    writer.saveAsTable(
        qualified_table(table_name)
    )

    print(
        f"✓ {table_name}: "
        f"appended {len(records):,} records"
    )

    return len(records)


# ============================================================
# Merge / Upsert
# ============================================================

def merge_records(table_name, records):
    """
    Merge transactional records into a Delta table.

    The merge condition is generated from the configured
    merge keys.

    Example:

        keys = ["encounter_id"]

    produces:

        target.encounter_id = source.encounter_id


    Compound key example:

        keys = ["encounter_id", "site_id"]

    produces:

        target.encounter_id = source.encounter_id
        AND target.site_id = source.site_id

    Behaviour:

        Existing matching record
            -> UPDATE

        New record
            -> INSERT
    """

    if not records:
        return 0

    full_table = qualified_table(
        table_name
    )

    # --------------------------------------------------------
    # Create table if it does not exist
    # --------------------------------------------------------

    if not table_exists(table_name):

        append_records(
            table_name,
            records,
        )

        print(
            f"✓ {table_name}: "
            f"created and inserted "
            f"{len(records):,} records"
        )

        return len(records)

    # --------------------------------------------------------
    # Get configured merge keys
    # --------------------------------------------------------

    merge_keys = get_merge_keys(
        table_name
    )

    # --------------------------------------------------------
    # Validate that merge keys exist
    # in the incoming data
    # --------------------------------------------------------

    incoming_columns = set(
        records[0].keys()
    )

    missing_keys = [
        key
        for key in merge_keys
        if key not in incoming_columns
    ]

    if missing_keys:

        raise ValueError(
            f"Missing merge key(s) "
            f"{missing_keys} in incoming data "
            f"for table '{table_name}'."
        )

    # --------------------------------------------------------
    # Create source DataFrame
    # --------------------------------------------------------

    source_df = _create_dataframe(
        table_name,
        records,
    )

    # --------------------------------------------------------
    # Get Delta table
    # --------------------------------------------------------

    delta_table = DeltaTable.forName(
        spark,
        full_table,
    )

    # --------------------------------------------------------
    # Build MERGE condition
    # --------------------------------------------------------

    merge_condition = build_merge_condition(
        table_name,
        target_alias="target",
        source_alias="source",
    )

    print(
        f"→ {table_name}: "
        f"MERGE keys = {merge_keys}"
    )

    print(
        f"→ {table_name}: "
        f"MERGE condition = {merge_condition}"
    )

    # --------------------------------------------------------
    # Execute MERGE
    # --------------------------------------------------------

    (
        delta_table.alias("target")

        .merge(
            source_df.alias("source"),
            merge_condition,
        )

        .whenMatchedUpdateAll()

        .whenNotMatchedInsertAll()

        .execute()
    )

    print(
        f"✓ {table_name}: "
        f"merged {len(records):,} records"
    )

    return len(records)


# ============================================================
# Public API
# ============================================================

def save_metadata(metadata_type, records):
    """
    Save metadata records.

    Metadata is append-only.
    """

    return append_records(
        metadata_type,
        records,
    )


def save_transactions(table_name, records):
    """
    Save transactional records.

    Transactional data uses Delta MERGE/UPSERT
    for idempotent synchronization.

    Merge keys are defined in TRANSACTION_TABLES.
    """

    return merge_records(
        table_name,
        records,
    )