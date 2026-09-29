"""
Storage utilities for Databricks Delta tables.

Responsible for:
    - Saving metadata
    - Saving transactional data
    - Merge / upsert support
    - Table existence
    - Schema inference
    - Single-column and compound merge keys

Spark session handling:
    - Databricks provides the Spark session.
    - This module does NOT create a new Spark session.
    - The notebook must call configure_spark(spark).
"""

from datetime import date, datetime

from delta.tables import DeltaTable

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


# ============================================================
# Spark Session
# ============================================================

# The Spark session is supplied by Databricks.
#
# DO NOT do:
#
# spark = SparkSession.builder.getOrCreate()
#
# because this can cause Spark Connect session problems when
# modules are reloaded in Databricks.

spark = None


def configure_spark(session):
    """
    Configure this module to use the Spark session supplied
    by the Databricks notebook.
    """

    global spark

    if session is None:
        raise ValueError(
            "A valid Spark session must be supplied."
        )

    spark = session

    print(
        "✓ storage.py configured with Databricks Spark session"
    )


def _require_spark():
    """
    Ensure the module has been configured with a Spark session.
    """

    if spark is None:
        raise RuntimeError(
            "Spark session has not been configured. "
            "Call src.storage.configure_spark(spark) "
            "from the Databricks notebook first."
        )

    return spark


# ============================================================
# Helpers
# ============================================================

def qualified_table(table_name: str) -> str:
    """
    Return the fully-qualified Unity Catalog table name.

    Example:

        programsdev.malawi.patient
    """

    return f"{TARGET_SCHEMA}.{table_name}"


def table_exists(table_name: str) -> bool:
    """
    Return True if the Delta table exists.
    """

    session = _require_spark()

    return session.catalog.tableExists(
        qualified_table(table_name)
    )


def get_table_names():
    """
    Return all tables in the configured catalog/schema.
    """

    session = _require_spark()

    tables = session.sql(
        f"SHOW TABLES IN {CATALOG}.{SCHEMA}"
    )

    return [
        table.tableName
        for table in tables.collect()
    ]


# ============================================================
# Merge Keys
# ============================================================

def get_merge_keys(table_name: str) -> list[str]:
    """
    Return the configured merge keys for a transactional table.

    Supports:

        "keys": ["patient_id"]

    and compound keys:

        "keys": ["encounter_id", "site_id"]
    """

    for table in TRANSACTION_TABLES:

        if table["table"] == table_name:

            keys = table.get("keys")

            if not keys:
                raise ValueError(
                    f"No merge keys configured for "
                    f"'{table_name}'."
                )

            if isinstance(keys, str):
                keys = [keys]

            return keys

    raise ValueError(
        f"No merge keys configured for "
        f"'{table_name}'."
    )


def build_merge_condition(
    table_name: str,
    target_alias: str = "target",
    source_alias: str = "source",
) -> str:
    """
    Build the Delta MERGE condition.

    Single key:

        target.patient_id = source.patient_id

    Compound key:

        target.encounter_id = source.encounter_id
        AND target.site_id = source.site_id
    """

    keys = get_merge_keys(table_name)

    conditions = [
        f"{target_alias}.{key} = "
        f"{source_alias}.{key}"
        for key in keys
    ]

    return " AND ".join(conditions)


# ============================================================
# Schema Inference
# ============================================================

def _infer_spark_type(value):
    """
    Infer Spark SQL type from a Python value.
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

    The first non-null value found for each field is used
    to determine the Spark type.
    """

    if not records:
        raise ValueError(
            "Cannot build schema from empty records."
        )

    fields = {}

    for record in records:

        for key, value in record.items():

            if key in fields:
                continue

            if value is not None:
                fields[key] = _infer_spark_type(
                    value
                )

    schema = StructType()

    for key in records[0].keys():

        schema.add(
            StructField(
                key,
                fields.get(
                    key,
                    StringType()
                ),
                True,
            )
        )

    return schema


# ============================================================
# DataFrame Creation
# ============================================================

def _create_dataframe(
    table_name,
    records,
):
    """
    Create a Spark DataFrame.

    If the Delta table already exists, use its schema.

    Otherwise infer the schema from the incoming records.
    """

    session = _require_spark()

    if not records:
        raise ValueError(
            f"No records supplied for "
            f"'{table_name}'."
        )

    full_table = qualified_table(
        table_name
    )

    if table_exists(table_name):

        schema = session.table(
            full_table
        ).schema

    else:

        schema = _build_schema(
            records
        )

    return session.createDataFrame(
        records,
        schema=schema,
    )


# ============================================================
# Append
# ============================================================

def append_records(
    table_name,
    records,
):
    """
    Append records to a Delta table.

    Primarily used for metadata tables.
    """

    if not records:
        print(
            f"⚠ {table_name}: "
            f"no records to append"
        )

        return 0

    session = _require_spark()

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

def merge_records(
    table_name,
    records,
):
    """
    Merge transactional records into a Delta table.

    Existing records:
        UPDATE

    New records:
        INSERT
    """

    if not records:

        print(
            f"⚠ {table_name}: "
            f"no records to merge"
        )

        return 0

    session = _require_spark()

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
    # Merge keys
    # --------------------------------------------------------

    merge_keys = get_merge_keys(
        table_name
    )

    # --------------------------------------------------------
    # Validate merge keys
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
    # Delta table
    # --------------------------------------------------------

    delta_table = DeltaTable.forName(
        session,
        full_table,
    )

    # --------------------------------------------------------
    # Merge condition
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
        f"MERGE condition = "
        f"{merge_condition}"
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

def save_metadata(
    metadata_type,
    records,
):
    """
    Save metadata records.

    Metadata is appended to the target table.
    """

    return append_records(
        metadata_type,
        records,
    )


def save_transactions(
    table_name,
    records,
):
    """
    Save transactional records.

    Transactional data uses Delta MERGE/UPSERT
    for idempotent synchronization.
    """

    return merge_records(
        table_name,
        records,
    )