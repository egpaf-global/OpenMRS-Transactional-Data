from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def save_metadata(metadata_type, records):

    if not records:
        print(f"No records returned for {metadata_type}")
        return

    df = spark.createDataFrame(records)

    table_name = f"metadata_{metadata_type}"

    (
        df.write
          .format("delta")
          .mode("overwrite")
          .saveAsTable(table_name)
    )

    print(f"Saved {df.count()} rows to {table_name}")