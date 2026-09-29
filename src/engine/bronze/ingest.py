"""
Bronze Ingestion: Appends raw events with audit metadata and rescued data columns.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Explicit target schema for incoming raw events
RAW_TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("op_type", StringType(), True),  # 'I' (Insert), 'U' (Update), 'D' (Delete)
    StructField("event_timestamp", TimestampType(), True),
])

def ingest_to_bronze(
    raw_df: DataFrame,
    bronze_table_path: str,
    checkpoint_path: str,
) -> None:
    """
    Enriches raw ingestion with metadata, isolates parsing failures, 
    and writes to an append-only Delta Bronze table.
    """
    bronze_df = (
        raw_df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        # Rescued data placeholder: catches unparsed or malformed raw rows
        .withColumn(
            "_rescued_data",
            F.when(F.col("transaction_id").isNull(), F.lit("Missing transaction_id"))
            .otherwise(F.lit(None).cast(StringType())),
        )
    )

    (
        bronze_df.write.format("delta")
        .mode("append")
        .option("checkpointLocation", checkpoint_path)
        .save(bronze_table_path)
    )
