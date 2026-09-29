"""
Silver Layer: Ingests CDC batches, resolves duplicates via windows, 
and applies ACID MERGE into Delta.
"""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

def deduplicate_cdc_batch(batch_df: DataFrame) -> DataFrame:
    """
    Deduplicates intra-batch events for the same primary key,
    keeping strictly the latest state by event_timestamp.
    """
    window_spec = Window.partitionBy("transaction_id").orderBy(
        F.col("event_timestamp").desc(),
        F.col("_ingested_at").desc()
    )

    return (
        batch_df.withColumn("row_rank", F.row_number().over(window_spec))
        .filter(F.col("row_rank") == 1)
        .drop("row_rank")
    )

def merge_cdc_to_silver(
    spark_session,
    micro_batch_df: DataFrame,
    silver_table_path: str,
) -> None:
    """
    Executes an atomic Delta Lake MERGE covering INSERT, UPDATE, and DELETE operations.
    Initializes the target table if it does not yet exist.
    """
    cleaned_batch = deduplicate_cdc_batch(
        micro_batch_df.filter(F.col("_rescued_data").isNull())
    )

    # Self-bootstrap table if not present
    if not DeltaTable.isDeltaTable(spark_session, silver_table_path):
        (
            cleaned_batch.filter(F.col("op_type") != "D")
            .withColumn("_updated_at", F.current_timestamp())
            .write.format("delta")
            .save(silver_table_path)
        )
        return

    silver_target = DeltaTable.forPath(spark_session, silver_table_path)

    (
        silver_target.alias("target")
        .merge(
            source=cleaned_batch.alias("source"),
            condition="target.transaction_id = source.transaction_id",
        )
        # 1. DELETE handling
        .whenMatchedDelete(condition="source.op_type = 'D'")
        # 2. UPDATE handling (apply only when source event is strictly newer)
        .whenMatchedUpdate(
            condition="source.op_type IN ('U', 'I') AND source.event_timestamp >= target.event_timestamp",
            set={
                "customer_id": "source.customer_id",
                "amount": "source.amount",
                "currency": "source.currency",
                "op_type": "source.op_type",
                "event_timestamp": "source.event_timestamp",
                "_updated_at": F.current_timestamp(),
            },
        )
        # 3. INSERT handling
        .whenNotMatchedInsert(
            condition="source.op_type != 'D'",
            values={
                "transaction_id": "source.transaction_id",
                "customer_id": "source.customer_id",
                "amount": "source.amount",
                "currency": "source.currency",
                "op_type": "source.op_type",
                "event_timestamp": "source.event_timestamp",
                "_updated_at": F.current_timestamp(),
            },
        )
        .execute()
    )
