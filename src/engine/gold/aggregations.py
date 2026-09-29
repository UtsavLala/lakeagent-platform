"""
Gold Layer: Aggregates analytical KPIs and persists with Liquid Clustering.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def build_gold_customer_metrics(silver_df: DataFrame) -> DataFrame:
    """Computes high-value business metrics for consumption by APIs and Genie Spaces."""
    return (
        silver_df.groupBy("customer_id")
        .agg(
            F.count("transaction_id").alias("total_transactions"),
            F.round(F.sum("amount"), 2).alias("total_spend"),
            F.round(F.avg("amount"), 2).alias("average_order_value"),
            F.max("event_timestamp").alias("last_active_timestamp"),
        )
        .withColumn(
            "customer_tier",
            F.when(F.col("total_spend") >= 10000.0, "Enterprise")
            .when(F.col("total_spend") >= 2500.0, "Growth")
            .otherwise("Standard"),
        )
        .withColumn("_computed_at", F.current_timestamp())
    )

def write_gold_liquid_clustered(gold_df: DataFrame, gold_table_path: str) -> None:
    """
    Writes data to Gold Delta table with Liquid Clustering enabled on customer_id.
    Liquid Clustering prevents query performance degradation from static partition skew.
    """
    (
        gold_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        # Liquid Clustering syntax in Delta 3.x+
        .clusterBy("customer_id")
        .save(gold_table_path)
    )
