"""
Solves data skew during distributed joins using Key Salting and broadcast hints.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def salted_join(
    large_skewed_df: DataFrame,
    dimension_df: DataFrame,
    join_key: str,
    salt_buckets: int = 4,
) -> DataFrame:
    """
    Mitigates severe shuffle data skew by distributing skewed keys across N buckets.

    Mechanism:
    1. Large table: Adds a pseudo-random integer column [0, salt_buckets - 1].
    2. Dimension table: Explodes every row by an array of [0, salt_buckets - 1].
    3. Composite join on (join_key, salt_key).
    """
    # 1. Salt the large skewed dataset
    salted_large = large_skewed_df.withColumn(
        "_salt_key", F.floor(F.rand() * salt_buckets)
    )

    # 2. Replicate dimension rows across all possible salt buckets
    salt_array = F.array([F.lit(i) for i in range(salt_buckets)])
    replicated_dimension = dimension_df.withColumn("_salt_array", salt_array).select(
        "*", F.explode("_salt_array").alias("_salt_key")
    ).drop("_salt_array")

    # 3. Perform the balanced distributed join
    joined_df = salted_large.join(
        replicated_dimension,
        on=[join_key, "_salt_key"],
        how="inner",
    ).drop("_salt_key")

    return joined_df

def optimized_broadcast_join(
    fact_df: DataFrame,
    dim_df: DataFrame,
    join_key: str,
) -> DataFrame:
    """
    Enforces a map-side broadcast hash join, bypassing Spark's SortMerge shuffle entirely.
    Suitable when dimension table fits easily in executor memory (< 100MB by default).
    """
    return fact_df.join(F.broadcast(dim_df), on=join_key, how="inner")
