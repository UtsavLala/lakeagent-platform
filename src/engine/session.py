
from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "LakeAgent-Engine") -> SparkSession:
    """Creates a local SparkSession equipped with Delta Lake 3.x support and AQE."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Enable AQE and dynamic skew mitigation
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "67108864")  # 64MB
        # Optimize default shuffle partitions for development/testing
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )
