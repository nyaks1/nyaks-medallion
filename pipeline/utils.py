"""
Shared utilities: SparkSession factory and config loader.
"""

import os
import yaml
from pyspark.sql import SparkSession


def load_config(config_path: str = None) -> dict:
    if config_path is None:
        config_path = os.environ.get(
            "PIPELINE_CONFIG", "/data/config/pipeline_config.yaml"
        )
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_spark(config: dict) -> SparkSession:
    """
    Create or retrieve the singleton SparkSession with Delta Lake support.

    WHY pre-downloaded JARs at /opt/delta-jars/?
        The scoring system runs with --network=none. Runtime JAR downloads
        (configure_spark_with_delta_pip, --packages) fail silently or hang.
        We download the exact JARs during docker build — they're baked into
        the image and available without any network access at runtime.

    WHY both spark.jars AND the extension configs?
        spark.jars  → puts Delta JARs on the JVM classpath
        extensions  → tells Spark to load the Delta SQL extension
        catalog     → tells Spark to use Delta as the catalog
        All three are required. Missing any one = ClassNotFoundException.
    """
    spark_cfg = config.get("spark", {})

    # JARs pre-downloaded during docker build — no network needed at runtime
    jar_dir = "/opt/delta-jars"
    if os.path.isdir(jar_dir):
        jars = [os.path.join(jar_dir, f)
                for f in os.listdir(jar_dir) if f.endswith(".jar")]
        jars_str = ",".join(jars)
    else:
        # Fallback for local dev outside Docker
        jars_str = ""

    builder = (
        SparkSession.builder
        .master(spark_cfg.get("master", "local[2]"))
        .appName(spark_cfg.get("app_name", "nyaks-medallion"))
        .config("spark.driver.memory",
                spark_cfg.get("driver_memory", "512m"))
        .config("spark.executor.memory",
                spark_cfg.get("executor_memory", "1g"))
        .config("spark.sql.shuffle.partitions",
                str(spark_cfg.get("shuffle_partitions", 4)))
        .config("spark.default.parallelism",
                str(spark_cfg.get("parallelism", 2)))
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.showConsoleProgress", "false")
    )

    if jars_str:
        builder = builder.config("spark.jars", jars_str)

    return builder.getOrCreate()