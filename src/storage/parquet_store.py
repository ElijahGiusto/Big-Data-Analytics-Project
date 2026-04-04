"""
Distributed Parquet Storage Module

Writes normalized DataFrames to Parquet format, partitioned by platform
and snapshot_date. This layout mirrors a wide-column data model where
data is logically organized by entity (artist) with time-ordered values,
enabling efficient range scans over historical intervals.

Parquet's columnar encoding provides compression efficiency and supports
high-performance analytical queries via Spark SQL.
"""

import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


def load_config():
    """Load pipeline configuration from settings.yaml."""
    project_root = Path(__file__).resolve().parents[2]
    config_path = project_root / "config" / "settings.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


def save_to_parquet(df, config):
    """
    Write the unified DataFrame to Parquet, partitioned by platform
    and snapshot_date.

    New snapshots are appended alongside existing data so the processed
    directory accumulates a growing time-series dataset across runs.

    Partitioning layout:
        data/processed/
            platform=spotify/
                snapshot_date=2026-04-01/
                    part-*.parquet
                snapshot_date=2026-04-02/
                    ...
            platform=lastfm/
                ...
            platform=deezer/
                ...

    Args:
        df: Normalized Spark DataFrame to persist.
        config: Pipeline configuration dict.
    """
    project_root = Path(__file__).resolve().parents[2]
    output_path = str(project_root / config["storage"]["processed_path"])

    record_count = df.count()
    logger.info("Writing %d records to Parquet at %s", record_count, output_path)

    # Append mode preserves existing snapshots from prior pipeline runs.
    # Partitioning by platform and date enables efficient time-range queries
    # and platform-specific filtering without scanning the full dataset.
    df.write.mode("append") \
        .partitionBy("platform", "snapshot_date") \
        .parquet(output_path)

    logger.info("Parquet write complete -> %s", output_path)


def load_all_snapshots(spark, config):
    """
    Load all historical snapshots from the processed Parquet store.

    Reads across all partitions (platforms and dates) to reconstruct
    the full time-series dataset for analytical queries.

    Args:
        spark: Active SparkSession.
        config: Pipeline configuration dict.

    Returns:
        DataFrame: All historical records across platforms and dates.
    """
    project_root = Path(__file__).resolve().parents[2]
    input_path = str(project_root / config["storage"]["processed_path"])

    logger.info("Loading all snapshots from %s", input_path)

    try:
        df = spark.read.parquet(input_path)
        record_count = df.count()
        logger.info("Loaded %d historical records", record_count)
        return df
    except Exception as e:
        logger.error("Failed to load Parquet data: %s", e)
        return None
