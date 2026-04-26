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
import shutil
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


def load_config():
    """Load pipeline configuration from settings.yaml."""
    project_root = Path(__file__).resolve().parents[2]
    config_path = project_root / "config" / "settings.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


def storage_mode(config):
    """Return the configured processed-data storage mode."""
    return config.get("storage", {}).get("mode", "local").lower()


def processed_data_path(config):
    """Return the Spark-readable processed Parquet path."""
    project_root = Path(__file__).resolve().parents[2]
    storage = config.get("storage", {})
    mode = storage_mode(config)

    if mode == "hdfs":
        hdfs_uri = storage.get("hdfs_uri", "hdfs://localhost:9000").rstrip("/")
        hdfs_path = storage.get(
            "hdfs_processed_path", "/artist-popularity/processed"
        )
        return hdfs_uri + "/" + hdfs_path.lstrip("/")

    return str(project_root / storage.get("processed_path", "data/processed"))


def _delete_local_snapshot_partitions(output_path, snapshot_dates):
    """Remove current snapshot partitions from local storage."""
    output_dir = Path(output_path)
    resolved_output = output_dir.resolve()
    if not output_dir.exists():
        return

    for snapshot_date in snapshot_dates:
        for platform_dir in output_dir.glob("platform=*"):
            partition_dir = platform_dir / f"snapshot_date={snapshot_date}"
            if not partition_dir.exists():
                continue
            resolved_partition = partition_dir.resolve()
            if resolved_output not in resolved_partition.parents:
                raise RuntimeError(
                    f"Refusing to remove partition outside {resolved_output}"
                )
            shutil.rmtree(partition_dir)
            logger.info("Removed existing snapshot partition %s", partition_dir)


def _delete_hdfs_snapshot_partitions(spark, output_path, snapshot_dates):
    """Remove current snapshot partitions from HDFS using Hadoop FileSystem."""
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    uri = jvm.java.net.URI.create(output_path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
    root = jvm.org.apache.hadoop.fs.Path(output_path)

    if not fs.exists(root):
        return

    for snapshot_date in snapshot_dates:
        for platform_status in fs.listStatus(root):
            platform_path = platform_status.getPath()
            if not platform_status.isDirectory():
                continue
            if not platform_path.getName().startswith("platform="):
                continue
            partition_path = jvm.org.apache.hadoop.fs.Path(
                platform_path, f"snapshot_date={snapshot_date}"
            )
            if fs.exists(partition_path):
                fs.delete(partition_path, True)
                logger.info("Removed existing HDFS snapshot partition %s", partition_path)


def save_to_parquet(df, config):
    """
    Write the unified DataFrame to Parquet, partitioned by platform
    and snapshot_date.

    New snapshots are appended alongside existing data so the processed
    directory accumulates a growing time-series dataset across runs.
    If a snapshot for the same date is rerun, only that date's platform
    partitions are replaced to avoid duplicate artist-platform rows.

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
    mode = storage_mode(config)
    output_path = processed_data_path(config)

    record_count = df.count()
    logger.info(
        "Writing %d records to %s Parquet at %s",
        record_count, mode.upper(), output_path,
    )

    snapshot_dates = [
        row["snapshot_date"]
        for row in df.select("snapshot_date").distinct().collect()
        if row["snapshot_date"]
    ]
    if mode == "hdfs":
        _delete_hdfs_snapshot_partitions(df.sparkSession, output_path, snapshot_dates)
    else:
        _delete_local_snapshot_partitions(output_path, snapshot_dates)

    # Dynamic partition overwrite keeps historical snapshots but replaces
    # same-day reruns. Existing partitions for the snapshot date are removed
    # first so disabled sources cannot remain from an earlier run.
    df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df.write.mode("overwrite") \
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
    input_path = processed_data_path(config)

    logger.info("Loading all snapshots from %s", input_path)

    try:
        df = spark.read.parquet(input_path)
        record_count = df.count()
        logger.info("Loaded %d historical records", record_count)
        return df
    except Exception as e:
        logger.error("Failed to load Parquet data: %s", e)
        return None
