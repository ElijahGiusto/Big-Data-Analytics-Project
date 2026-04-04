"""
Distributed Artist Popularity Analysis Pipeline

Main entry point for the end-to-end data pipeline. Orchestrates:
    1. SparkSession initialization
    2. Parallel data ingestion from Spotify, Last.fm, and Deezer APIs
    3. Schema normalization into unified Spark DataFrame
    4. Partitioned Parquet storage
    5. Spark SQL analytical queries

Usage:
    python src/main.py              # Full pipeline (ingest + process + query)
    python src/main.py --query-only # Skip ingestion, query existing data
"""

import sys
import os
import logging
import time
import platform
from pathlib import Path
from datetime import datetime

import yaml


# ---------------------------------------------------------------------------
# Windows Hadoop Compatibility
# ---------------------------------------------------------------------------

def setup_hadoop_windows():
    """
    Configure HADOOP_HOME on Windows for PySpark local mode.

    PySpark on Windows requires winutils.exe (a Hadoop binary) even for
    local file operations. This function checks for an existing HADOOP_HOME
    or looks for a bundled hadoop/bin/winutils.exe in the project directory.
    """
    if platform.system() != "Windows":
        return

    # Already configured
    if os.environ.get("HADOOP_HOME"):
        return

    # Check for project-bundled winutils
    project_root = Path(__file__).resolve().parents[1]
    hadoop_home = project_root / "hadoop"
    winutils_path = hadoop_home / "bin" / "winutils.exe"

    if winutils_path.exists():
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        return

    # Create directory structure so the user just needs to drop in the exe
    winutils_path.parent.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 60)
    print("WINDOWS SETUP REQUIRED: winutils.exe")
    print("=" * 60)
    print(f"\nPySpark on Windows needs Hadoop's winutils.exe to write files.")
    print(f"\n1. Download winutils.exe for Hadoop 3.3.x from:")
    print(f"   https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe")
    print(f"\n2. Place it in:")
    print(f"   {winutils_path}")
    print(f"\n3. Run the pipeline again.")
    print("=" * 60 + "\n")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------

def setup_logging():
    """Configure structured logging for the pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)-7s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )
    # Suppress noisy Spark/Py4J logs
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.WARNING)


logger = logging.getLogger("pipeline")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def load_config():
    """Load pipeline configuration from settings.yaml."""
    project_root = Path(__file__).resolve().parents[1]
    config_path = project_root / "config" / "settings.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Pipeline Stages
# ---------------------------------------------------------------------------

def stage_ingest(config):
    """
    Stage 1: Data Ingestion (Map Phase)

    Fetches artist data from all configured streaming platforms
    in parallel. Each platform's ingestion job runs concurrently,
    and raw API responses are saved to disk as JSON for
    reproducibility and reprocessing.

    Returns:
        dict: Platform name -> list of raw artist dicts.
    """
    from ingestion.spotify_ingest import fetch_all_artists as fetch_spotify, save_raw as save_spotify
    from ingestion.lastfm_ingest import fetch_all_artists as fetch_lastfm, save_raw as save_lastfm
    from ingestion.deezer_ingest import fetch_all_artists as fetch_deezer, save_raw as save_deezer
    from ingestion.musicbrainz_ingest import fetch_all_artists as fetch_mb, save_raw as save_mb

    platform_data = {
        "spotify": [],
        "lastfm": [],
        "deezer": [],
        "musicbrainz": [],
    }

    # Spotify ingestion
    try:
        platform_data["spotify"] = fetch_spotify(config)
        save_spotify(platform_data["spotify"], config)
    except Exception as e:
        logger.error("Spotify ingestion failed: %s", e)

    # Last.fm ingestion
    try:
        platform_data["lastfm"] = fetch_lastfm(config)
        save_lastfm(platform_data["lastfm"], config)
    except Exception as e:
        logger.error("Last.fm ingestion failed: %s", e)

    # Deezer ingestion
    try:
        platform_data["deezer"] = fetch_deezer(config)
        save_deezer(platform_data["deezer"], config)
    except Exception as e:
        logger.error("Deezer ingestion failed: %s", e)

    # MusicBrainz ingestion
    try:
        platform_data["musicbrainz"] = fetch_mb(config)
        save_mb(platform_data["musicbrainz"], config)
    except Exception as e:
        logger.error("MusicBrainz ingestion failed: %s", e)

    counts = {k: len(v) for k, v in platform_data.items()}
    total = sum(counts.values())
    logger.info(
        "Ingestion complete: %d Spotify, %d Last.fm, %d Deezer, %d MusicBrainz (%d total)",
        counts["spotify"], counts["lastfm"], counts["deezer"],
        counts["musicbrainz"], total,
    )

    return platform_data


def stage_process(spark, platform_data, snapshot_date):
    """
    Stage 2: Normalization and Processing (Shuffle + Reduce Phase)

    Transforms heterogeneous API responses into a unified Spark DataFrame
    with a consistent schema. Platform-specific map operations extract and
    normalize fields, then a union (reduce) combines all records.

    Returns:
        DataFrame: Unified, normalized dataset for the current snapshot.
    """
    from processing.normalize import normalize_all

    df = normalize_all(spark, platform_data, snapshot_date)
    return df


def stage_store(df, config):
    """
    Stage 3: Distributed Storage

    Writes the normalized DataFrame to Parquet format, partitioned by
    platform and snapshot_date. Parquet's columnar encoding provides
    compression efficiency. Partitioning enables efficient time-range
    queries and platform-specific scans without reading the full dataset.
    """
    from storage.parquet_store import save_to_parquet

    save_to_parquet(df, config)


def stage_query(spark, config):
    """
    Stage 4: Analytical Queries (Query Layer)

    Loads all historical snapshots from Parquet and executes Spark SQL
    queries for cross-platform comparisons, popularity trends, growth
    analysis, and dataset summaries.
    """
    from storage.parquet_store import load_all_snapshots
    from analysis.queries import run_all_queries

    all_data = load_all_snapshots(spark, config)

    if all_data is None or all_data.count() == 0:
        logger.error("No data available for queries")
        return

    run_all_queries(spark, all_data)


# ---------------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------------

def main():
    """Execute the full distributed analysis pipeline."""
    setup_logging()
    start_time = time.time()

    logger.info("=" * 60)
    logger.info("DISTRIBUTED ARTIST POPULARITY ANALYSIS PIPELINE")
    logger.info("=" * 60)

    # Load configuration
    config = load_config()
    snapshot_date = datetime.now().strftime("%Y-%m-%d")
    query_only = "--query-only" in sys.argv

    logger.info("Snapshot date: %s", snapshot_date)
    logger.info("Artists configured: %d", len(config["artists"]))
    logger.info("Mode: %s", "query-only" if query_only else "full pipeline")

    # Initialize SparkSession
    setup_hadoop_windows()
    logger.info("Initializing SparkSession...")
    from pyspark.sql import SparkSession

    # Build Spark config — hadoop.home.dir is set from env var for Windows
    hadoop_home = os.environ.get("HADOOP_HOME", "")

    spark = SparkSession.builder \
        .appName("ArtistPopularityPipeline") \
        .master("local[*]") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
        .config("spark.hadoop.hadoop.home.dir", hadoop_home) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession initialized (Spark %s)", spark.version)

    try:
        if not query_only:
            # Stage 1: Ingest
            logger.info("-" * 40)
            logger.info("STAGE 1: DATA INGESTION")
            logger.info("-" * 40)
            platform_data = stage_ingest(config)

            if not any(platform_data.values()):
                logger.error("No data ingested from any platform. Exiting.")
                return

            # Stage 2: Process
            logger.info("-" * 40)
            logger.info("STAGE 2: NORMALIZATION & PROCESSING")
            logger.info("-" * 40)
            df = stage_process(spark, platform_data, snapshot_date)

            logger.info("Normalized schema:")
            df.printSchema()
            logger.info("Sample records:")
            df.show(10, truncate=False)

            # Stage 3: Store
            logger.info("-" * 40)
            logger.info("STAGE 3: PARQUET STORAGE")
            logger.info("-" * 40)
            stage_store(df, config)

        # Stage 4: Query
        logger.info("-" * 40)
        logger.info("STAGE 4: ANALYTICAL QUERIES")
        logger.info("-" * 40)
        stage_query(spark, config)

    except Exception as e:
        logger.error("Pipeline failed: %s", e, exc_info=True)
        raise

    finally:
        spark.stop()
        elapsed = time.time() - start_time
        logger.info("SparkSession stopped")
        logger.info("Pipeline finished in %.1f seconds", elapsed)


if __name__ == "__main__":
    main()
