"""
Distributed Artist Popularity Analysis Pipeline

Main entry point for the end-to-end data pipeline. Orchestrates:
    1. SparkSession initialization
    2. Data ingestion from music platform and public metadata APIs
    3. Schema normalization into unified Spark DataFrame
    4. Partitioned Parquet storage
    5. Spark SQL analytical queries

Usage:
    python src/main.py              # Full pipeline (ingest + process + query)
    python src/main.py --query-only # Skip ingestion, query existing data
    python src/main.py --validate-only # Validate existing data only
"""

import sys
import os
import logging
import time
import platform
import argparse
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml


# ---------------------------------------------------------------------------
# Windows Hadoop Compatibility
# ---------------------------------------------------------------------------

def setup_hadoop_windows():
    """
    Configure HADOOP_HOME on Windows for PySpark local mode.

    PySpark on Windows requires Hadoop native binaries even for local file
    operations. This function checks for an existing HADOOP_HOME or looks for
    hadoop/bin/winutils.exe and hadoop/bin/hadoop.dll in the project directory.
    """
    if platform.system() != "Windows":
        return

    project_root = Path(__file__).resolve().parents[1]
    configured_home = os.environ.get("HADOOP_HOME")
    hadoop_home = Path(configured_home) if configured_home else project_root / "hadoop"
    hadoop_bin = hadoop_home / "bin"
    required_files = [
        hadoop_bin / "winutils.exe",
        hadoop_bin / "hadoop.dll",
    ]
    missing_files = [path for path in required_files if not path.exists()]

    if not missing_files:
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        bin_path = str(hadoop_bin)
        path_entries = os.environ.get("PATH", "").split(os.pathsep)
        if bin_path.lower() not in {entry.lower() for entry in path_entries}:
            os.environ["PATH"] = bin_path + os.pathsep + os.environ.get("PATH", "")
        return

    # Create directory structure so the setup script or user can drop files in.
    hadoop_bin.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 60)
    print("WINDOWS SETUP REQUIRED: Hadoop native binaries")
    print("=" * 60)
    print("\nPySpark on Windows needs these files to write Parquet locally:")
    for path in missing_files:
        print(f"   - {path}")
    print("\nRecommended one-time setup from the project root:")
    print(r"   powershell -ExecutionPolicy Bypass -File .\scripts\setup_windows_hadoop.ps1")
    print("\nManual download links for Hadoop 3.3.6 binaries:")
    print(f"   https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe")
    print(f"   https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll")
    print(f"\nPlace both files in:")
    print(f"   {hadoop_bin}")
    print("\nRun the pipeline again. The app will set HADOOP_HOME and PATH.")
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


def enabled_source_names(config):
    """Return source names enabled for the default pipeline run."""
    enabled = config.get("sources", {}).get("enabled", {})
    return {name for name, is_enabled in enabled.items() if is_enabled}


def apply_cli_overrides(config, args):
    """Apply command-line overrides to the loaded configuration."""
    if args.storage:
        config.setdefault("storage", {})["mode"] = args.storage
    if args.hdfs_uri:
        config.setdefault("storage", {})["hdfs_uri"] = args.hdfs_uri
    return config


def parse_args(argv=None):
    """Parse command-line options for repeatable pipeline modes."""
    parser = argparse.ArgumentParser(
        description="Run the artist popularity data pipeline."
    )
    parser.add_argument(
        "--query-only",
        action="store_true",
        help="Skip ingestion and processing; query existing Parquet data.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Skip ingestion and queries; validate existing Parquet data.",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Run ingestion/query stages without the final validation gate.",
    )
    parser.add_argument(
        "--storage",
        choices=["local", "hdfs"],
        help="Override processed-data storage mode from config/settings.yaml.",
    )
    parser.add_argument(
        "--hdfs-uri",
        help="Override the HDFS NameNode URI, for example hdfs://localhost:9000.",
    )
    return parser.parse_args(argv)


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
    from ingestion.listenbrainz_ingest import fetch_all_artists as fetch_lb, save_raw as save_lb
    from ingestion.wikimedia_ingest import fetch_all_artists as fetch_wikimedia, save_raw as save_wikimedia
    from ingestion.theaudiodb_ingest import fetch_all_artists as fetch_audio, save_raw as save_audio
    from ingestion.itunes_ingest import fetch_all_artists as fetch_itunes, save_raw as save_itunes
    from ingestion.wikidata_ingest import fetch_all_artists as fetch_wikidata, save_raw as save_wikidata

    source_jobs = {
        "spotify": (fetch_spotify, save_spotify),
        "lastfm": (fetch_lastfm, save_lastfm),
        "deezer": (fetch_deezer, save_deezer),
        "musicbrainz": (fetch_mb, save_mb),
        "wikimedia": (fetch_wikimedia, save_wikimedia),
        "theaudiodb": (fetch_audio, save_audio),
        "itunes": (fetch_itunes, save_itunes),
        "wikidata": (fetch_wikidata, save_wikidata),
    }

    enabled = enabled_source_names(config)
    platform_data = {name: [] for name in set(source_jobs) | {"listenbrainz"}}

    def run_source(source_name):
        fetch_func, save_func = source_jobs[source_name]
        records = fetch_func(config) or []
        save_func(records, config)
        return records

    independent_sources = [
        name for name in source_jobs
        if name in enabled and name != "listenbrainz"
    ]
    skipped_sources = sorted(set(source_jobs) - set(independent_sources))
    if skipped_sources:
        logger.info("Skipping disabled sources: %s", ", ".join(skipped_sources))

    source_workers = min(4, len(independent_sources)) or 1
    with ThreadPoolExecutor(max_workers=source_workers) as executor:
        future_to_source = {
            executor.submit(run_source, source_name): source_name
            for source_name in independent_sources
        }
        for future in as_completed(future_to_source):
            source_name = future_to_source[future]
            try:
                platform_data[source_name] = future.result()
            except Exception as e:
                logger.error("%s ingestion failed: %s", source_name, e)

    # ListenBrainz depends on MBIDs from MusicBrainz and Last.fm, so it runs
    # after those source jobs have completed.
    if "listenbrainz" in enabled:
        try:
            platform_data["listenbrainz"] = fetch_lb(config, platform_data) or []
            save_lb(platform_data["listenbrainz"], config)
        except Exception as e:
            logger.error("ListenBrainz ingestion failed: %s", e)
    else:
        logger.info("Skipping disabled source: listenbrainz")

    counts = {k: len(v) for k, v in platform_data.items()}
    total = sum(counts.values())
    logger.info(
        "Ingestion complete: %s (%d total)",
        ", ".join(f"{count} {platform}" for platform, count in counts.items()),
        total,
    )

    return platform_data


def stage_process(spark, platform_data, snapshot_date, config):
    """
    Stage 2: Normalization and Processing (Shuffle + Reduce Phase)

    Transforms heterogeneous API responses into a unified Spark DataFrame
    with a consistent schema. Platform-specific map operations extract and
    normalize fields, then a union (reduce) combines all records.

    Returns:
        DataFrame: Unified, normalized dataset for the current snapshot.
    """
    from processing.normalize import normalize_all_with_artists

    df = normalize_all_with_artists(
        spark, platform_data, snapshot_date, config["artists"]
    )
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
    from processing.normalize import canonicalize_artist_names_df

    all_data = load_all_snapshots(spark, config)

    if all_data is None or all_data.count() == 0:
        logger.error("No data available for queries")
        return

    all_data = canonicalize_artist_names_df(all_data, config["artists"])
    run_all_queries(spark, all_data, enabled_sources=enabled_source_names(config))


def stage_validate(spark, config):
    """Run the validation gate against the latest available snapshot."""
    from storage.parquet_store import load_all_snapshots
    from processing.normalize import canonicalize_artist_names_df
    from validation.validate import validate_artist_popularity

    all_data = load_all_snapshots(spark, config)

    if all_data is None or all_data.count() == 0:
        logger.error("No data available for validation")
        return False

    all_data = canonicalize_artist_names_df(all_data, config["artists"])
    return validate_artist_popularity(
        all_data,
        expected_artists=config["artists"],
        enabled_sources=enabled_source_names(config),
        min_platform_coverage=0.95,
    )


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
    args = parse_args()
    config = apply_cli_overrides(load_config(), args)
    snapshot_date = datetime.now().strftime("%Y-%m-%d")
    query_only = args.query_only or args.validate_only
    storage_config = config.get("storage", {})
    storage_mode = storage_config.get("mode", "local").lower()

    logger.info("Snapshot date: %s", snapshot_date)
    logger.info("Artists configured: %d", len(config["artists"]))
    logger.info("Enabled sources: %s", ", ".join(sorted(enabled_source_names(config))))
    logger.info("Storage mode: %s", storage_mode)
    if storage_mode == "hdfs":
        logger.info("HDFS URI: %s", storage_config.get("hdfs_uri"))
    logger.info(
        "Mode: %s",
        "validate-only" if args.validate_only
        else "query-only" if args.query_only
        else "full pipeline",
    )

    # Initialize SparkSession
    setup_hadoop_windows()
    logger.info("Initializing SparkSession...")
    from pyspark.sql import SparkSession

    # Build Spark config — hadoop.home.dir is set from env var for Windows
    hadoop_home = os.environ.get("HADOOP_HOME", "")

    builder = SparkSession.builder \
        .appName("ArtistPopularityPipeline") \
        .master("local[*]") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
        .config("spark.hadoop.hadoop.home.dir", hadoop_home)

    if storage_mode == "hdfs":
        builder = builder \
            .config("spark.hadoop.fs.defaultFS", storage_config.get("hdfs_uri")) \
            .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
            .config("spark.hadoop.dfs.replication", "1")

    spark = builder.getOrCreate()

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
            df = stage_process(spark, platform_data, snapshot_date, config)

            logger.info("Normalized schema:")
            df.printSchema()
            logger.info("Sample records:")
            df.show(10, truncate=False)

            # Stage 3: Store
            logger.info("-" * 40)
            logger.info("STAGE 3: PARQUET STORAGE")
            logger.info("-" * 40)
            stage_store(df, config)

        if not args.validate_only:
            # Stage 4: Query
            logger.info("-" * 40)
            logger.info("STAGE 4: ANALYTICAL QUERIES")
            logger.info("-" * 40)
            stage_query(spark, config)

        if not args.skip_validation:
            # Stage 5: Validate
            logger.info("-" * 40)
            logger.info("STAGE 5: VALIDATION")
            logger.info("-" * 40)
            validation_passed = stage_validate(spark, config)
            if not validation_passed:
                raise RuntimeError("Validation failed")

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
