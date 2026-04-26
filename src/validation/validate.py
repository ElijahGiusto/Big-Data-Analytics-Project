"""
Validation checks for the artist popularity pipeline.

The checks focus on evidence that the latest snapshot is complete,
queryable, and analytically meaningful across the configured artist list.
"""

import logging

from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {
    "artist_name", "artist_id", "platform", "snapshot_date",
    "popularity_score", "listeners", "playcount", "genres",
}

REQUIRED_PLATFORMS = {"lastfm", "deezer", "musicbrainz", "wikimedia"}
MIN_COVERAGE_BY_PLATFORM = {
    "lastfm": 0.95,
    "deezer": 0.90,
    "musicbrainz": 0.75,
    "wikimedia": 0.75,
    "itunes": 0.50,
    "wikidata": 0.50,
    "theaudiodb": 0.40,
}
REQUIRED_METRIC_PLATFORMS = {"lastfm", "deezer", "musicbrainz", "wikimedia"}
MIN_SOURCE_COUNT = 6


def _latest_snapshot(df):
    """Return only the most recent snapshot in the dataset."""
    latest = df.select(F.max("snapshot_date").alias("latest")).collect()[0]["latest"]
    return latest, df.where(F.col("snapshot_date") == latest)


def _log_check(name, passed, detail):
    """Log a validation check in a consistent format."""
    status = "PASS" if passed else "FAIL"
    logger.info("[%s] %s - %s", status, name, detail)
    return passed


def validate_artist_popularity(
    df,
    expected_artists,
    enabled_sources=None,
    min_platform_coverage=0.95,
):
    """
    Validate the latest processed artist-popularity snapshot.

    Args:
        df: Spark DataFrame loaded from processed Parquet.
        expected_artists: Configured artist names expected in the snapshot.
        enabled_sources: Optional set of source names included in validation.
        min_platform_coverage: Minimum per-platform artist coverage ratio.

    Returns:
        bool: True when all required checks pass.
    """
    checks = []
    expected_artist_count = len(expected_artists)

    if enabled_sources:
        df = df.where(F.col("platform").isin(*sorted(enabled_sources)))

    columns = set(df.columns)
    missing_columns = sorted(REQUIRED_COLUMNS - columns)
    checks.append(_log_check(
        "required columns",
        not missing_columns,
        "all required columns present"
        if not missing_columns else f"missing {missing_columns}",
    ))

    latest, latest_df = _latest_snapshot(df)
    total_rows = latest_df.count()
    logger.info("Validating latest snapshot: %s (%d rows)", latest, total_rows)

    null_keys = latest_df.where(
        F.col("artist_name").isNull()
        | F.col("artist_id").isNull()
        | F.col("platform").isNull()
        | F.col("snapshot_date").isNull()
    ).count()
    checks.append(_log_check(
        "non-null keys",
        null_keys == 0,
        f"{null_keys} key-field nulls found",
    ))

    unique_artists = latest_df.select("artist_name").distinct().count()
    checks.append(_log_check(
        "canonical artist count",
        unique_artists == expected_artist_count,
        f"{unique_artists}/{expected_artist_count} unique artists",
    ))

    unexpected_artists = (
        latest_df
        .select("artist_name")
        .distinct()
        .where(~F.col("artist_name").isin(expected_artists))
        .count()
    )
    checks.append(_log_check(
        "artist names match config",
        unexpected_artists == 0,
        f"{unexpected_artists} names outside configured artist list",
    ))

    duplicate_rows = (
        latest_df
        .groupBy("artist_name", "platform", "snapshot_date")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    checks.append(_log_check(
        "no duplicate artist-platform rows",
        duplicate_rows == 0,
        f"{duplicate_rows} duplicate groups found",
    ))

    platform_counts = {
        row["platform"]: row["count"]
        for row in latest_df.groupBy("platform").count().collect()
    }
    source_count = len(platform_counts)
    checks.append(_log_check(
        "multi-source coverage",
        source_count >= MIN_SOURCE_COUNT,
        f"{source_count} source platforms present",
    ))

    required_platforms = REQUIRED_PLATFORMS
    if enabled_sources:
        required_platforms = REQUIRED_PLATFORMS & set(enabled_sources)
    missing_platforms = sorted(required_platforms - set(platform_counts))
    checks.append(_log_check(
        "required metric platforms present",
        not missing_platforms,
        "all required metric platforms present"
        if not missing_platforms else f"missing {missing_platforms}",
    ))

    for platform in sorted(platform_counts):
        count = platform_counts.get(platform, 0)
        coverage = count / expected_artist_count if expected_artist_count else 0
        min_coverage = MIN_COVERAGE_BY_PLATFORM.get(platform)
        if min_coverage is None:
            _log_check(
                f"{platform} coverage",
                True,
                f"{count}/{expected_artist_count} artists ({coverage:.1%})",
            )
        else:
            checks.append(_log_check(
                f"{platform} coverage",
                coverage >= min_coverage,
                f"{count}/{expected_artist_count} artists ({coverage:.1%}); minimum {min_coverage:.0%}",
            ))

    negative_metrics = latest_df.where(
        (F.col("popularity_score") < 0)
        | (F.col("listeners") < 0)
        | (F.col("playcount") < 0)
    ).count()
    checks.append(_log_check(
        "non-negative metrics",
        negative_metrics == 0,
        f"{negative_metrics} negative metric rows found",
    ))

    metric_platforms = {
        row["platform"]
        for row in (
            latest_df
            .where(F.col("popularity_score").isNotNull())
            .select("platform")
            .distinct()
            .collect()
        )
    }
    required_metric_platforms = REQUIRED_METRIC_PLATFORMS
    if enabled_sources:
        required_metric_platforms = REQUIRED_METRIC_PLATFORMS & set(enabled_sources)
    checks.append(_log_check(
        "required metrics populated",
        required_metric_platforms.issubset(metric_platforms),
        f"metric platforms present: {sorted(metric_platforms)}",
    ))

    for platform in sorted(required_metric_platforms):
        metric_rows = (
            latest_df
            .where((F.col("platform") == platform) & F.col("popularity_score").isNotNull())
            .count()
        )
        min_metric_rows = int(
            expected_artist_count
            * MIN_COVERAGE_BY_PLATFORM.get(platform, min_platform_coverage)
        )
        checks.append(_log_check(
            f"{platform} metrics populated",
            metric_rows >= min_metric_rows,
            f"{metric_rows}/{expected_artist_count} rows with popularity_score",
        ))

    mb_range_errors = latest_df.where(
        (F.col("platform") == "musicbrainz")
        & ((F.col("popularity_score") < 0) | (F.col("popularity_score") > 100))
    ).count()
    checks.append(_log_check(
        "bounded 0-100 scores",
        mb_range_errors == 0,
        f"musicbrainz={mb_range_errors}",
    ))

    passed = all(checks)
    logger.info("Validation %s", "passed" if passed else "failed")
    return passed
