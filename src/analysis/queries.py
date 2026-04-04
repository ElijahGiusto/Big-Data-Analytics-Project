"""
Spark SQL Analytical Queries Module

Executes analytical queries against the normalized artist popularity
time-series dataset using Spark SQL. Implements the query layer
described in the M1 architecture.

Platform data context:
    - Spotify: Artist identification only (name + ID, no metrics)
    - Last.fm: Listener count (unique listeners) and total playcount
    - Deezer: Fan count (users who favorited the artist)
    - MusicBrainz: Community rating (0-100) and genre tags

Queries are designed around these differences rather than treating
all platforms' numbers as equivalent.
"""

import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def register_table(spark, df):
    """
    Register the DataFrame as a Spark SQL temporary view for querying.

    Args:
        spark: Active SparkSession.
        df: Normalized DataFrame to register.
    """
    df.createOrReplaceTempView("artist_popularity")
    logger.info("Registered 'artist_popularity' table with %d records", df.count())


def dataset_summary(spark):
    """
    Query 1: High-level summary statistics for the entire dataset.

    Provides an overview of data volume, platform distribution,
    and time coverage to validate pipeline completeness.

    Returns:
        DataFrame: Summary statistics.
    """
    logger.info("Query: Dataset summary statistics")

    result = spark.sql("""
        SELECT
            COUNT(*) AS total_records,
            COUNT(DISTINCT artist_name) AS unique_artists,
            COUNT(DISTINCT platform) AS platforms,
            COUNT(DISTINCT snapshot_date) AS snapshots,
            MIN(snapshot_date) AS earliest_snapshot,
            MAX(snapshot_date) AS latest_snapshot
        FROM artist_popularity
    """)

    result.show(truncate=False)
    return result


def cross_platform_comparison(spark):
    """
    Query 2: Compare artist reach across platforms with real metrics.

    Shows Last.fm listeners, Deezer fans, and MusicBrainz community
    rating side by side. Spotify is excluded since it only provides
    artist identification under Client Credentials auth. Each column
    represents a different popularity signal:
        - Last.fm listeners: unique users who played the artist
        - Deezer fans: users who favorited the artist
        - MusicBrainz rating: community score out of 100

    Returns:
        DataFrame: Artists with per-platform metrics.
    """
    logger.info("Query: Cross-platform popularity comparison")

    result = spark.sql("""
        SELECT
            artist_name,
            MAX(CASE WHEN platform = 'lastfm' THEN listeners END) AS lastfm_listeners,
            MAX(CASE WHEN platform = 'lastfm' THEN playcount END) AS lastfm_playcount,
            MAX(CASE WHEN platform = 'deezer' THEN listeners END) AS deezer_fans,
            MAX(CASE WHEN platform = 'musicbrainz' THEN popularity_score END) AS mb_rating
        FROM artist_popularity
        WHERE platform != 'spotify'
        GROUP BY artist_name
        ORDER BY lastfm_listeners DESC NULLS LAST
    """)

    result.show(50, truncate=False)
    return result


def top_artists_by_reach(spark, n=10):
    """
    Query 3: Top N artists ranked by listener/fan count per platform.

    Ranks artists within Last.fm and Deezer separately using window
    functions. Spotify is excluded (no metrics) and MusicBrainz is
    excluded (its rating scale is not comparable to listener counts).

    Args:
        spark: Active SparkSession.
        n: Number of top artists to return per platform.

    Returns:
        DataFrame: Top N artists per platform with rank.
    """
    logger.info("Query: Top %d artists by reach", n)

    result = spark.sql(f"""
        WITH ranked AS (
            SELECT
                artist_name,
                platform,
                listeners,
                playcount,
                ROW_NUMBER() OVER (
                    PARTITION BY platform
                    ORDER BY listeners DESC
                ) AS rank
            FROM artist_popularity
            WHERE platform IN ('lastfm', 'deezer')
              AND listeners IS NOT NULL
        )
        SELECT
            platform,
            rank,
            artist_name,
            listeners,
            playcount
        FROM ranked
        WHERE rank <= {n}
        ORDER BY platform, rank
    """)

    result.show(n * 2, truncate=False)
    return result


def musicbrainz_ratings(spark):
    """
    Query 4: MusicBrainz community ratings and genre tags.

    MusicBrainz ratings are on a different scale (community votes,
    0-100) than listener counts, so they get their own query. This
    shows which artists are most highly rated by the MusicBrainz
    community, along with their genre classifications.

    Returns:
        DataFrame: Artists ranked by MusicBrainz community rating.
    """
    logger.info("Query: MusicBrainz community ratings")

    result = spark.sql("""
        SELECT
            artist_name,
            popularity_score AS community_rating,
            listeners AS total_votes,
            genres AS genre_tags
        FROM artist_popularity
        WHERE platform = 'musicbrainz'
          AND popularity_score IS NOT NULL
        ORDER BY community_rating DESC, total_votes DESC
    """)

    result.show(50, truncate=False)
    return result


def genre_analysis(spark):
    """
    Query 5: Genre distribution across platforms.

    Shows genre tags from Last.fm and MusicBrainz (the two platforms
    that provide genre data) alongside listener metrics, enabling
    analysis of which genres dominate in popularity.

    Returns:
        DataFrame: Artists with genre data and associated metrics.
    """
    logger.info("Query: Genre analysis across platforms")

    result = spark.sql("""
        SELECT
            artist_name,
            platform,
            genres,
            listeners,
            popularity_score
        FROM artist_popularity
        WHERE genres IS NOT NULL
          AND platform IN ('lastfm', 'musicbrainz')
        ORDER BY listeners DESC NULLS LAST
    """)

    result.show(50, truncate=False)
    return result


def platform_coverage(spark):
    """
    Query 6: Analyze which artists have data across all platforms
    versus partial coverage.

    Useful for identifying gaps in the ingestion pipeline and
    understanding data completeness. Also highlights artist name
    inconsistencies across platforms (e.g., "Bjork" vs "Björk").

    Returns:
        DataFrame: Per-artist platform coverage summary.
    """
    logger.info("Query: Platform coverage analysis")

    result = spark.sql("""
        SELECT
            artist_name,
            COLLECT_SET(platform) AS platforms_present,
            COUNT(DISTINCT platform) AS num_platforms,
            COUNT(DISTINCT snapshot_date) AS num_snapshots
        FROM artist_popularity
        GROUP BY artist_name
        ORDER BY num_platforms DESC, artist_name
    """)

    result.show(50, truncate=False)
    return result


def artist_popularity_trend(spark, artist_name):
    """
    Query 7: Retrieve an artist's data across all platforms.

    Shows how each platform represents the same artist with different
    metrics, demonstrating the heterogeneous data challenge from the
    M1 pitch. Supports time-range analysis when multiple snapshots exist.

    Args:
        spark: Active SparkSession.
        artist_name: Name of the artist to query.

    Returns:
        DataFrame: All records for the artist across platforms.
    """
    logger.info("Query: Artist detail for '%s'", artist_name)

    result = spark.sql(f"""
        SELECT
            artist_name,
            platform,
            snapshot_date,
            popularity_score,
            listeners,
            playcount,
            genres
        FROM artist_popularity
        WHERE LOWER(artist_name) = LOWER('{artist_name}')
        ORDER BY platform, snapshot_date
    """)

    result.show(truncate=False)
    return result


def run_all_queries(spark, df):
    """
    Execute all analytical queries and display results.

    This serves as the query layer entry point, running the full
    suite of analytics against the loaded dataset.

    Args:
        spark: Active SparkSession.
        df: Normalized DataFrame (all historical snapshots).
    """
    register_table(spark, df)

    print("\n" + "=" * 70)
    print("ANALYTICAL QUERY RESULTS")
    print("=" * 70)

    print("\n--- Query 1: Dataset Summary ---")
    dataset_summary(spark)

    print("\n--- Query 2: Cross-Platform Comparison ---")
    cross_platform_comparison(spark)

    print("\n--- Query 3: Top 10 Artists by Reach ---")
    top_artists_by_reach(spark, n=10)

    print("\n--- Query 4: MusicBrainz Community Ratings ---")
    musicbrainz_ratings(spark)

    print("\n--- Query 5: Genre Analysis ---")
    genre_analysis(spark)

    print("\n--- Query 6: Platform Coverage ---")
    platform_coverage(spark)

    print("\n--- Query 7: Artist Detail (Radiohead) ---")
    artist_popularity_trend(spark, "Radiohead")

    print("\n" + "=" * 70)
    print("ALL QUERIES COMPLETE")
    print("=" * 70)