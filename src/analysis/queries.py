"""
Spark SQL analytical queries for the artist popularity dataset.

The default query set uses only sources enabled in config/settings.yaml.
Spotify is optional catalog data and is disabled by default, so the
portfolio output focuses on sources with usable metrics or enrichment.
"""

import logging

from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def _metric_source_sql():
    """Reusable SQL fragment for real popularity/reach signals."""
    return """
        SELECT artist_name, 'lastfm_listeners' AS signal,
               CAST(listeners AS DOUBLE) AS metric_value
        FROM latest
        WHERE platform = 'lastfm' AND listeners IS NOT NULL
        UNION ALL
        SELECT artist_name, 'deezer_fans' AS signal,
               CAST(listeners AS DOUBLE) AS metric_value
        FROM latest
        WHERE platform = 'deezer' AND listeners IS NOT NULL
        UNION ALL
        SELECT artist_name, 'wikimedia_views' AS signal,
               CAST(listeners AS DOUBLE) AS metric_value
        FROM latest
        WHERE platform = 'wikimedia' AND listeners IS NOT NULL
        UNION ALL
        SELECT artist_name, 'listenbrainz_listens' AS signal,
               CAST(playcount AS DOUBLE) AS metric_value
        FROM latest
        WHERE platform = 'listenbrainz' AND playcount IS NOT NULL
        UNION ALL
        SELECT artist_name, 'musicbrainz_rating' AS signal,
               CAST(popularity_score AS DOUBLE) AS metric_value
        FROM latest
        WHERE platform = 'musicbrainz' AND popularity_score IS NOT NULL
    """


def register_table(spark, df, enabled_sources=None):
    """
    Register enabled source rows as a Spark SQL temporary view.

    Args:
        spark: Active SparkSession.
        df: Normalized DataFrame to register.
        enabled_sources: Optional iterable of source names to keep.

    Returns:
        DataFrame: Filtered DataFrame registered as artist_popularity.
    """
    filtered = df
    if enabled_sources:
        filtered = filtered.where(F.col("platform").isin(*sorted(enabled_sources)))

    filtered.createOrReplaceTempView("artist_popularity")
    logger.info(
        "Registered 'artist_popularity' table with %d enabled-source records",
        filtered.count(),
    )
    return filtered


def artist_scorecard(spark, n=50):
    """
    Query 1: Artist-level scorecard using meaningful metric sources.
    """
    logger.info("Query: Artist scorecard")

    result = spark.sql(f"""
        WITH latest AS (
            SELECT *
            FROM artist_popularity
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM artist_popularity)
        )
        SELECT
            artist_name,
            MAX(CASE WHEN platform = 'lastfm' THEN listeners END) AS lastfm_listeners,
            MAX(CASE WHEN platform = 'lastfm' THEN playcount END) AS lastfm_playcount,
            MAX(CASE WHEN platform = 'deezer' THEN listeners END) AS deezer_fans,
            MAX(CASE WHEN platform = 'wikimedia' THEN listeners END) AS wikipedia_monthly_views,
            MAX(CASE WHEN platform = 'listenbrainz' THEN playcount END) AS listenbrainz_listens,
            MAX(CASE WHEN platform = 'musicbrainz' THEN popularity_score END) AS musicbrainz_rating,
            COALESCE(
                MAX(CASE WHEN platform = 'lastfm' THEN genres END),
                MAX(CASE WHEN platform = 'musicbrainz' THEN genres END),
                MAX(CASE WHEN platform = 'wikidata' THEN genres END),
                MAX(CASE WHEN platform = 'theaudiodb' THEN genres END),
                MAX(CASE WHEN platform = 'itunes' THEN genres END)
            ) AS primary_genres
        FROM latest
        GROUP BY artist_name
        ORDER BY lastfm_listeners DESC NULLS LAST,
                 deezer_fans DESC NULLS LAST,
                 wikipedia_monthly_views DESC NULLS LAST
        LIMIT {n}
    """)

    result.show(n, truncate=False)
    return result


def composite_reach_rank(spark, n=25):
    """
    Query 2: Percentile-normalized artist reach across metric sources.
    """
    logger.info("Query: Composite reach rank")

    result = spark.sql(f"""
        WITH latest AS (
            SELECT *
            FROM artist_popularity
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM artist_popularity)
        ),
        metrics AS (
            {_metric_source_sql()}
        ),
        ranked AS (
            SELECT
                artist_name,
                signal,
                metric_value,
                PERCENT_RANK() OVER (
                    PARTITION BY signal
                    ORDER BY metric_value
                ) AS percentile
            FROM metrics
            WHERE metric_value IS NOT NULL AND metric_value > 0
        )
        SELECT
            artist_name,
            ROUND(AVG(percentile) * 100, 2) AS composite_reach_score,
            COUNT(*) AS metric_signals,
            ROUND(MAX(CASE WHEN signal = 'lastfm_listeners' THEN percentile END) * 100, 2)
                AS lastfm_percentile,
            ROUND(MAX(CASE WHEN signal = 'deezer_fans' THEN percentile END) * 100, 2)
                AS deezer_percentile,
            ROUND(MAX(CASE WHEN signal = 'wikimedia_views' THEN percentile END) * 100, 2)
                AS wikipedia_percentile,
            ROUND(MAX(CASE WHEN signal = 'listenbrainz_listens' THEN percentile END) * 100, 2)
                AS listenbrainz_percentile,
            ROUND(MAX(CASE WHEN signal = 'musicbrainz_rating' THEN percentile END) * 100, 2)
                AS musicbrainz_percentile
        FROM ranked
        GROUP BY artist_name
        ORDER BY composite_reach_score DESC, metric_signals DESC, artist_name
        LIMIT {n}
    """)

    result.show(n, truncate=False)
    return result


def platform_gap_analysis(spark, n=25):
    """
    Query 3: Artists whose rank differs most across metric sources.
    """
    logger.info("Query: Platform gap analysis")

    result = spark.sql(f"""
        WITH latest AS (
            SELECT *
            FROM artist_popularity
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM artist_popularity)
        ),
        metrics AS (
            {_metric_source_sql()}
        ),
        ranked AS (
            SELECT
                artist_name,
                signal,
                DENSE_RANK() OVER (
                    PARTITION BY signal
                    ORDER BY metric_value DESC
                ) AS signal_rank
            FROM metrics
            WHERE metric_value IS NOT NULL AND metric_value > 0
        )
        SELECT
            artist_name,
            COUNT(*) AS signals_present,
            MIN(signal_rank) AS best_rank,
            MAX(signal_rank) AS worst_rank,
            MAX(signal_rank) - MIN(signal_rank) AS rank_spread,
            SORT_ARRAY(
                COLLECT_LIST(CONCAT(signal, '=', CAST(signal_rank AS STRING)))
            ) AS rank_details
        FROM ranked
        GROUP BY artist_name
        HAVING COUNT(*) >= 2
        ORDER BY rank_spread DESC, signals_present DESC, artist_name
        LIMIT {n}
    """)

    result.show(n, truncate=False)
    return result


def genre_reach_summary(spark, n=50):
    """
    Query 4: Genre-level reach summary across metric and metadata sources.
    """
    logger.info("Query: Genre reach summary")

    result = spark.sql(f"""
        WITH latest AS (
            SELECT *
            FROM artist_popularity
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM artist_popularity)
        ),
        artist_metrics AS (
            SELECT
                artist_name,
                MAX(CASE WHEN platform = 'lastfm' THEN listeners END) AS lastfm_listeners,
                MAX(CASE WHEN platform = 'deezer' THEN listeners END) AS deezer_fans,
                MAX(CASE WHEN platform = 'wikimedia' THEN listeners END) AS wikipedia_views,
                MAX(CASE WHEN platform = 'listenbrainz' THEN playcount END) AS listenbrainz_listens,
                MAX(CASE WHEN platform = 'musicbrainz' THEN popularity_score END) AS musicbrainz_rating
            FROM latest
            GROUP BY artist_name
        ),
        genre_rows AS (
            SELECT
                artist_name,
                TRIM(genre_value) AS genre
            FROM latest
            LATERAL VIEW EXPLODE(SPLIT(genres, ',')) exploded_genres AS genre_value
            WHERE genres IS NOT NULL
        )
        SELECT
            genre,
            COUNT(DISTINCT g.artist_name) AS artist_count,
            ROUND(AVG(m.lastfm_listeners), 0) AS avg_lastfm_listeners,
            ROUND(AVG(m.deezer_fans), 0) AS avg_deezer_fans,
            ROUND(AVG(m.wikipedia_views), 0) AS avg_wikipedia_views,
            ROUND(AVG(m.listenbrainz_listens), 0) AS avg_listenbrainz_listens,
            ROUND(AVG(m.musicbrainz_rating), 1) AS avg_musicbrainz_rating
        FROM genre_rows g
        LEFT JOIN artist_metrics m
            ON g.artist_name = m.artist_name
        WHERE genre <> ''
          AND LOWER(genre) NOT IN ('seen live')
        GROUP BY genre
        HAVING COUNT(DISTINCT g.artist_name) >= 2
        ORDER BY artist_count DESC,
                 avg_lastfm_listeners DESC NULLS LAST,
                 genre
        LIMIT {n}
    """)

    result.show(n, truncate=False)
    return result


def metadata_coverage(spark, n=50):
    """
    Query 5: Enrichment-source coverage without pretending it is popularity.
    """
    logger.info("Query: Metadata coverage")

    result = spark.sql(f"""
        WITH latest AS (
            SELECT *
            FROM artist_popularity
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM artist_popularity)
        ),
        per_artist AS (
            SELECT
                artist_name,
                MAX(CASE WHEN platform = 'itunes' THEN artist_id END) AS itunes_id,
                MAX(CASE WHEN platform = 'wikidata' THEN artist_id END) AS wikidata_id,
                MAX(CASE WHEN platform = 'theaudiodb' THEN artist_id END) AS theaudiodb_id,
                MAX(CASE WHEN platform = 'itunes' THEN genres END) AS itunes_genre,
                MAX(CASE WHEN platform = 'wikidata' THEN genres END) AS wikidata_genres,
                MAX(CASE WHEN platform = 'theaudiodb' THEN genres END) AS audiodb_genres
            FROM latest
            GROUP BY artist_name
        )
        SELECT
            artist_name,
            CASE WHEN itunes_id IS NOT NULL THEN 1 ELSE 0 END AS has_itunes_id,
            CASE WHEN wikidata_id IS NOT NULL THEN 1 ELSE 0 END AS has_wikidata_id,
            CASE WHEN theaudiodb_id IS NOT NULL THEN 1 ELSE 0 END AS has_theaudiodb_id,
            CONCAT_WS(' | ', audiodb_genres, wikidata_genres, itunes_genre)
                AS enrichment_genres
        FROM per_artist
        ORDER BY
            has_itunes_id + has_wikidata_id + has_theaudiodb_id DESC,
            artist_name
        LIMIT {n}
    """)

    result.show(n, truncate=False)
    return result


def artist_profile(spark, artist_name):
    """
    Query 6: Detail view for one artist across enabled sources.
    """
    logger.info("Query: Artist profile for '%s'", artist_name)

    safe_name = artist_name.replace("'", "''")
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
        WHERE LOWER(artist_name) = LOWER('{safe_name}')
        ORDER BY
            CASE platform
                WHEN 'lastfm' THEN 1
                WHEN 'deezer' THEN 2
                WHEN 'wikimedia' THEN 3
                WHEN 'listenbrainz' THEN 4
                WHEN 'musicbrainz' THEN 5
                ELSE 6
            END,
            platform,
            snapshot_date
    """)

    result.show(truncate=False)
    return result


def dataset_health_summary(spark):
    """
    Supporting summary for enabled-source data volume and coverage.
    """
    logger.info("Query: Dataset health summary")

    result = spark.sql("""
        WITH latest AS (
            SELECT *
            FROM artist_popularity
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM artist_popularity)
        )
        SELECT
            COUNT(*) AS latest_snapshot_rows,
            COUNT(DISTINCT artist_name) AS unique_artists,
            COUNT(DISTINCT platform) AS enabled_platforms,
            SORT_ARRAY(COLLECT_SET(platform)) AS platforms_present,
            MAX(snapshot_date) AS latest_snapshot
        FROM latest
    """)

    result.show(truncate=False)
    return result


def run_all_queries(spark, df, enabled_sources=None):
    """
    Execute the portfolio query suite against enabled source data.
    """
    register_table(spark, df, enabled_sources=enabled_sources)

    print("\n" + "=" * 70)
    print("ANALYTICAL QUERY RESULTS")
    print("=" * 70)

    print("\n--- Query 1: Artist Scorecard ---")
    artist_scorecard(spark, n=50)

    print("\n--- Query 2: Composite Reach Rank ---")
    composite_reach_rank(spark, n=25)

    print("\n--- Query 3: Platform Gap Analysis ---")
    platform_gap_analysis(spark, n=25)

    print("\n--- Query 4: Genre Reach Summary ---")
    genre_reach_summary(spark, n=50)

    print("\n--- Query 5: Metadata Coverage ---")
    metadata_coverage(spark, n=50)

    print("\n--- Query 6: Artist Profile (Radiohead) ---")
    artist_profile(spark, "Radiohead")

    print("\n--- Enabled-Source Dataset Health ---")
    dataset_health_summary(spark)

    print("\n" + "=" * 70)
    print("ALL QUERIES COMPLETE")
    print("=" * 70)
