"""
Data Normalization Module

Transforms raw API responses from multiple streaming platforms into a
unified Spark DataFrame schema. Each platform's heterogeneous data format
is mapped (map phase) into the normalized schema, then unioned together
(reduce phase) to produce a single cross-platform dataset.

Note: DataFrames are constructed via pandas intermediary to avoid
PySpark Row serialization issues on Python 3.13+. The pandas-to-Spark
conversion uses Apache Arrow for efficient transfer.

Unified Schema (from M1 Pitch):
    artist_name     : string   - Display name of the artist
    artist_id       : string   - Platform-specific unique identifier
    platform        : string   - Source platform (spotify, lastfm, deezer)
    snapshot_date   : string   - Date of data collection (YYYY-MM-DD)
    popularity_score: integer  - Platform-native popularity metric
    listeners       : long     - Listener or fan count
    playcount       : long     - Total play count (where available)
    genres          : string   - Comma-separated genre tags
"""

import logging
import pandas as pd
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType
)

logger = logging.getLogger(__name__)

# Unified schema enforced across all platform records
UNIFIED_SCHEMA = StructType([
    StructField("artist_name", StringType(), False),
    StructField("artist_id", StringType(), False),
    StructField("platform", StringType(), False),
    StructField("snapshot_date", StringType(), False),
    StructField("popularity_score", IntegerType(), True),
    StructField("listeners", LongType(), True),
    StructField("playcount", LongType(), True),
    StructField("genres", StringType(), True),
])


def _to_spark_df(spark, records):
    """
    Convert a list of dicts to a Spark DataFrame via pandas.

    Using pandas as an intermediary avoids PySpark's Row pickling
    which causes RecursionError on Python 3.13+. The pandas-to-Spark
    path uses Apache Arrow for efficient columnar transfer.

    Args:
        spark: Active SparkSession.
        records: List of dicts matching the unified schema.

    Returns:
        DataFrame: Spark DataFrame with UNIFIED_SCHEMA applied.
    """
    if not records:
        empty_pdf = pd.DataFrame(columns=[
            "artist_name", "artist_id", "platform", "snapshot_date",
            "popularity_score", "listeners", "playcount", "genres",
        ])
        return spark.createDataFrame(empty_pdf, schema=UNIFIED_SCHEMA)

    pdf = pd.DataFrame(records)

    # Ensure correct types before Spark conversion
    pdf["popularity_score"] = pdf["popularity_score"].astype("Int32")
    pdf["listeners"] = pdf["listeners"].astype("Int64")
    pdf["playcount"] = pdf["playcount"].astype("Int64")

    return spark.createDataFrame(pdf, schema=UNIFIED_SCHEMA)


def normalize_spotify(spark, raw_data, snapshot_date):
    """
    Transform raw Spotify search results into unified schema rows.

    Spotify's search endpoint (under Client Credentials auth) returns
    artist name and ID only — popularity, followers, and genres are not
    included. Spotify's role in the pipeline is providing artist
    identification for cross-platform matching.

    Args:
        spark: Active SparkSession.
        raw_data: List of raw Spotify artist dicts.
        snapshot_date: Date string (YYYY-MM-DD) for this snapshot.

    Returns:
        DataFrame: Normalized Spotify records.
    """
    records = []

    for artist in raw_data:
        try:
            name = artist.get("name", "Unknown")
            artist_id = artist.get("id", "")

            records.append({
                "artist_name": name,
                "artist_id": artist_id,
                "platform": "spotify",
                "snapshot_date": snapshot_date,
                "popularity_score": None,
                "listeners": None,
                "playcount": None,
                "genres": None,
            })
        except (KeyError, TypeError, ValueError) as e:
            logger.warning("Skipping malformed Spotify record: %s", e)
            continue

    df = _to_spark_df(spark, records)
    logger.info("Normalized %d Spotify records", df.count())
    return df


def normalize_lastfm(spark, raw_data, snapshot_date):
    """
    Transform raw Last.fm artist objects into unified schema rows.

    Last.fm provides listener count and total playcount. The listener
    count is used as the popularity_score (raw value, not 0-100).

    Args:
        spark: Active SparkSession.
        raw_data: List of raw Last.fm artist dicts.
        snapshot_date: Date string (YYYY-MM-DD) for this snapshot.

    Returns:
        DataFrame: Normalized Last.fm records.
    """
    records = []

    for artist in raw_data:
        try:
            name = artist.get("name", "Unknown")
            mbid = artist.get("mbid", "")
            stats = artist.get("stats", {})
            listeners = int(stats.get("listeners", 0))
            playcount = int(stats.get("playcount", 0))

            # Extract top tags as genre approximation
            tags = artist.get("tags", {}).get("tag", [])
            genres = ", ".join(t["name"] for t in tags[:5]) if tags else None

            records.append({
                "artist_name": name,
                "artist_id": mbid if mbid else name.lower().replace(" ", ""),
                "platform": "lastfm",
                "snapshot_date": snapshot_date,
                "popularity_score": int(listeners),
                "listeners": int(listeners),
                "playcount": int(playcount),
                "genres": genres,
            })
        except (KeyError, TypeError, ValueError) as e:
            logger.warning("Skipping malformed Last.fm record: %s", e)
            continue

    df = _to_spark_df(spark, records)
    logger.info("Normalized %d Last.fm records", df.count())
    return df


def normalize_deezer(spark, raw_data, snapshot_date):
    """
    Transform raw Deezer artist objects into unified schema rows.

    Deezer provides a fan count (nb_fan) which serves as the popularity
    metric. Album count is available but not directly mapped.

    Args:
        spark: Active SparkSession.
        raw_data: List of raw Deezer artist dicts.
        snapshot_date: Date string (YYYY-MM-DD) for this snapshot.

    Returns:
        DataFrame: Normalized Deezer records.
    """
    records = []

    for artist in raw_data:
        try:
            name = artist.get("name", "Unknown")
            artist_id = str(artist.get("id", ""))
            nb_fan = artist.get("nb_fan", 0)

            records.append({
                "artist_name": name,
                "artist_id": artist_id,
                "platform": "deezer",
                "snapshot_date": snapshot_date,
                "popularity_score": int(nb_fan),
                "listeners": int(nb_fan),
                "playcount": None,
                "genres": None,
            })
        except (KeyError, TypeError, ValueError) as e:
            logger.warning("Skipping malformed Deezer record: %s", e)
            continue

    df = _to_spark_df(spark, records)
    logger.info("Normalized %d Deezer records", df.count())
    return df


def normalize_musicbrainz(spark, raw_data, snapshot_date):
    """
    Transform raw MusicBrainz artist objects into unified schema rows.

    MusicBrainz provides community-submitted ratings (0-100 scale)
    and genre tags with vote counts. The rating vote count serves as
    a listener-equivalent metric.

    Args:
        spark: Active SparkSession.
        raw_data: List of raw MusicBrainz artist dicts.
        snapshot_date: Date string (YYYY-MM-DD) for this snapshot.

    Returns:
        DataFrame: Normalized MusicBrainz records.
    """
    records = []

    for artist in raw_data:
        try:
            name = artist.get("name", "Unknown")
            artist_id = artist.get("id", "")

            # Rating is 0-5 scale, convert to 0-100
            rating_info = artist.get("rating", {})
            rating_value = rating_info.get("value", 0) or 0
            rating_count = rating_info.get("votes-count", 0) or 0
            popularity = int(float(rating_value) * 20)  # 0-5 -> 0-100

            # Extract tags as genres (sorted by vote count)
            tags = artist.get("tags", [])
            if tags:
                sorted_tags = sorted(tags, key=lambda t: t.get("count", 0), reverse=True)
                genres = ", ".join(t["name"] for t in sorted_tags[:5])
            else:
                genres = None

            records.append({
                "artist_name": name,
                "artist_id": artist_id,
                "platform": "musicbrainz",
                "snapshot_date": snapshot_date,
                "popularity_score": popularity,
                "listeners": int(rating_count),
                "playcount": None,
                "genres": genres,
            })
        except (KeyError, TypeError, ValueError) as e:
            logger.warning("Skipping malformed MusicBrainz record: %s", e)
            continue

    df = _to_spark_df(spark, records)
    logger.info("Normalized %d MusicBrainz records", df.count())
    return df


def normalize_all(spark, platform_data, snapshot_date):
    """
    Normalize and union data from all platforms into a single DataFrame.

    This is the reduce phase of the MapReduce pattern: each platform's
    data has been independently mapped to the unified schema, and this
    function unions them into one cohesive dataset.

    Args:
        spark: Active SparkSession.
        platform_data: Dict mapping platform name to raw data list.
            Example: {"spotify": [...], "lastfm": [...], "deezer": [...], "musicbrainz": [...]}
        snapshot_date: Date string (YYYY-MM-DD) for this snapshot.

    Returns:
        DataFrame: Combined and normalized dataset across all platforms.
    """
    logger.info("Starting normalization for snapshot %s", snapshot_date)

    normalizers = {
        "spotify": normalize_spotify,
        "lastfm": normalize_lastfm,
        "deezer": normalize_deezer,
        "musicbrainz": normalize_musicbrainz,
    }

    dfs = []

    for platform, raw_data in platform_data.items():
        if raw_data and platform in normalizers:
            dfs.append(normalizers[platform](spark, raw_data, snapshot_date))

    if not dfs:
        logger.error("No data from any platform to normalize")
        empty_pdf = pd.DataFrame(columns=[
            "artist_name", "artist_id", "platform", "snapshot_date",
            "popularity_score", "listeners", "playcount", "genres",
        ])
        return spark.createDataFrame(empty_pdf, schema=UNIFIED_SCHEMA)

    # Union all platform DataFrames (reduce phase)
    unified = dfs[0]
    for df in dfs[1:]:
        unified = unified.unionByName(df)

    total = unified.count()
    logger.info(
        "Normalization complete: %d total records across %d platforms",
        total, len(dfs),
    )

    return unified