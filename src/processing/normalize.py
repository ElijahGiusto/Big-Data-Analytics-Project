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
    platform        : string   - Source platform/API
    snapshot_date   : string   - Date of data collection (YYYY-MM-DD)
    popularity_score: integer  - Platform-native popularity metric
    listeners       : long     - Listener or fan count
    playcount       : long     - Total play count (where available)
    genres          : string   - Comma-separated genre tags
"""

import logging
import re
import unicodedata
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

UNIFIED_COLUMNS = [
    "artist_name", "artist_id", "platform", "snapshot_date",
    "popularity_score", "listeners", "playcount", "genres",
]


def _canonical_key(name):
    """
    Convert artist names into a stable comparison key.

    This removes casing, accents, punctuation, and spacing differences
    that otherwise split one artist into multiple groups across APIs.
    """
    if not name:
        return ""

    normalized = unicodedata.normalize("NFKD", str(name))
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_name = ascii_name.replace("&", " and ")
    return re.sub(r"[^a-z0-9]+", "", ascii_name.lower())


def build_artist_lookup(expected_artists=None):
    """Build canonical artist-name lookup from the configured artist list."""
    lookup = {}
    for artist in expected_artists or []:
        lookup[_canonical_key(artist)] = artist
    return lookup


def canonicalize_artist_name(name, artist_lookup=None):
    """Return the configured artist spelling when a platform variant matches."""
    artist_lookup = artist_lookup or {}
    key = _canonical_key(name)
    return artist_lookup.get(key, name or "Unknown")


def _record_name(artist, artist_lookup):
    """Prefer the configured source name, then fall back to the API name."""
    source_name = artist.get("source_artist_name") or artist.get("name")
    return canonicalize_artist_name(source_name, artist_lookup)


def _optional_int(value):
    """Convert optional numeric fields without turning missing values into 0."""
    if value in (None, ""):
        return None
    return int(value)


def _genres_to_string(genres):
    """Normalize genre/tag lists to a comma-separated string."""
    if not genres:
        return None
    if isinstance(genres, list):
        names = []
        for genre in genres[:5]:
            if isinstance(genre, dict):
                name = genre.get("name")
            else:
                name = str(genre)
            if name:
                names.append(name)
        return ", ".join(names) if names else None
    return str(genres)


def _join_nonempty(values):
    """Join non-empty metadata values into a comma-separated string."""
    cleaned = []
    for value in values:
        if isinstance(value, list):
            cleaned.extend(str(item) for item in value if item)
        elif value:
            cleaned.append(str(value))
    deduped = []
    for value in cleaned:
        if value not in deduped:
            deduped.append(value)
    return ", ".join(deduped) if deduped else None


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
        empty_pdf = pd.DataFrame(columns=UNIFIED_COLUMNS)
        return spark.createDataFrame(empty_pdf, schema=UNIFIED_SCHEMA)

    pdf = pd.DataFrame(records)
    for column in UNIFIED_COLUMNS:
        if column not in pdf.columns:
            pdf[column] = None
    pdf = pdf[UNIFIED_COLUMNS]

    # Ensure correct types before Spark conversion
    pdf["popularity_score"] = pdf["popularity_score"].astype("Int32")
    pdf["listeners"] = pdf["listeners"].astype("Int64")
    pdf["playcount"] = pdf["playcount"].astype("Int64")

    return spark.createDataFrame(pdf, schema=UNIFIED_SCHEMA)


def normalize_spotify(spark, raw_data, snapshot_date, artist_lookup=None):
    """
    Transform raw Spotify search results into unified schema rows.

    Spotify's current developer-mode responses may omit artist popularity,
    followers, and genres. The normalizer captures those fields when they
    are present and otherwise preserves Spotify as a catalog-ID source.

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
            name = _record_name(artist, artist_lookup)
            artist_id = artist.get("id", "")
            followers = artist.get("followers", {}).get("total")
            popularity = artist.get("popularity")
            genres = _genres_to_string(artist.get("genres"))

            records.append({
                "artist_name": name,
                "artist_id": artist_id,
                "platform": "spotify",
                "snapshot_date": snapshot_date,
                "popularity_score": _optional_int(popularity),
                "listeners": _optional_int(followers),
                "playcount": None,
                "genres": genres,
            })
        except (KeyError, TypeError, ValueError) as e:
            logger.warning("Skipping malformed Spotify record: %s", e)
            continue

    df = _to_spark_df(spark, records)
    logger.info("Normalized %d Spotify records", df.count())
    return df


def normalize_lastfm(spark, raw_data, snapshot_date, artist_lookup=None):
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
            name = _record_name(artist, artist_lookup)
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


def normalize_deezer(spark, raw_data, snapshot_date, artist_lookup=None):
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
            name = _record_name(artist, artist_lookup)
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


def normalize_musicbrainz(spark, raw_data, snapshot_date, artist_lookup=None):
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
            name = _record_name(artist, artist_lookup)
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


def normalize_listenbrainz(spark, raw_data, snapshot_date, artist_lookup=None):
    """
    Transform ListenBrainz artist listener statistics into unified rows.

    ListenBrainz provides total listen count for a MusicBrainz artist ID.
    That value is stored as the source-native popularity score and as
    playcount because it represents listen volume rather than followers.
    """
    records = []

    for artist in raw_data:
        try:
            name = _record_name(artist, artist_lookup)
            artist_id = artist.get("artist_mbid", "")
            total_listens = artist.get("total_listen_count")
            top_listeners = artist.get("listeners", []) or []

            records.append({
                "artist_name": name,
                "artist_id": artist_id,
                "platform": "listenbrainz",
                "snapshot_date": snapshot_date,
                "popularity_score": _optional_int(total_listens),
                "listeners": len(top_listeners) if top_listeners else None,
                "playcount": _optional_int(total_listens),
                "genres": None,
            })
        except (KeyError, TypeError, ValueError) as e:
            logger.warning("Skipping malformed ListenBrainz record: %s", e)
            continue

    df = _to_spark_df(spark, records)
    logger.info("Normalized %d ListenBrainz records", df.count())
    return df


def normalize_wikimedia(spark, raw_data, snapshot_date, artist_lookup=None):
    """
    Transform Wikimedia Pageviews records into unified rows.

    latest_month_views is the most recent complete month in the window;
    total_window_views is the sum across the configured rolling window.
    """
    records = []

    for artist in raw_data:
        try:
            name = _record_name(artist, artist_lookup)
            page_title = artist.get("page_title", name)
            latest_views = artist.get("latest_month_views")
            total_views = artist.get("total_window_views")

            records.append({
                "artist_name": name,
                "artist_id": page_title,
                "platform": "wikimedia",
                "snapshot_date": snapshot_date,
                "popularity_score": _optional_int(latest_views),
                "listeners": _optional_int(latest_views),
                "playcount": _optional_int(total_views),
                "genres": None,
            })
        except (KeyError, TypeError, ValueError) as e:
            logger.warning("Skipping malformed Wikimedia record: %s", e)
            continue

    df = _to_spark_df(spark, records)
    logger.info("Normalized %d Wikimedia records", df.count())
    return df


def normalize_theaudiodb(spark, raw_data, snapshot_date, artist_lookup=None):
    """
    Transform TheAudioDB artist metadata into unified rows.

    TheAudioDB is primarily an enrichment source, so metric columns remain
    null while genre/style metadata is retained.
    """
    records = []

    for artist in raw_data:
        try:
            name = _record_name(artist, artist_lookup)
            genres = _join_nonempty([
                artist.get("strGenre"),
                artist.get("strStyle"),
                artist.get("strMood"),
            ])

            records.append({
                "artist_name": name,
                "artist_id": artist.get("idArtist", ""),
                "platform": "theaudiodb",
                "snapshot_date": snapshot_date,
                "popularity_score": None,
                "listeners": None,
                "playcount": None,
                "genres": genres,
            })
        except (KeyError, TypeError, ValueError) as e:
            logger.warning("Skipping malformed TheAudioDB record: %s", e)
            continue

    df = _to_spark_df(spark, records)
    logger.info("Normalized %d TheAudioDB records", df.count())
    return df


def normalize_itunes(spark, raw_data, snapshot_date, artist_lookup=None):
    """
    Transform iTunes Search artist metadata into unified rows.
    """
    records = []

    for artist in raw_data:
        try:
            name = _record_name(artist, artist_lookup)
            genres = artist.get("primaryGenreName")

            records.append({
                "artist_name": name,
                "artist_id": str(artist.get("artistId", "")),
                "platform": "itunes",
                "snapshot_date": snapshot_date,
                "popularity_score": None,
                "listeners": None,
                "playcount": None,
                "genres": genres,
            })
        except (KeyError, TypeError, ValueError) as e:
            logger.warning("Skipping malformed iTunes record: %s", e)
            continue

    df = _to_spark_df(spark, records)
    logger.info("Normalized %d iTunes records", df.count())
    return df


def normalize_wikidata(spark, raw_data, snapshot_date, artist_lookup=None):
    """
    Transform Wikidata canonical identifiers and genre claims.
    """
    records = []

    for artist in raw_data:
        try:
            name = _record_name(artist, artist_lookup)
            genres = _genres_to_string(artist.get("genre_labels"))

            records.append({
                "artist_name": name,
                "artist_id": artist.get("id", ""),
                "platform": "wikidata",
                "snapshot_date": snapshot_date,
                "popularity_score": None,
                "listeners": None,
                "playcount": None,
                "genres": genres,
            })
        except (KeyError, TypeError, ValueError) as e:
            logger.warning("Skipping malformed Wikidata record: %s", e)
            continue

    df = _to_spark_df(spark, records)
    logger.info("Normalized %d Wikidata records", df.count())
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
    return normalize_all_with_artists(spark, platform_data, snapshot_date)


def normalize_all_with_artists(
    spark, platform_data, snapshot_date, expected_artists=None
):
    """
    Normalize and union data while canonicalizing artist names.

    expected_artists should be the configured artist list. When supplied,
    platform spellings such as "Björk", "TOOL", or "Tyler, The Creator"
    are normalized back to the configured name so cross-platform queries
    group by the intended artist list.
    """
    logger.info("Starting normalization for snapshot %s", snapshot_date)

    normalizers = {
        "spotify": normalize_spotify,
        "lastfm": normalize_lastfm,
        "deezer": normalize_deezer,
        "musicbrainz": normalize_musicbrainz,
        "listenbrainz": normalize_listenbrainz,
        "wikimedia": normalize_wikimedia,
        "theaudiodb": normalize_theaudiodb,
        "itunes": normalize_itunes,
        "wikidata": normalize_wikidata,
    }
    artist_lookup = build_artist_lookup(expected_artists)

    dfs = []

    for platform, raw_data in platform_data.items():
        if raw_data and platform in normalizers:
            dfs.append(
                normalizers[platform](
                    spark, raw_data, snapshot_date, artist_lookup
                )
            )

    if not dfs:
        logger.error("No data from any platform to normalize")
        empty_pdf = pd.DataFrame(columns=UNIFIED_COLUMNS)
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


def canonicalize_artist_names_df(df, expected_artists=None):
    """
    Canonicalize artist names in an existing Spark DataFrame.

    This keeps query-only and validation runs accurate even when they read
    older snapshots produced before canonicalization was added.
    """
    if not expected_artists:
        return df

    artist_lookup = build_artist_lookup(expected_artists)

    from pyspark.sql import functions as F

    accent_chars = "àáâãäåāçèéêëìíîïñòóôõöøùúûüýÿ"
    ascii_chars = "aaaaaaaceeeeiiiinoooooouuuuyy"

    artist_key = F.regexp_replace(
        F.translate(
            F.regexp_replace(F.lower(F.col("artist_name")), "&", " and "),
            accent_chars,
            ascii_chars,
        ),
        "[^a-z0-9]+",
        "",
    )

    mapping_entries = []
    for key, value in artist_lookup.items():
        mapping_entries.extend([F.lit(key), F.lit(value)])

    mapping_expr = F.create_map(*mapping_entries)
    return (
        df
        .withColumn("_artist_key", artist_key)
        .withColumn(
            "artist_name",
            F.coalesce(F.element_at(mapping_expr, F.col("_artist_key")), F.col("artist_name")),
        )
        .drop("_artist_key")
    )
