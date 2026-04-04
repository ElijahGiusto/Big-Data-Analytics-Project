# Data Dictionary

## Unified Schema: `artist_popularity`

All data from Spotify, Last.fm, Deezer, and MusicBrainz is normalized into
this schema before being written to Parquet. The schema is enforced by
PySpark's `StructType` definition in `src/processing/normalize.py`.

| Field              | Type    | Nullable | Description                                                        |
| ------------------ | ------- | -------- | ------------------------------------------------------------------ |
| `artist_name`      | string  | No       | Display name of the artist (e.g., "Radiohead")                     |
| `artist_id`        | string  | No       | Platform-specific unique identifier (Spotify ID, Last.fm MBID, Deezer ID, or MusicBrainz UUID) |
| `platform`         | string  | No       | Source platform: `"spotify"`, `"lastfm"`, `"deezer"`, or `"musicbrainz"` |
| `snapshot_date`    | string  | No       | Date of data collection in `YYYY-MM-DD` format                     |
| `popularity_score` | integer | Yes      | Platform-native popularity metric (see Platform Metrics below)     |
| `listeners`        | long    | Yes      | Listener or fan count from the platform                            |
| `playcount`        | long    | Yes      | Total play count (available from Last.fm only)                     |
| `genres`           | string  | Yes      | Comma-separated genre tags (available from Last.fm and MusicBrainz) |

---

## Platform Metrics

Each platform provides different raw metrics. The normalization step maps
them into the unified schema as follows:

### Spotify
- **popularity_score**: Not available (null) — Client Credentials auth restricts the search endpoint
- **listeners**: Not available (null)
- **playcount**: Not available (null)
- **genres**: Not available (null)
- **Note**: Spotify contributes artist name and Spotify ID for cross-platform identification. Popularity metrics require user-level OAuth scopes not available under Client Credentials.

### Last.fm
- **popularity_score**: Total listener count (raw value, unique users who played the artist)
- **listeners**: Same as popularity_score (total unique listeners)
- **playcount**: Total cumulative play count across all tracks
- **genres**: Top 5 user-applied tags from Last.fm

### Deezer
- **popularity_score**: Number of fans (nb_fan, users who favorited the artist)
- **listeners**: Same as popularity_score (fan count)
- **playcount**: Not available (null)
- **genres**: Not available (null)

### MusicBrainz
- **popularity_score**: Community rating scaled to 0–100 (original 0–5 scale × 20)
- **listeners**: Number of rating votes (votes-count)
- **playcount**: Not available (null)
- **genres**: Top 5 community-applied tags sorted by vote count

---

## Parquet Partitioning

Processed data is stored in Parquet format partitioned by:

```
data/processed/
    platform=spotify/
        snapshot_date=2026-04-04/
            part-00000-*.parquet
    platform=lastfm/
        snapshot_date=2026-04-04/
            ...
    platform=deezer/
        snapshot_date=2026-04-04/
            ...
    platform=musicbrainz/
        snapshot_date=2026-04-04/
            ...
```

This layout enables:
- **Platform-specific queries**: Read only one platform's partition
- **Time-range scans**: Access specific date ranges efficiently
- **Incremental appends**: New pipeline runs add partitions without overwriting history

---

## Raw Data Files

Raw API responses are stored as timestamped JSON files:

| File Pattern                          | Contents                             |
| ------------------------------------- | ------------------------------------ |
| `data/raw/spotify_YYYYMMDD.json`      | Raw Spotify search API responses     |
| `data/raw/lastfm_YYYYMMDD.json`       | Raw Last.fm artist.getinfo results   |
| `data/raw/deezer_YYYYMMDD.json`       | Raw Deezer artist search results     |
| `data/raw/musicbrainz_YYYYMMDD.json`  | Raw MusicBrainz artist detail results|

Raw files preserve the original API responses for reproducibility and
reprocessing if the normalization logic changes.
