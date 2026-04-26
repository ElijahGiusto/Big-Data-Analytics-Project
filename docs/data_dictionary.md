# Data Dictionary

## Unified Table: `artist_popularity`

All raw API responses are normalized into this PySpark schema before being written to Parquet.

| Field | Type | Nullable | Description |
| --- | --- | --- | --- |
| `artist_name` | string | No | Canonical artist name from `config/settings.yaml` |
| `artist_id` | string | No | Platform-specific identifier, such as MusicBrainz UUID, Wikidata QID, Wikipedia page title, Deezer artist ID, or iTunes artist ID |
| `platform` | string | No | Source platform/API |
| `snapshot_date` | string | No | Pipeline run date in `YYYY-MM-DD` format |
| `popularity_score` | integer | Yes | Source-native metric when available |
| `listeners` | long | Yes | Listener, fan, view, vote, or source-equivalent reach count when available |
| `playcount` | long | Yes | Total plays/listens/pageviews over a source-specific period when available |
| `genres` | string | Yes | Comma-separated genre/tag/style metadata |

## Source Mapping

| Platform | `popularity_score` | `listeners` | `playcount` | `genres` |
| --- | --- | --- | --- | --- |
| `lastfm` | Listener count | Listener count | Total Last.fm playcount | Top 5 Last.fm tags |
| `deezer` | Fan count | Fan count | Null | Null |
| `musicbrainz` | Community rating scaled 0-100 | Rating vote count | Null | Top 5 community tags |
| `listenbrainz` | Total ListenBrainz listens | Count of returned top listeners | Total ListenBrainz listens | Null |
| `wikimedia` | Latest complete month pageviews | Latest complete month pageviews | Sum of configured rolling pageview window | Null |
| `theaudiodb` | Null | Null | Null | Genre/style/mood metadata |
| `itunes` | Null | Null | Null | Apple primary genre |
| `wikidata` | Null | Null | Null | Wikidata genre claims |

Spotify normalization remains in the codebase for optional catalog matching, but `spotify` is disabled in the default source configuration and is not required for validation or analysis.

## Parquet Partitioning

Processed data is stored in Parquet format partitioned by source and run date. Local mode writes to `data/processed`; HDFS mode writes to `hdfs://localhost:9000/artist-popularity/processed`.

```text
data/processed/
    platform=lastfm/
        snapshot_date=2026-04-26/
            part-*.parquet
    platform=wikimedia/
        snapshot_date=2026-04-26/
            part-*.parquet
```

The HDFS layout uses the same partition names:

```text
hdfs://localhost:9000/artist-popularity/processed/
    platform=lastfm/
        snapshot_date=2026-04-26/
            part-*.parquet
```

Same-day reruns remove existing partitions for the current snapshot date before writing, then use dynamic partition overwrite. This keeps repeat runs from duplicating rows and prevents disabled sources from lingering in the latest snapshot.

## Raw Files

Raw API responses are written as JSON files in `data/raw` using this pattern:

```text
{platform}_YYYYMMDD.json
```

Raw JSON files are ignored by Git because they are reproducible outputs and may contain API response metadata. A small sample dataset is committed under `data/sample`.
