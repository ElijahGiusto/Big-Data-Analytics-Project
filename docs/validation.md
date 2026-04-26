# Validation Report

Latest validated run: **April 26, 2026**  
Command: `python src/main.py`

## Summary

| Metric | Result |
| --- | ---: |
| Runtime | 581.7 seconds |
| Historical rows loaded | 1,320 |
| Latest snapshot rows | 1,162 |
| Source platforms present | 8 enabled sources |
| Spotify rows in latest snapshot | 0 |
| Canonical artists | 150/150 |
| Duplicate artist-platform-date groups | 0 |
| Validation result | Passed |

## Platform Coverage

| Platform | Rows | Coverage |
| --- | ---: | ---: |
| Last.fm | 150/150 | 100.0% |
| Deezer | 150/150 | 100.0% |
| Wikimedia | 150/150 | 100.0% |
| iTunes | 149/150 | 99.3% |
| MusicBrainz | 147/150 | 98.0% |
| ListenBrainz | 146/150 | 97.3% |
| TheAudioDB | 144/150 | 96.0% |
| Wikidata | 126/150 | 84.0% |

## Data Quality Checks

| Check | Result | Evidence |
| --- | --- | --- |
| Required columns present | Pass | All unified schema columns loaded from Parquet |
| Non-null key fields | Pass | 0 nulls in artist/platform/snapshot key fields |
| Canonical artist count | Pass | 150 unique artists in latest snapshot |
| Artist names match config | Pass | 0 names outside configured artist list |
| Duplicate rows | Pass | 0 duplicate artist-platform-date groups |
| Multi-source coverage | Pass | 8 enabled source platforms present |
| Required metric platforms | Pass | Last.fm, Deezer, MusicBrainz, Wikimedia present |
| Metric completeness | Pass | Last.fm 150, Deezer 150, MusicBrainz 147, Wikimedia 150 rows with `popularity_score` |
| Non-negative metrics | Pass | 0 rows with negative metric values |
| Bounded scores | Pass | 0 MusicBrainz scores outside 0-100 |

## Correctness Sample

Radiohead appears across all eight enabled latest-snapshot sources:

| Source | Metric/Metadata |
| --- | --- |
| Last.fm | 8,233,471 listeners; 1,373,615,108 plays |
| Deezer | 4,020,215 fans |
| ListenBrainz | 5,956,119 listens |
| Wikimedia | 3,967 latest-month pageviews; 257,469 rolling-window pageviews |
| MusicBrainz | 90/100 community rating from 76 votes |
| iTunes | Apple artist ID `657515`; genre `Alternative` |
| TheAudioDB | Genre/style metadata: `Alternative Rock, Rock/Pop, Sad` |
| Wikidata | QID `Q44190` |

## Edge Cases

- **Spotify API restriction:** Spotify is disabled by default because the current API response available to this project does not provide useful artist popularity, follower, or genre metrics. The latest validated snapshot contains 0 Spotify rows.
- **Artist name variants:** The normalizer maps variants such as `Björk`, `TOOL`, `Mac Demarco`, `Tyler, The Creator`, and `King Gizzard & The Lizard Wizard` to configured canonical names.
- **Expanded artist list:** The default list now contains 150 unique canonical artists. The original 40-artist seed list is retained at the top of `config/settings.yaml`.
- **MusicBrainz gaps:** MusicBrainz missed 3 of 150 artists in the latest run; validation treats MusicBrainz as required but allows partial coverage because the public database can have lookup gaps.
- **ListenBrainz dependency:** ListenBrainz depends on MusicBrainz IDs, so it is useful but not a required metric source.
- **iTunes throttling:** iTunes runs sequentially at about 3.1 seconds per request and caches successful lookups. This stays under Apple's approximately 20 calls/minute Search API guidance and avoided 429 errors in the validated run.
- **Catalog-only sources:** TheAudioDB, iTunes, and Wikidata improve metadata richness but are not treated as popularity sources.

## Reproduce

```bash
python src/main.py
python src/main.py --query-only
python src/main.py --validate-only
python -m unittest discover -s tests
```

For HDFS mode:

```bash
docker compose up -d --build
python src/main.py --storage hdfs
python src/main.py --validate-only --storage hdfs
```
