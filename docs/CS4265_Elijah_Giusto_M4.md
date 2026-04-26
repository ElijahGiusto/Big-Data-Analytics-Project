# CS 4265 Milestone 4 Final Report

**Project:** Artist Popularity Pipeline Across Music Platforms  
**Student:** Elijah Giusto  
**Date:** April 26, 2026

## Executive Summary

This project implements a batch-oriented Big Data pipeline for comparing artist popularity and metadata across multiple public music and web data sources. The pipeline tracks 150 artists and collects default data from Last.fm, Deezer, MusicBrainz, ListenBrainz, Wikimedia Pageviews, TheAudioDB, iTunes Search, and Wikidata. Each source exposes a different view of artist popularity: Last.fm provides listener and play counts, Deezer provides fan counts, Wikimedia provides public attention through pageviews, ListenBrainz provides open listening statistics, and MusicBrainz provides community ratings and tags.

The project solves a common analytics problem: no single music platform provides a complete, stable, and open view of artist popularity. Spotify originally seemed like a strong source, but during the project its available developer-mode API response stopped returning artist popularity, follower, and genre fields. Rather than hiding that limitation, the final version disables Spotify by default and expands the pipeline with open sources that provide richer metrics and metadata.

The final deliverable is portfolio-ready: the repository includes a professional README, pinned dependencies, a validation report, a data dictionary, architecture documentation, a sample dataset, a license, and a repeatable validation command.

## System Architecture

The pipeline follows a layered batch architecture:

1. **Configuration layer:** `config/settings.yaml` defines the tracked artists, enabled sources, API endpoints, rate-limit settings, storage paths, and Wikipedia title overrides for ambiguous artist names.
2. **Ingestion layer:** source-specific modules fetch raw JSON from APIs using retry logic and source-appropriate throttling.
3. **Raw data layer:** reproducible raw JSON snapshots are written to `data/raw`.
4. **Processing layer:** PySpark normalizes all source records into one schema and canonicalizes artist names.
5. **Storage layer:** processed data is written to Snappy-compressed Parquet partitioned by `platform` and `snapshot_date`, either locally or in HDFS.
6. **Analysis layer:** Spark SQL queries compare source-native popularity metrics and metadata coverage.
7. **Validation layer:** a validation gate checks schema, coverage, duplicates, canonical names, metric completeness, and edge cases.

The main technology choices are Python, PySpark, Spark SQL, Apache Hadoop HDFS, Parquet, YAML configuration, Docker Compose, and the `requests` API client. PySpark was chosen because the course focuses on Big Data pipelines and distributed processing concepts. Parquet was chosen because it is columnar, compressed, partition-friendly, and compatible with Spark SQL. HDFS mode provides a real Hadoop-backed storage target while local mode keeps the project easy to clone and run.

## Implementation Details

The ingestion stage uses one module per source. Each module handles request construction, rate limiting, transient errors, and raw JSON persistence. Independent sources run concurrently, while each module still enforces its own public API limits. MusicBrainz is run slowly because its public API requires approximately one request per second. iTunes is also sequential and cached because Apple documents the Search API at about 20 calls per minute. Public APIs such as Wikimedia, Wikidata, and ListenBrainz are called with identifiable user-agent headers where appropriate.

The normalization stage converts heterogeneous API responses into the shared schema:

`artist_name | artist_id | platform | snapshot_date | popularity_score | listeners | playcount | genres`

Because platforms use inconsistent artist names, the pipeline canonicalizes names against the configured artist list. This resolves variants such as `Björk` versus `Bjork`, `TOOL` versus `Tool`, `Tyler, The Creator` versus `Tyler the Creator`, and `King Gizzard & The Lizard Wizard` versus `King Gizzard and the Lizard Wizard`.

The storage stage writes partitioned Parquet. The default mode writes to `data/processed` for a one-command local run, and the scalable mode writes to `hdfs://localhost:9000/artist-popularity/processed` after starting the bundled Hadoop service with Docker Compose. The final version removes existing partitions for the current snapshot date before writing and then uses Spark dynamic partition overwrite. This is important for portfolio readiness because repeated validation/debug runs should not corrupt the dataset or leave disabled-source partitions in the latest snapshot.

The analysis layer now focuses on usable music insights: artist scorecards, percentile-normalized composite reach, platform gap analysis, genre reach summaries, metadata coverage, and artist-level profiles. Spotify fields are not included in the default analytical output.

## Validation And Results

The latest successful run completed on April 26, 2026 in 581.7 seconds. The processed dataset loaded 1,320 historical rows and the latest snapshot contained 1,162 rows across eight enabled source platforms. Spotify was disabled by default and the latest snapshot contained 0 Spotify rows.

Validation passed with the following evidence:

- 150/150 canonical artists were present.
- 0 key fields were null.
- 0 artist names were outside the configured list.
- 0 duplicate artist-platform-date groups were found.
- 8 enabled source platforms were present in the latest snapshot.
- Required metric platforms were populated: Last.fm, Deezer, MusicBrainz, and Wikimedia.
- All metric values were non-negative.
- MusicBrainz scores stayed within the expected 0-100 range.

Platform coverage in the latest snapshot was strong across the required metric sources: Last.fm, Deezer, and Wikimedia covered all 150 artists, while MusicBrainz covered 147. ListenBrainz covered 146 artists, iTunes covered 149, TheAudioDB covered 144, and Wikidata covered 126.

Radiohead is a representative correctness sample. The latest snapshot includes Last.fm listener/playcount data, Deezer fan data, ListenBrainz open listen counts, Wikimedia pageview metrics, MusicBrainz community rating and tags, iTunes catalog ID and genre, TheAudioDB style metadata, and Wikidata genre claims.

## Challenges

The largest challenge was API instability. Spotify changed its public developer-mode API behavior during the semester and no longer returned the key artist metrics that the project originally expected. The final solution adapts by documenting the limitation and expanding to open data sources instead of relying on hidden assumptions.

Another challenge was artist identity resolution. Even for a fixed list of artists, different APIs returned slightly different names and spellings. Without canonicalization, cross-platform queries produced 49-51 unique artist names in the 40-artist milestone run. The final version fixes this with a deterministic canonical-name key and scales the same validation to 150 artists.

Rate limiting also shaped the design. MusicBrainz, TheAudioDB, iTunes, and Wikidata cannot be called aggressively, so ingestion is intentionally throttled and iTunes uses a local cache for successful lookups. This increases runtime but produces a more responsible and reproducible pipeline.

## Reflection And Future Work

This project reinforced that data engineering is not just about making a pipeline run. A portfolio-ready pipeline must handle changing APIs, partial data, validation, documentation, and reproducibility. The final version is stronger because it treats source limitations as part of the design instead of hiding them.

Future improvements could include adding optional API-key sources such as Ticketmaster, setlist.fm, Discogs, and YouTube Data API; building a dashboard over the Parquet output; adding historical trend charts after multiple snapshots; and expanding from the single-node Docker HDFS service to a multi-node Hadoop or cloud object-storage deployment.

The final repository demonstrates course concepts including distributed processing with Spark, MapReduce-style ingestion/normalization, columnar storage, partitioned data layout, SQL analytics, data quality validation, and fault-tolerant source handling.
