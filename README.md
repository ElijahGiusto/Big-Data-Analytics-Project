# Distributed Analysis of Artist Popularity Trends Across Streaming Platforms

**CS 4265 — Big Data Analytics: Building Data Pipelines at Scale**
Elijah Giusto

---

## Project Overview

Music artists distribute content across multiple streaming platforms, each exposing distinct popularity metrics, update frequencies, and data formats. This project implements a distributed data pipeline that ingests artist-level popularity data from **Spotify**, **Last.fm**, **Deezer**, and **MusicBrainz**, normalizes it into a unified schema, stores it in partitioned Parquet format, and enables cross-platform analytical queries using Apache Spark SQL.

The system follows a batch-oriented processing model using MapReduce-style stages: parallel map operations ingest and normalize data from each platform, a shuffle phase groups records by artist and platform, and reduce operations compute aggregated statistics. Each pipeline run captures a timestamped snapshot, building a time-series dataset over repeated executions.

## Technology Stack

| Layer       | Technology                                |
| ----------- | ----------------------------------------- |
| Processing  | Apache Spark (PySpark), Spark SQL         |
| Storage     | Parquet (columnar, partitioned)           |
| Ingestion   | Python `requests`, `ThreadPoolExecutor`   |
| Data Format | JSON (raw), Parquet (processed)           |
| Config      | YAML, python-dotenv                       |

### Note on Implementation Changes

The Milestone 1 proposal specified HDFS for distributed storage. During development, the implementation was rebuilt from the ground up for Milestone 3 to use PySpark as the primary processing engine (replacing the pandas/Dask approach from M2). Parquet storage with platform and date partitioning follows the same distributed storage principles as HDFS (block-level partitioning, parallel access, fault-tolerant layout). The storage paths are configurable and can be pointed at an HDFS cluster by changing the path prefix in `config/settings.yaml`.

## Repository Structure

```
CS-4265-Project/
├── README.md                           # This file
├── .env.example                        # Template for API credentials
├── .gitignore
├── requirements.txt                    # Python dependencies
├── config/
│   └── settings.yaml                   # Pipeline and API configuration
├── data/
│   ├── raw/                            # Raw JSON API responses (timestamped)
│   └── processed/                      # Parquet time-series (partitioned)
├── hadoop/
│   └── bin/                            # Windows Hadoop binaries (not committed)
│       ├── winutils.exe
│       └── hadoop.dll
├── src/
│   ├── main.py                         # Pipeline orchestrator
│   ├── ingestion/
│   │   ├── spotify_ingest.py           # Spotify API ingestion
│   │   ├── lastfm_ingest.py            # Last.fm API ingestion
│   │   ├── deezer_ingest.py            # Deezer API ingestion
│   │   └── musicbrainz_ingest.py       # MusicBrainz API ingestion
│   ├── processing/
│   │   └── normalize.py                # Schema normalization (PySpark)
│   ├── storage/
│   │   └── parquet_store.py            # Parquet read/write operations
│   └── analysis/
│       └── queries.py                  # Spark SQL analytical queries
└── docs/
    └── data_dictionary.md              # Schema and field documentation
```

## Setup Instructions

### Prerequisites
- Python 3.9+ (tested with Python 3.14)
- Java 17 (required by PySpark 3.5+, download from https://adoptium.net/)

### 1. Clone the Repository

```bash
git clone https://github.com/ElijahGiusto/CS-4265-Project.git
cd CS-4265-Project
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure API Credentials

Create a `.env` file with your API keys:

```
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
LASTFM_API_KEY=your_lastfm_api_key
```

- **Spotify**: Register an app at https://developer.spotify.com/dashboard
- **Last.fm**: Create an API account at https://www.last.fm/api/account/create
- **Deezer**: No credentials required (public API)
- **MusicBrainz**: No credentials required (public API, uses User-Agent header)

### 4. Windows Setup (Hadoop Binaries)

PySpark on Windows requires Hadoop binaries for local file operations. Create the directory and download:

```powershell
mkdir hadoop\bin -Force
curl -o hadoop\bin\winutils.exe https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe
curl -o hadoop\bin\hadoop.dll https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll
```

Before each pipeline run, add `hadoop\bin` to your PATH:

```powershell
$env:PATH = "$PWD\hadoop\bin;$env:PATH"
```

This is not needed on Linux or macOS.

### 5. Run the Pipeline

```bash
# Full pipeline: ingest -> process -> store -> query
python src/main.py

# Query-only mode: skip ingestion, analyze existing data
python src/main.py --query-only
```

## Pipeline Stages

### Stage 1 — Data Ingestion (Map Phase)
Artist data is fetched from Spotify, Last.fm, Deezer, and MusicBrainz APIs using parallel HTTP requests via `ThreadPoolExecutor`. Each platform's ingestion job runs independently with retry logic, rate limiting, and error handling. Raw API responses are saved as timestamped JSON files.

### Stage 2 — Normalization (Shuffle + Reduce Phase)
Raw responses are transformed into PySpark DataFrames with a unified schema: `artist_name | artist_id | platform | snapshot_date | popularity_score | listeners | playcount | genres`. Platform-specific map functions extract relevant fields, then all records are unioned into a single DataFrame.

### Stage 3 — Parquet Storage
The normalized DataFrame is written to Parquet format, partitioned by `platform` and `snapshot_date`. This columnar layout enables efficient compression, parallel reads, and time-range queries. New pipeline runs append snapshots without overwriting historical data.

### Stage 4 — Analytical Queries (Query Layer)
Spark SQL queries are executed against the full historical dataset:
1. **Dataset Summary** — Record counts, platforms, snapshot coverage
2. **Cross-Platform Comparison** — Last.fm listeners, Deezer fans, and MusicBrainz ratings side by side
3. **Top Artists by Reach** — Ranked lists using window functions across Last.fm and Deezer
4. **MusicBrainz Community Ratings** — Artist ratings and genre tags from community votes
5. **Genre Analysis** — Genre distribution from Last.fm and MusicBrainz
6. **Platform Coverage** — Data completeness per artist across all platforms
7. **Artist Detail** — Single artist view across all platforms and snapshots

## Configuration

All pipeline settings are in `config/settings.yaml`:
- **API endpoints** for each platform
- **Pipeline parameters**: batch size, retry count, timeout
- **Storage paths**: raw and processed data directories
- **Artist list**: artists to track (currently 40 artists)

## Known Limitations

- **Spotify**: Client Credentials OAuth tokens restrict the search endpoint to returning artist name and ID only — popularity scores, follower counts, and genres are not included. Spotify contributes artist identification for cross-platform matching while Last.fm, Deezer, and MusicBrainz provide the popularity metrics.
- **Artist Name Inconsistencies**: Platforms format artist names differently (e.g., "Bjork" vs "Björk", "Tool" vs "TOOL"), which can cause deduplication gaps in cross-platform queries.
- **MusicBrainz Rate Limit**: MusicBrainz enforces a strict 1 request/second limit, so ingestion for 40 artists takes approximately 90 seconds.

## Status

This repository contains the complete Milestone 3 implementation. The pipeline runs end-to-end from API ingestion through Parquet storage to Spark SQL queries. The architecture was rebuilt from scratch for M3 to align with the original Milestone 1 proposal using PySpark.
