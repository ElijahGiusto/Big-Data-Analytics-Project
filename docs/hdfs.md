# HDFS Mode

The default pipeline writes processed Parquet to local `data/processed` so a reviewer can clone and run the project with minimal setup. HDFS mode uses the same pipeline and schema, but writes the processed Parquet lake to a real Apache Hadoop HDFS service.

## Start Hadoop

```bash
docker compose up -d --build
```

This starts a single-node HDFS service with:

- NameNode RPC: `hdfs://localhost:9000`
- NameNode UI: `http://localhost:9870`
- DataNode HTTP: `http://localhost:9864`

## Run The Pipeline Against HDFS

```bash
python src/main.py --storage hdfs
```

Query or validate existing HDFS data without new API calls:

```bash
python src/main.py --query-only --storage hdfs
python src/main.py --validate-only --storage hdfs
```

## Inspect HDFS

```bash
docker compose exec hdfs hdfs dfs -ls -R /artist-popularity/processed
```

The partition layout matches local mode:

```text
/artist-popularity/processed/
    platform=lastfm/
        snapshot_date=2026-04-26/
            part-*.parquet
```

## Stop Or Reset

Stop the service while keeping HDFS data:

```bash
docker compose down
```

Remove the HDFS volumes and start with an empty filesystem:

```bash
docker compose down -v
```

## Notes

- Docker Desktop must be running before `docker compose up`.
- HDFS mode is a single-node Hadoop deployment for local portfolio demonstration. The code uses HDFS URIs and Hadoop FileSystem APIs, so the storage target can be moved to a larger Hadoop cluster by changing `storage.hdfs_uri` and `storage.hdfs_processed_path` in `config/settings.yaml`.
- Raw API JSON files still write locally to `data/raw`; HDFS mode is for the processed analytical Parquet lake.
