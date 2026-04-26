"""
iTunes Search API ingestion module.

The iTunes Search API is credential-free and provides catalog identifiers
and primary genre metadata for music artists.

Apple documents the Search API as approximately 20 calls per minute, so
this module intentionally runs sequentially with a ~3.1 second interval.
"""

import json
import logging
import time
import threading
from pathlib import Path
from datetime import datetime

import requests

logger = logging.getLogger(__name__)


class RateLimiter:
    """Keep public catalog requests polite."""

    def __init__(self, min_interval=3.1):
        self._min_interval = min_interval
        self._last_call = 0.0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            elapsed = time.time() - self._last_call
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_call = time.time()

    def set_interval(self, min_interval):
        with self._lock:
            self._min_interval = float(min_interval)


_rate_limiter = RateLimiter(min_interval=3.1)


def _request_json(url, params, retries, delay, timeout):
    headers = {"User-Agent": "ArtistPopularityPipeline/1.0 (CS4265 Big Data Project)"}
    for attempt in range(1, retries + 1):
        _rate_limiter.wait()
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code == 429:
                wait_time = int(resp.headers.get("Retry-After", 60))
                logger.warning(
                    "iTunes rate limited (429). Waiting %d seconds "
                    "(attempt %d/%d)",
                    wait_time, attempt, retries,
                )
                time.sleep(wait_time + 1)
                continue
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            logger.warning(
                "iTunes request error (attempt %d/%d): %s",
                attempt, retries, e,
            )
            time.sleep(delay * attempt)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status >= 500 or status == 429:
                logger.warning(
                    "iTunes HTTP error %d (attempt %d/%d)",
                    status, attempt, retries,
                )
                time.sleep(delay * attempt)
                continue
            raise
    return None


def _cache_path(config):
    project_root = Path(__file__).resolve().parents[2]
    configured_path = config.get("itunes", {}).get(
        "cache_path", "data/cache/itunes_artist_cache.json"
    )
    return project_root / configured_path


def _load_cache(config):
    path = _cache_path(config)
    if not path.exists():
        return {}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        logger.warning("Could not read iTunes cache at %s; starting fresh", path)
        return {}


def _save_cache(cache, config):
    path = _cache_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(cache, f, indent=2)


def fetch_artist(artist_name, config):
    data = _request_json(
        config["itunes"]["search_url"],
        params={
            "term": artist_name,
            "country": config.get("itunes", {}).get("country", "US"),
            "media": "music",
            "entity": "musicArtist",
            "attribute": "artistTerm",
            "limit": 1,
        },
        retries=config["pipeline"]["max_retries"],
        delay=config["pipeline"]["retry_delay"],
        timeout=config["pipeline"]["request_timeout"],
    )
    results = (data or {}).get("results", [])
    if not results:
        logger.warning("No iTunes results for '%s'", artist_name)
        return None

    artist = results[0]
    artist["source_artist_name"] = artist_name
    return artist


def fetch_all_artists(config):
    artists = config["artists"]
    results = []
    cache = _load_cache(config)
    _rate_limiter.set_interval(
        config.get("itunes", {}).get("min_interval_seconds", 3.1)
    )

    logger.info(
        "Starting iTunes ingestion for %d artists (sequential, cached)",
        len(artists),
    )
    for name in artists:
        cached = cache.get(name)
        if cached:
            results.append(cached)
            logger.info("  [iTunes] Cache hit: %s", name)
            continue

        try:
            result = fetch_artist(name, config)
            if result:
                results.append(result)
                cache[name] = result
                _save_cache(cache, config)
                logger.info("  [iTunes] Retrieved: %s", name)
            else:
                logger.warning("  [iTunes] No data: %s", name)
        except Exception as e:
            logger.error("  [iTunes] Error for %s: %s", name, e)

    logger.info("iTunes ingestion complete: %d/%d artists", len(results), len(artists))
    return results


def save_raw(data, config):
    project_root = Path(__file__).resolve().parents[2]
    raw_path = project_root / config["storage"]["raw_path"]
    raw_path.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime("%Y%m%d")
    file_path = raw_path / f"itunes_{date_str}.json"

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    logger.info("Saved raw iTunes data -> %s (%d records)", file_path, len(data))
    return file_path
