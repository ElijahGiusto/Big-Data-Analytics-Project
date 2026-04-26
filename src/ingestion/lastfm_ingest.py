"""
Last.fm API Ingestion Module

Fetches artist metadata and listener/playcount metrics from the Last.fm API.
Uses API key authentication and ThreadPoolExecutor for parallel requests.

Rate Limiting Strategy:
    - Last.fm allows ~5 requests/second for standard API keys
    - Thread-safe rate limiter enforces 250ms minimum between requests
    - Concurrency capped at 4 workers
    - Exponential backoff on transient failures
"""

import os
import json
import time
import logging
import threading
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import yaml
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Thread-safe rate limiter (Last.fm: ~5 req/sec)
# ---------------------------------------------------------------------------

class RateLimiter:
    """Enforces a minimum interval between API calls across threads."""

    def __init__(self, min_interval=0.25):
        self._min_interval = min_interval
        self._last_call = 0.0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            now = time.time()
            elapsed = now - self._last_call
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_call = time.time()


_rate_limiter = RateLimiter(min_interval=0.25)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def load_config():
    """Load pipeline configuration from settings.yaml."""
    project_root = Path(__file__).resolve().parents[2]
    config_path = project_root / "config" / "settings.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Rate-limited request helper
# ---------------------------------------------------------------------------

def _lastfm_request(url, params, retries, delay, timeout):
    """
    Make an HTTP GET request to Last.fm with rate limit awareness.

    Handles connection errors, timeouts, and server errors with
    exponential backoff. Last.fm returns errors inside the JSON
    body rather than using HTTP status codes in most cases.

    Returns:
        dict: Parsed JSON response.

    Raises:
        requests.RequestException: If all retries are exhausted.
    """
    for attempt in range(1, retries + 1):
        _rate_limiter.wait()

        try:
            resp = requests.get(url, params=params, timeout=timeout)

            # Last.fm sometimes returns 429 under heavy load
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 5))
                logger.warning(
                    "Last.fm rate limited (429). Waiting %d seconds "
                    "(attempt %d/%d)", retry_after, attempt, retries,
                )
                time.sleep(retry_after + 1)
                continue

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status >= 500:
                logger.warning(
                    "Last.fm server error %d (attempt %d/%d)",
                    status, attempt, retries,
                )
                time.sleep(delay * attempt)
                continue
            raise

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            logger.warning(
                "Last.fm request error (attempt %d/%d): %s",
                attempt, retries, e,
            )
            time.sleep(delay * attempt)

    raise requests.exceptions.ConnectionError(
        f"Last.fm request failed after {retries} attempts"
    )


# ---------------------------------------------------------------------------
# Artist fetching
# ---------------------------------------------------------------------------

def fetch_artist(artist_name, api_key, config):
    """
    Fetch metadata for a single artist from the Last.fm API.

    Uses the artist.getinfo method which returns listener count,
    playcount, tags, and biographical data.

    Args:
        artist_name: Name of the artist to look up.
        api_key: Last.fm API key.
        config: Pipeline configuration dict.

    Returns:
        dict: Artist info object from Last.fm, or None on failure.
    """
    timeout = config["pipeline"]["request_timeout"]
    retries = config["pipeline"]["max_retries"]
    delay = config["pipeline"]["retry_delay"]

    params = {
        "method": "artist.getinfo",
        "artist": artist_name,
        "api_key": api_key,
        "format": "json",
    }

    try:
        data = _lastfm_request(
            config["lastfm"]["base_url"], params,
            retries=retries, delay=delay, timeout=timeout,
        )

        if "artist" in data:
            logger.debug("Fetched Last.fm data for '%s'", artist_name)
            artist = data["artist"]
            artist["source_artist_name"] = artist_name
            return artist

        # Last.fm returns errors in the JSON body
        error_msg = data.get("message", "unknown error")
        logger.warning(
            "Last.fm returned no artist data for '%s': %s",
            artist_name, error_msg,
        )
        return None

    except Exception as e:
        logger.error("Failed to fetch Last.fm data for '%s': %s", artist_name, e)
        return None


def fetch_all_artists(config):
    """
    Retrieve Last.fm data for all artists listed in configuration.

    Uses ThreadPoolExecutor for parallel API requests (map phase),
    then collects and filters results (reduce phase).

    Returns:
        list[dict]: List of Last.fm artist info objects.
    """
    api_key = os.getenv("LASTFM_API_KEY")

    if not api_key:
        raise EnvironmentError("LASTFM_API_KEY must be set in .env")

    artists = config["artists"]
    results = []

    # Last.fm allows ~5 req/sec, so 4 workers + 250ms limiter is safe
    max_workers = min(config["pipeline"]["batch_size"], 4)

    logger.info(
        "Starting Last.fm ingestion for %d artists (concurrency: %d)",
        len(artists), max_workers,
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_name = {
            executor.submit(fetch_artist, name, api_key, config): name
            for name in artists
        }

        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
                    logger.info("  [Last.fm] Retrieved: %s", name)
                else:
                    logger.warning("  [Last.fm] No data: %s", name)
            except Exception as e:
                logger.error("  [Last.fm] Error for %s: %s", name, e)

    logger.info(
        "Last.fm ingestion complete: %d/%d artists", len(results), len(artists)
    )
    return results


# ---------------------------------------------------------------------------
# Raw data persistence
# ---------------------------------------------------------------------------

def save_raw(data, config):
    """
    Save raw Last.fm API responses to disk as JSON.

    Files are timestamped to support accumulation of historical
    snapshots across multiple pipeline runs.
    """
    project_root = Path(__file__).resolve().parents[2]
    raw_path = project_root / config["storage"]["raw_path"]
    raw_path.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime("%Y%m%d")
    file_path = raw_path / f"lastfm_{date_str}.json"

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    logger.info("Saved raw Last.fm data -> %s (%d records)", file_path, len(data))
    return file_path
