"""
Deezer API Ingestion Module

Fetches artist metadata and fan counts from the Deezer API.
Deezer's API is publicly accessible with no authentication required,
making it an ideal third data source for cross-platform analysis.

Rate Limiting Strategy:
    - Deezer allows 50 requests per 5 seconds
    - Thread-safe rate limiter enforces 150ms minimum between requests
    - Concurrency capped at 5 workers
    - Exponential backoff on transient failures
"""

import json
import time
import logging
import threading
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import yaml

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Thread-safe rate limiter (Deezer: 50 req / 5 sec)
# ---------------------------------------------------------------------------

class RateLimiter:
    """Enforces a minimum interval between API calls across threads."""

    def __init__(self, min_interval=0.15):
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


_rate_limiter = RateLimiter(min_interval=0.15)


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

def _deezer_request(url, params, retries, delay, timeout):
    """
    Make an HTTP GET request to Deezer with rate limit awareness.

    Deezer returns a JSON error object with a "code" field on rate
    limit violations (code 4) rather than HTTP 429.

    Returns:
        dict: Parsed JSON response.

    Raises:
        requests.RequestException: If all retries are exhausted.
    """
    for attempt in range(1, retries + 1):
        _rate_limiter.wait()

        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()

            # Deezer uses in-body error codes for rate limits
            if "error" in data:
                error_code = data["error"].get("code", 0)
                error_msg = data["error"].get("message", "unknown")

                if error_code == 4:  # Quota exceeded
                    wait_time = 5 * attempt
                    logger.warning(
                        "Deezer rate limited (code 4). Waiting %d seconds "
                        "(attempt %d/%d)", wait_time, attempt, retries,
                    )
                    time.sleep(wait_time)
                    continue

                logger.warning("Deezer API error: %s (code %d)", error_msg, error_code)
                return data  # Return so caller can handle

            return data

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            logger.warning(
                "Deezer request error (attempt %d/%d): %s",
                attempt, retries, e,
            )
            time.sleep(delay * attempt)

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status >= 500:
                logger.warning(
                    "Deezer server error %d (attempt %d/%d)",
                    status, attempt, retries,
                )
                time.sleep(delay * attempt)
                continue
            raise

    raise requests.exceptions.ConnectionError(
        f"Deezer request failed after {retries} attempts"
    )


# ---------------------------------------------------------------------------
# Artist fetching
# ---------------------------------------------------------------------------

def fetch_artist(artist_name, config):
    """
    Fetch metadata for a single artist from the Deezer API.

    Uses the /search/artist endpoint which returns artist ID,
    name, number of fans, and album count. No authentication needed.

    Args:
        artist_name: Name of the artist to search for.
        config: Pipeline configuration dict.

    Returns:
        dict: Deezer artist object, or None on failure.
    """
    timeout = config["pipeline"]["request_timeout"]
    retries = config["pipeline"]["max_retries"]
    delay = config["pipeline"]["retry_delay"]
    base_url = config["deezer"]["base_url"]
    search_endpoint = config["deezer"]["search_endpoint"]

    try:
        data = _deezer_request(
            f"{base_url}{search_endpoint}",
            params={"q": artist_name},
            retries=retries, delay=delay, timeout=timeout,
        )

        results = data.get("data", [])
        if results:
            logger.debug("Fetched Deezer data for '%s'", artist_name)
            artist = results[0]
            artist["source_artist_name"] = artist_name
            return artist

        logger.warning("No Deezer results for '%s'", artist_name)
        return None

    except Exception as e:
        logger.error("Failed to fetch Deezer data for '%s': %s", artist_name, e)
        return None


def fetch_all_artists(config):
    """
    Retrieve Deezer data for all artists listed in configuration.

    Uses ThreadPoolExecutor for parallel API requests (map phase),
    then collects and filters results (reduce phase).

    Returns:
        list[dict]: List of Deezer artist objects.
    """
    artists = config["artists"]
    results = []

    # Deezer allows 50 req/5 sec, so 5 workers + 150ms limiter is safe
    max_workers = min(config["pipeline"]["batch_size"], 5)

    logger.info(
        "Starting Deezer ingestion for %d artists (concurrency: %d)",
        len(artists), max_workers,
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_name = {
            executor.submit(fetch_artist, name, config): name
            for name in artists
        }

        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
                    logger.info("  [Deezer] Retrieved: %s", name)
                else:
                    logger.warning("  [Deezer] No data: %s", name)
            except Exception as e:
                logger.error("  [Deezer] Error for %s: %s", name, e)

    logger.info(
        "Deezer ingestion complete: %d/%d artists", len(results), len(artists)
    )
    return results


# ---------------------------------------------------------------------------
# Raw data persistence
# ---------------------------------------------------------------------------

def save_raw(data, config):
    """
    Save raw Deezer API responses to disk as JSON.

    Files are timestamped to support accumulation of historical
    snapshots across multiple pipeline runs.
    """
    project_root = Path(__file__).resolve().parents[2]
    raw_path = project_root / config["storage"]["raw_path"]
    raw_path.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime("%Y%m%d")
    file_path = raw_path / f"deezer_{date_str}.json"

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    logger.info("Saved raw Deezer data -> %s (%d records)", file_path, len(data))
    return file_path
