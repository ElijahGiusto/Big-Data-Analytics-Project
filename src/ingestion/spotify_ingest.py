"""
Spotify API Ingestion Module

Fetches artist catalog metadata from the Spotify Web API.
Uses Client Credentials OAuth for authentication and ThreadPoolExecutor
for parallel API requests following the MapReduce ingestion pattern.

Spotify's 2026 development-mode responses may omit artist popularity,
followers, and genres. This source is therefore treated as a catalog ID
and URL source rather than a required popularity metric source.

Rate Limiting Strategy:
    - Thread-safe rate limiter enforces minimum delay between requests
    - HTTP 429 responses are caught and the Retry-After header is respected
    - Concurrency is capped at 3 workers to stay within Spotify's limits
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
# Thread-safe rate limiter
# ---------------------------------------------------------------------------

class RateLimiter:
    """
    Enforces a minimum interval between API calls across threads.

    Spotify's rate limits are not publicly documented with exact numbers,
    but testing shows that spacing requests ~300ms apart with max 3
    concurrent workers avoids 429 responses under normal load.
    """

    def __init__(self, min_interval=0.3):
        self._min_interval = min_interval
        self._last_call = 0.0
        self._lock = threading.Lock()

    def wait(self):
        """Block until enough time has passed since the last request."""
        with self._lock:
            now = time.time()
            elapsed = now - self._last_call
            if elapsed < self._min_interval:
                sleep_time = self._min_interval - elapsed
                time.sleep(sleep_time)
            self._last_call = time.time()


# Module-level rate limiter shared across threads
_rate_limiter = RateLimiter(min_interval=0.3)


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
# Rate-limited HTTP request helper
# ---------------------------------------------------------------------------

def _spotify_request(method, url, retries, delay, timeout, **kwargs):
    """
    Make an HTTP request to Spotify with rate limit awareness.

    Handles:
        - 429 Too Many Requests: waits for the Retry-After duration
        - 5xx Server Errors: retries with exponential backoff
        - Connection errors: retries with exponential backoff

    Args:
        method: "get" or "post"
        url: Full request URL.
        retries: Max number of retry attempts.
        delay: Base delay between retries (multiplied by attempt number).
        timeout: Request timeout in seconds.
        **kwargs: Additional arguments passed to requests.get/post.

    Returns:
        requests.Response: Successful response object.

    Raises:
        requests.RequestException: If all retries are exhausted.
    """
    for attempt in range(1, retries + 1):
        _rate_limiter.wait()

        try:
            if method == "post":
                resp = requests.post(url, timeout=timeout, **kwargs)
            else:
                resp = requests.get(url, timeout=timeout, **kwargs)

            # Handle rate limiting (429)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 5))
                logger.warning(
                    "Spotify rate limited (429). Waiting %d seconds "
                    "(attempt %d/%d)", retry_after, attempt, retries
                )
                time.sleep(retry_after + 1)  # +1 buffer
                continue

            resp.raise_for_status()
            return resp

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status >= 500:
                logger.warning(
                    "Spotify server error %d (attempt %d/%d): %s",
                    status, attempt, retries, e,
                )
                time.sleep(delay * attempt)
                continue
            raise  # 4xx errors (other than 429) are not retryable

        except requests.exceptions.ConnectionError as e:
            logger.warning(
                "Spotify connection error (attempt %d/%d): %s",
                attempt, retries, e,
            )
            time.sleep(delay * attempt)

        except requests.exceptions.Timeout as e:
            logger.warning(
                "Spotify request timeout (attempt %d/%d): %s",
                attempt, retries, e,
            )
            time.sleep(delay * attempt)

    raise requests.exceptions.ConnectionError(
        f"Spotify request failed after {retries} attempts: {url}"
    )


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def get_access_token(config):
    """
    Authenticate with Spotify using the Client Credentials OAuth flow.

    Returns:
        str: A valid Bearer access token.

    Raises:
        RuntimeError: If authentication fails after retries.
    """
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise EnvironmentError(
            "SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET must be set in .env"
        )

    retries = config["pipeline"]["max_retries"]
    delay = config["pipeline"]["retry_delay"]
    timeout = config["pipeline"]["request_timeout"]

    try:
        resp = _spotify_request(
            "post",
            config["spotify"]["auth_url"],
            retries=retries,
            delay=delay,
            timeout=timeout,
            data={"grant_type": "client_credentials"},
            auth=(client_id, client_secret),
        )
        token = resp.json()["access_token"]
        logger.info("Spotify authentication successful")
        return token
    except Exception as e:
        raise RuntimeError(f"Failed to authenticate with Spotify: {e}")


# ---------------------------------------------------------------------------
# Artist fetching
# ---------------------------------------------------------------------------

def fetch_artist(artist_name, token, config):
    """
    Fetch metadata for a single artist from the Spotify API.

    Uses the /search endpoint with type=artist to locate the artist,
    then follows the artist ID to /artists/{id}. Under current Spotify
    development-mode restrictions, the detail endpoint may still omit
    popularity, followers, and genres.

    Args:
        artist_name: Name of the artist to search for.
        token: Valid Spotify Bearer token.
        config: Pipeline configuration dict.

    Returns:
        dict: First matching artist object from search, or None.
    """
    headers = {"Authorization": f"Bearer {token}"}
    timeout = config["pipeline"]["request_timeout"]
    retries = config["pipeline"]["max_retries"]
    delay = config["pipeline"]["retry_delay"]
    base_url = config["spotify"]["base_url"]

    try:
        search_url = base_url + config["spotify"]["search_endpoint"]
        params = {"q": artist_name, "type": "artist", "limit": 1}

        search_resp = _spotify_request(
            "get", search_url,
            retries=retries, delay=delay, timeout=timeout,
            headers=headers, params=params,
        )
        items = search_resp.json().get("artists", {}).get("items", [])

        if items:
            artist = items[0]
            artist["source_artist_name"] = artist_name
            artist_id = artist.get("id")

            if not artist_id:
                return artist

            detail_url = (
                base_url
                + config["spotify"]["artist_endpoint"].rstrip("/")
                + f"/{artist_id}"
            )
            detail_resp = _spotify_request(
                "get", detail_url,
                retries=retries, delay=delay, timeout=timeout,
                headers=headers,
            )
            detail = detail_resp.json()
            detail["source_artist_name"] = artist_name
            return detail

        logger.warning("No Spotify results for '%s'", artist_name)
        return None

    except Exception as e:
        logger.error("Failed to fetch Spotify data for '%s': %s", artist_name, e)
        return None


def fetch_all_artists(config):
    """
    Retrieve Spotify data for all artists listed in configuration.

    Uses ThreadPoolExecutor for parallel API requests (map phase),
    then collects and filters results (reduce phase). Concurrency
    is capped at 3 to respect Spotify's rate limits.

    Returns:
        list[dict]: List of Spotify artist objects.
    """
    token = get_access_token(config)
    artists = config["artists"]
    results = []

    # Cap at 3 concurrent workers for Spotify to avoid rate limits.
    max_workers = min(config["pipeline"]["batch_size"], 3)

    logger.info(
        "Starting Spotify ingestion for %d artists (concurrency: %d)",
        len(artists), max_workers,
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_name = {
            executor.submit(fetch_artist, name, token, config): name
            for name in artists
        }

        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
                    logger.info("  [Spotify] Retrieved: %s", name)
                else:
                    logger.warning("  [Spotify] No data: %s", name)
            except Exception as e:
                logger.error("  [Spotify] Error for %s: %s", name, e)

    logger.info(
        "Spotify ingestion complete: %d/%d artists",
        len(results), len(artists),
    )
    return results


# ---------------------------------------------------------------------------
# Raw data persistence
# ---------------------------------------------------------------------------

def save_raw(data, config):
    """
    Save raw Spotify API responses to disk as JSON.

    Files are timestamped to support accumulation of historical
    snapshots across multiple pipeline runs.

    Args:
        data: List of raw Spotify artist objects.
        config: Pipeline configuration dict.
    """
    project_root = Path(__file__).resolve().parents[2]
    raw_path = project_root / config["storage"]["raw_path"]
    raw_path.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime("%Y%m%d")
    file_path = raw_path / f"spotify_{date_str}.json"

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    logger.info("Saved raw Spotify data -> %s (%d records)", file_path, len(data))
    return file_path
