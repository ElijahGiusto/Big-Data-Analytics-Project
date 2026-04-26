"""
MusicBrainz API Ingestion Module

Fetches artist metadata, user ratings, and genre tags from the
MusicBrainz open music database. MusicBrainz is a community-maintained
encyclopedia of music metadata with no authentication required — only
a descriptive User-Agent header is needed.

Rate Limiting Strategy:
    - MusicBrainz enforces a strict 1 request per second limit
    - Thread-safe rate limiter enforces 1.1s minimum between requests
    - Concurrency capped at 1 worker (sequential, rate-limit compliant)
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
# Thread-safe rate limiter (MusicBrainz: 1 req/sec strict)
# ---------------------------------------------------------------------------

class RateLimiter:
    """Enforces a minimum interval between API calls across threads."""

    def __init__(self, min_interval=1.1):
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


_rate_limiter = RateLimiter(min_interval=1.1)

# MusicBrainz requires a descriptive User-Agent
_USER_AGENT = "ArtistPopularityPipeline/1.0 (CS4265 Big Data Project)"


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

def _mb_request(url, params, retries, delay, timeout):
    """
    Make an HTTP GET request to MusicBrainz with rate limit awareness.

    MusicBrainz returns HTTP 503 when rate limited and expects clients
    to back off. The User-Agent header is mandatory.

    Returns:
        dict: Parsed JSON response.

    Raises:
        requests.RequestException: If all retries are exhausted.
    """
    headers = {"User-Agent": _USER_AGENT, "Accept": "application/json"}

    for attempt in range(1, retries + 1):
        _rate_limiter.wait()

        try:
            resp = requests.get(
                url, params=params, headers=headers, timeout=timeout
            )

            # MusicBrainz uses 503 for rate limiting
            if resp.status_code == 503:
                wait_time = 2 * attempt
                logger.warning(
                    "MusicBrainz rate limited (503). Waiting %d seconds "
                    "(attempt %d/%d)", wait_time, attempt, retries,
                )
                time.sleep(wait_time)
                continue

            resp.raise_for_status()
            return resp.json()

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            logger.warning(
                "MusicBrainz request error (attempt %d/%d): %s",
                attempt, retries, e,
            )
            time.sleep(delay * attempt)

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status >= 500:
                logger.warning(
                    "MusicBrainz server error %d (attempt %d/%d)",
                    status, attempt, retries,
                )
                time.sleep(delay * attempt)
                continue
            raise

    raise requests.exceptions.ConnectionError(
        f"MusicBrainz request failed after {retries} attempts"
    )


# ---------------------------------------------------------------------------
# Artist fetching
# ---------------------------------------------------------------------------

def fetch_artist(artist_name, config):
    """
    Fetch metadata for a single artist from the MusicBrainz API.

    Uses the /artist endpoint with a query search. Returns the best
    matching artist with their MusicBrainz ID, type, area, tags
    (genre equivalents with vote counts), and user rating.

    Args:
        artist_name: Name of the artist to search for.
        config: Pipeline configuration dict.

    Returns:
        dict: MusicBrainz artist object, or None on failure.
    """
    timeout = config["pipeline"]["request_timeout"]
    retries = config["pipeline"]["max_retries"]
    delay = config["pipeline"]["retry_delay"]
    base_url = config["musicbrainz"]["base_url"]

    try:
        # Search for the artist with tags and ratings included
        data = _mb_request(
            f"{base_url}/artist/",
            params={
                "query": f'artist:"{artist_name}"',
                "fmt": "json",
                "limit": 1,
            },
            retries=retries, delay=delay, timeout=timeout,
        )

        artists = data.get("artists", [])
        if not artists:
            logger.warning("No MusicBrainz results for '%s'", artist_name)
            return None

        artist = artists[0]

        # Fetch full artist details with tags and ratings
        artist_id = artist.get("id")
        if artist_id:
            try:
                detail = _mb_request(
                    f"{base_url}/artist/{artist_id}",
                    params={
                        "fmt": "json",
                        "inc": "tags+ratings",
                    },
                    retries=retries, delay=delay, timeout=timeout,
                )
                detail["source_artist_name"] = artist_name
                return detail
            except Exception:
                # Fall back to search result if detail fetch fails
                artist["source_artist_name"] = artist_name
                return artist

        artist["source_artist_name"] = artist_name
        return artist

    except Exception as e:
        logger.error(
            "Failed to fetch MusicBrainz data for '%s': %s", artist_name, e
        )
        return None


def fetch_all_artists(config):
    """
    Retrieve MusicBrainz data for all artists listed in configuration.

    MusicBrainz has a strict 1 req/sec limit, so this runs sequentially
    with rate limiting. Each artist can require 2 requests (search + detail),
    so larger artist lists intentionally take several minutes.

    Returns:
        list[dict]: List of MusicBrainz artist objects.
    """
    artists = config["artists"]
    results = []

    # MusicBrainz strict 1 req/sec means we run sequentially
    max_workers = 1

    logger.info(
        "Starting MusicBrainz ingestion for %d artists (concurrency: %d)",
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
                    logger.info("  [MusicBrainz] Retrieved: %s", name)
                else:
                    logger.warning("  [MusicBrainz] No data: %s", name)
            except Exception as e:
                logger.error("  [MusicBrainz] Error for %s: %s", name, e)

    logger.info(
        "MusicBrainz ingestion complete: %d/%d artists",
        len(results), len(artists),
    )
    return results


# ---------------------------------------------------------------------------
# Raw data persistence
# ---------------------------------------------------------------------------

def save_raw(data, config):
    """
    Save raw MusicBrainz API responses to disk as JSON.

    Files are timestamped to support accumulation of historical
    snapshots across multiple pipeline runs.
    """
    project_root = Path(__file__).resolve().parents[2]
    raw_path = project_root / config["storage"]["raw_path"]
    raw_path.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime("%Y%m%d")
    file_path = raw_path / f"musicbrainz_{date_str}.json"

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    logger.info(
        "Saved raw MusicBrainz data -> %s (%d records)", file_path, len(data)
    )
    return file_path
