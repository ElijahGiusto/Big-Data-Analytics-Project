"""
TheAudioDB ingestion module.

TheAudioDB provides a free music metadata API. In this pipeline it
enriches each artist with genre/style and biographical catalog metadata.
"""

import json
import logging
import os
import time
import threading
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class RateLimiter:
    """TheAudioDB free tier is 30 requests/minute."""

    def __init__(self, min_interval=2.1):
        self._min_interval = min_interval
        self._last_call = 0.0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            elapsed = time.time() - self._last_call
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_call = time.time()


_rate_limiter = RateLimiter(min_interval=2.1)


def _request_json(url, params, retries, delay, timeout):
    for attempt in range(1, retries + 1):
        _rate_limiter.wait()
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                wait_time = int(resp.headers.get("Retry-After", 60))
                logger.warning("TheAudioDB rate limited. Waiting %d seconds", wait_time)
                time.sleep(wait_time)
                continue
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            logger.warning(
                "TheAudioDB request error (attempt %d/%d): %s",
                attempt, retries, e,
            )
            time.sleep(delay * attempt)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status >= 500:
                logger.warning(
                    "TheAudioDB server error %d (attempt %d/%d)",
                    status, attempt, retries,
                )
                time.sleep(delay * attempt)
                continue
            raise
    return None


def fetch_artist(artist_name, config):
    audio_config = config.get("theaudiodb", {})
    api_key = os.getenv("THEAUDIODB_API_KEY") or audio_config.get("api_key", "123")
    url = f"{audio_config['base_url']}/{api_key}/search.php"
    data = _request_json(
        url,
        params={"s": artist_name},
        retries=config["pipeline"]["max_retries"],
        delay=config["pipeline"]["retry_delay"],
        timeout=config["pipeline"]["request_timeout"],
    )
    artists = (data or {}).get("artists") or []
    if not artists:
        logger.warning("No TheAudioDB results for '%s'", artist_name)
        return None

    artist = artists[0]
    artist["source_artist_name"] = artist_name
    return artist


def fetch_all_artists(config):
    artists = config["artists"]
    max_workers = min(config["pipeline"]["batch_size"], 2)
    results = []

    logger.info(
        "Starting TheAudioDB ingestion for %d artists (concurrency: %d)",
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
                    logger.info("  [TheAudioDB] Retrieved: %s", name)
                else:
                    logger.warning("  [TheAudioDB] No data: %s", name)
            except Exception as e:
                logger.error("  [TheAudioDB] Error for %s: %s", name, e)

    logger.info("TheAudioDB ingestion complete: %d/%d artists", len(results), len(artists))
    return results


def save_raw(data, config):
    project_root = Path(__file__).resolve().parents[2]
    raw_path = project_root / config["storage"]["raw_path"]
    raw_path.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime("%Y%m%d")
    file_path = raw_path / f"theaudiodb_{date_str}.json"

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    logger.info("Saved raw TheAudioDB data -> %s (%d records)", file_path, len(data))
    return file_path
