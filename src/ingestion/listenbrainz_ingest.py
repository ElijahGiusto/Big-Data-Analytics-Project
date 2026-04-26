"""
ListenBrainz ingestion module.

Fetches public ListenBrainz artist listener statistics using MusicBrainz
artist MBIDs collected elsewhere in the pipeline.
"""

import json
import logging
import time
import threading
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

logger = logging.getLogger(__name__)

_USER_AGENT = "ArtistPopularityPipeline/1.0 (CS4265 Big Data Project)"


class RateLimiter:
    """Keep ListenBrainz public stats requests polite."""

    def __init__(self, min_interval=0.25):
        self._min_interval = min_interval
        self._last_call = 0.0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            elapsed = time.time() - self._last_call
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_call = time.time()


_rate_limiter = RateLimiter(min_interval=0.25)


def _request_json(url, params, retries, delay, timeout):
    headers = {"User-Agent": _USER_AGENT, "Accept": "application/json"}
    for attempt in range(1, retries + 1):
        _rate_limiter.wait()
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code in (204, 404):
                return None
            if resp.status_code == 429:
                wait_time = int(resp.headers.get("Retry-After", delay * attempt))
                logger.warning("ListenBrainz rate limited. Waiting %d seconds", wait_time)
                time.sleep(wait_time)
                continue
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            logger.warning(
                "ListenBrainz request error (attempt %d/%d): %s",
                attempt, retries, e,
            )
            time.sleep(delay * attempt)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status >= 500:
                logger.warning(
                    "ListenBrainz server error %d (attempt %d/%d)",
                    status, attempt, retries,
                )
                time.sleep(delay * attempt)
                continue
            raise
    return None


def _mbid_candidates(platform_data):
    candidates = {}

    for artist in platform_data.get("musicbrainz", []):
        name = artist.get("source_artist_name") or artist.get("name")
        mbid = artist.get("id")
        if name and mbid:
            candidates[name] = mbid

    for artist in platform_data.get("lastfm", []):
        name = artist.get("source_artist_name") or artist.get("name")
        mbid = artist.get("mbid")
        if name and mbid and name not in candidates:
            candidates[name] = mbid

    return candidates


def fetch_artist(artist_name, artist_mbid, config):
    lb_config = config.get("listenbrainz", {})
    url = f"{lb_config['base_url']}/1/stats/artist/{artist_mbid}/listeners"
    data = _request_json(
        url,
        params={
            "range": lb_config.get("range", "all_time"),
            "count": lb_config.get("top_listener_count", 25),
        },
        retries=config["pipeline"]["max_retries"],
        delay=config["pipeline"]["retry_delay"],
        timeout=config["pipeline"]["request_timeout"],
    )
    if not data:
        logger.warning("No ListenBrainz stats for '%s'", artist_name)
        return None

    payload = data.get("payload", {})
    payload["source_artist_name"] = artist_name
    payload["artist_mbid"] = payload.get("artist_mbid") or artist_mbid
    return payload


def fetch_all_artists(config, platform_data):
    mbids = _mbid_candidates(platform_data)
    artists = [name for name in config["artists"] if name in mbids]
    max_workers = min(config["pipeline"]["batch_size"], 4)
    results = []

    logger.info(
        "Starting ListenBrainz ingestion for %d artists with MBIDs (concurrency: %d)",
        len(artists), max_workers,
    )
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_name = {
            executor.submit(fetch_artist, name, mbids[name], config): name
            for name in artists
        }
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
                    logger.info("  [ListenBrainz] Retrieved: %s", name)
                else:
                    logger.warning("  [ListenBrainz] No data: %s", name)
            except Exception as e:
                logger.error("  [ListenBrainz] Error for %s: %s", name, e)

    logger.info("ListenBrainz ingestion complete: %d/%d artists", len(results), len(artists))
    return results


def save_raw(data, config):
    project_root = Path(__file__).resolve().parents[2]
    raw_path = project_root / config["storage"]["raw_path"]
    raw_path.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime("%Y%m%d")
    file_path = raw_path / f"listenbrainz_{date_str}.json"

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    logger.info("Saved raw ListenBrainz data -> %s (%d records)", file_path, len(data))
    return file_path
