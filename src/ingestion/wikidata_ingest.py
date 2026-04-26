"""
Wikidata ingestion module.

Fetches canonical Wikidata IDs and genre claims for configured artists.
This provides a public identifier bridge for cross-source matching.
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
    """Keep Wikidata requests polite."""

    def __init__(self, min_interval=0.5):
        self._min_interval = min_interval
        self._last_call = 0.0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            elapsed = time.time() - self._last_call
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_call = time.time()


_rate_limiter = RateLimiter(min_interval=0.5)


def _request_json(url, params, retries, delay, timeout):
    headers = {"User-Agent": _USER_AGENT, "Accept": "application/json"}
    for attempt in range(1, retries + 1):
        _rate_limiter.wait()
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code == 429:
                wait_time = int(resp.headers.get("Retry-After", delay * attempt))
                logger.warning("Wikidata rate limited. Waiting %d seconds", wait_time)
                time.sleep(wait_time + 1)
                continue
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            logger.warning(
                "Wikidata request error (attempt %d/%d): %s",
                attempt, retries, e,
            )
            time.sleep(delay * attempt)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status >= 500:
                logger.warning(
                    "Wikidata server error %d (attempt %d/%d)",
                    status, attempt, retries,
                )
                time.sleep(delay * attempt)
                continue
            raise
    return None


def _entity_claim_ids(entity, property_id):
    ids = []
    for claim in entity.get("claims", {}).get(property_id, []):
        value = (
            claim.get("mainsnak", {})
            .get("datavalue", {})
            .get("value", {})
            .get("id")
        )
        if value:
            ids.append(value)
    return ids


def _labels_for_ids(ids, config):
    if not ids:
        return []

    data = _request_json(
        config["wikidata"]["api_url"],
        params={
            "action": "wbgetentities",
            "ids": "|".join(ids[:10]),
            "props": "labels",
            "languages": "en",
            "format": "json",
        },
        retries=config["pipeline"]["max_retries"],
        delay=config["pipeline"]["retry_delay"],
        timeout=config["pipeline"]["request_timeout"],
    )
    entities = (data or {}).get("entities", {})
    labels = []
    for entity_id in ids:
        label = entities.get(entity_id, {}).get("labels", {}).get("en", {}).get("value")
        if label:
            labels.append(label)
    return labels


def fetch_artist(artist_name, config):
    data = _request_json(
        config["wikidata"]["api_url"],
        params={
            "action": "wbsearchentities",
            "search": artist_name,
            "language": "en",
            "type": "item",
            "limit": 1,
            "format": "json",
        },
        retries=config["pipeline"]["max_retries"],
        delay=config["pipeline"]["retry_delay"],
        timeout=config["pipeline"]["request_timeout"],
    )
    search_results = (data or {}).get("search", [])
    if not search_results:
        logger.warning("No Wikidata results for '%s'", artist_name)
        return None

    entity_id = search_results[0]["id"]
    entity_data = _request_json(
        config["wikidata"]["api_url"],
        params={
            "action": "wbgetentities",
            "ids": entity_id,
            "props": "labels|descriptions|claims",
            "languages": "en",
            "format": "json",
        },
        retries=config["pipeline"]["max_retries"],
        delay=config["pipeline"]["retry_delay"],
        timeout=config["pipeline"]["request_timeout"],
    )
    entity = (entity_data or {}).get("entities", {}).get(entity_id, {})
    genre_ids = _entity_claim_ids(entity, "P136")

    return {
        "source_artist_name": artist_name,
        "id": entity_id,
        "label": entity.get("labels", {}).get("en", {}).get("value"),
        "description": entity.get("descriptions", {}).get("en", {}).get("value"),
        "genre_ids": genre_ids,
        "genre_labels": _labels_for_ids(genre_ids, config),
    }


def fetch_all_artists(config):
    artists = config["artists"]
    max_workers = 1
    results = []

    logger.info(
        "Starting Wikidata ingestion for %d artists (concurrency: %d)",
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
                    logger.info("  [Wikidata] Retrieved: %s", name)
                else:
                    logger.warning("  [Wikidata] No data: %s", name)
            except Exception as e:
                logger.error("  [Wikidata] Error for %s: %s", name, e)

    logger.info("Wikidata ingestion complete: %d/%d artists", len(results), len(artists))
    return results


def save_raw(data, config):
    project_root = Path(__file__).resolve().parents[2]
    raw_path = project_root / config["storage"]["raw_path"]
    raw_path.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime("%Y%m%d")
    file_path = raw_path / f"wikidata_{date_str}.json"

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    logger.info("Saved raw Wikidata data -> %s (%d records)", file_path, len(data))
    return file_path
