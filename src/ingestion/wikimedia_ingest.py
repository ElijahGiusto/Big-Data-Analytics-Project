"""
Wikimedia Pageviews ingestion module.

Fetches public Wikipedia page-view counts for configured artists. This
adds a credential-free audience-attention signal that is independent of
streaming services and works well as a repeatable batch snapshot.
"""

import json
import logging
import time
import threading
from pathlib import Path
from datetime import datetime
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

logger = logging.getLogger(__name__)

_USER_AGENT = "ArtistPopularityPipeline/1.0 (CS4265 Big Data Project)"


class RateLimiter:
    """Enforces a minimum interval between API calls across threads."""

    def __init__(self, min_interval=0.2):
        self._min_interval = min_interval
        self._last_call = 0.0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            elapsed = time.time() - self._last_call
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_call = time.time()


_rate_limiter = RateLimiter(min_interval=0.2)


def _request_json(url, params, retries, delay, timeout):
    headers = {"User-Agent": _USER_AGENT, "Accept": "application/json"}
    for attempt in range(1, retries + 1):
        _rate_limiter.wait()
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code == 404:
                return None
            if resp.status_code == 429:
                wait_time = int(resp.headers.get("Retry-After", delay * attempt))
                logger.warning("Wikimedia rate limited. Waiting %d seconds", wait_time)
                time.sleep(wait_time)
                continue
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            logger.warning(
                "Wikimedia request error (attempt %d/%d): %s",
                attempt, retries, e,
            )
            time.sleep(delay * attempt)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status >= 500:
                logger.warning(
                    "Wikimedia server error %d (attempt %d/%d)",
                    status, attempt, retries,
                )
                time.sleep(delay * attempt)
                continue
            raise
    return None


def _add_month(year, month, delta):
    month_index = (year * 12 + (month - 1)) + delta
    return month_index // 12, month_index % 12 + 1


def _monthly_window(months_back):
    today = datetime.utcnow().date()
    latest_year, latest_month = _add_month(today.year, today.month, -1)
    start_year, start_month = _add_month(latest_year, latest_month, -(months_back - 1))
    start = f"{start_year:04d}{start_month:02d}0100"
    end = f"{latest_year:04d}{latest_month:02d}0100"
    return start, end


def configured_page_title(artist_name, config):
    wiki_config = config.get("wikimedia", {})
    overrides = wiki_config.get("page_title_overrides", {})
    if artist_name in overrides:
        return overrides[artist_name]
    return artist_name


def search_page_title(artist_name, config):
    wiki_config = config.get("wikimedia", {})

    retries = config["pipeline"]["max_retries"]
    delay = config["pipeline"]["retry_delay"]
    timeout = config["pipeline"]["request_timeout"]
    data = _request_json(
        wiki_config["search_url"],
        params={
            "action": "query",
            "list": "search",
            "srsearch": f"{artist_name} musician OR band",
            "srlimit": 1,
            "format": "json",
        },
        retries=retries,
        delay=delay,
        timeout=timeout,
    )
    results = (data or {}).get("query", {}).get("search", [])
    if results:
        return results[0]["title"]
    return None


def _fetch_pageviews(artist_name, title, config):
    wiki_config = config.get("wikimedia", {})
    months_back = wiki_config.get("months_back", 3)
    start, end = _monthly_window(months_back)
    encoded_title = quote(title.replace(" ", "_"), safe="")
    url = (
        f"{wiki_config['pageviews_url']}/per-article/"
        f"{wiki_config.get('project', 'en.wikipedia')}/"
        f"{wiki_config.get('access', 'all-access')}/"
        f"{wiki_config.get('agent', 'user')}/"
        f"{encoded_title}/monthly/{start}/{end}"
    )

    data = _request_json(
        url,
        params=None,
        retries=config["pipeline"]["max_retries"],
        delay=config["pipeline"]["retry_delay"],
        timeout=config["pipeline"]["request_timeout"],
    )
    if not data:
        return None

    items = data.get("items", [])
    total_views = sum(int(item.get("views", 0)) for item in items)
    latest_views = int(items[-1].get("views", 0)) if items else 0

    return {
        "source_artist_name": artist_name,
        "page_title": title,
        "period_start": start,
        "period_end": end,
        "latest_month_views": latest_views,
        "total_window_views": total_views,
        "months": items,
    }


def fetch_artist(artist_name, config):
    title = configured_page_title(artist_name, config)
    result = _fetch_pageviews(artist_name, title, config)
    if result:
        return result

    searched_title = search_page_title(artist_name, config)
    if searched_title and searched_title != title:
        result = _fetch_pageviews(artist_name, searched_title, config)
        if result:
            return result

    logger.warning("No pageview data for '%s'", artist_name)
    return None


def fetch_all_artists(config):
    artists = config["artists"]
    max_workers = min(config["pipeline"]["batch_size"], 4)
    results = []

    logger.info(
        "Starting Wikimedia ingestion for %d artists (concurrency: %d)",
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
                    logger.info("  [Wikimedia] Retrieved: %s", name)
                else:
                    logger.warning("  [Wikimedia] No data: %s", name)
            except Exception as e:
                logger.error("  [Wikimedia] Error for %s: %s", name, e)

    logger.info("Wikimedia ingestion complete: %d/%d artists", len(results), len(artists))
    return results


def save_raw(data, config):
    project_root = Path(__file__).resolve().parents[2]
    raw_path = project_root / config["storage"]["raw_path"]
    raw_path.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime("%Y%m%d")
    file_path = raw_path / f"wikimedia_{date_str}.json"

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    logger.info("Saved raw Wikimedia data -> %s (%d records)", file_path, len(data))
    return file_path
