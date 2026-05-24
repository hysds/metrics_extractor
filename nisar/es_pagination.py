"""
Elasticsearch/OpenSearch pagination utility.

Provides reliable pagination beyond the 10,000-hit default limit.

Strategy (auto-detected):
  1. search_after + PIT (Elasticsearch API, then OpenSearch API)
  2. Scroll API fallback (universally supported)
"""

import copy
import json
import logging
from urllib.parse import urlsplit


def parse_es_search_url(search_url):
    """
    Parse an Elasticsearch search URL into base URL and index.

    Input:  "https://host/mozart_es/logstash-*/_search"
    Returns: ("https://host/mozart_es", "logstash-*")

    @param search_url: full ES search endpoint URL
    @return: tuple of (base_url, index)
    """
    # Strip /_search suffix
    url = search_url.rstrip("/")
    if url.endswith("/_search"):
        url = url[: -len("/_search")]

    # Split off the last path segment as the index
    last_slash = url.rfind("/")
    base_url = url[:last_slash]
    index = url[last_slash + 1 :]

    return base_url, index


def _open_pit(session, base_url, index, headers):
    """Open a PIT, trying Elasticsearch API first then OpenSearch.

    Returns (pit_id, backend) where backend is 'es' or 'opensearch'.
    Returns (None, None) if PIT is not supported.
    """
    # Try Elasticsearch: POST /{index}/_pit?keep_alive=1m
    pit_response = session.post(
        f"{base_url}/{index}/_pit?keep_alive=1m",
        headers=headers,
        verify=False,
    )
    if pit_response.status_code == 200:
        pit_id = pit_response.json()["id"]
        logging.info(f"Opened PIT (Elasticsearch): {pit_id[:40]}...")
        return pit_id, "es"

    logging.debug(
        f"ES PIT failed ({pit_response.status_code}), trying OpenSearch API"
    )

    # Try OpenSearch: POST /_search/point_in_time
    pit_response = session.post(
        f"{base_url}/_search/point_in_time",
        data=json.dumps({"index": index, "keep_alive": "1m"}),
        headers=headers,
        verify=False,
    )
    if pit_response.status_code == 200:
        pit_id = pit_response.json()["pit_id"]
        logging.info(f"Opened PIT (OpenSearch): {pit_id[:40]}...")
        return pit_id, "opensearch"

    logging.debug(
        f"OpenSearch PIT failed ({pit_response.status_code}), will use scroll API"
    )
    return None, None


def _close_pit(session, base_url, pit_id, backend, headers):
    """Close a PIT using the appropriate API for the backend."""
    try:
        if backend == "opensearch":
            close_response = session.delete(
                f"{base_url}/_search/point_in_time",
                data=json.dumps({"pit_id": [pit_id]}),
                headers=headers,
                verify=False,
            )
        else:
            close_response = session.delete(
                f"{base_url}/_pit",
                data=json.dumps({"id": pit_id}),
                headers=headers,
                verify=False,
            )
        if close_response.status_code == 200:
            logging.info("Closed PIT successfully")
        else:
            logging.warning(
                f"Failed to close PIT: {close_response.status_code} {close_response.reason}"
            )
    except Exception as e:
        logging.warning(f"Error closing PIT: {e}")


def _search_with_pit(session, base_url, query, pit_id, backend, headers, page_size):
    """Paginate using search_after + PIT."""
    query["pit"] = {"id": pit_id, "keep_alive": "1m"}
    if "sort" not in query:
        query["sort"] = [{"@timestamp": "asc"}, {"_id": "asc"}]
    query["size"] = page_size

    all_hits = []
    try:
        while True:
            response = session.post(
                f"{base_url}/_search",
                data=json.dumps(query),
                headers=headers,
                verify=False,
            )
            if response.status_code != 200:
                raise Exception(
                    f"Search query failed: {response.status_code} {response.reason}"
                )

            hits = response.json().get("hits", {}).get("hits", [])
            all_hits.extend(hits)
            logging.info(f"Fetched {len(all_hits)} hits so far (page returned {len(hits)})")

            if len(hits) < page_size:
                break
            query["search_after"] = hits[-1]["sort"]
    finally:
        _close_pit(session, base_url, pit_id, backend, headers)

    return all_hits


def _search_with_scroll(session, base_url, index, query, headers, page_size):
    """Paginate using the scroll API (universal fallback)."""
    logging.info("Using scroll API for pagination")
    query["size"] = page_size
    scroll_id = None

    all_hits = []
    try:
        # Initial search with scroll
        response = session.post(
            f"{base_url}/{index}/_search?scroll=1m",
            data=json.dumps(query),
            headers=headers,
            verify=False,
        )
        if response.status_code != 200:
            raise Exception(
                f"Search query failed: {response.status_code} {response.reason}"
            )

        result = response.json()
        scroll_id = result.get("_scroll_id")
        hits = result.get("hits", {}).get("hits", [])
        all_hits.extend(hits)
        logging.info(f"Fetched {len(all_hits)} hits so far (page returned {len(hits)})")

        # Continue scrolling
        while len(hits) == page_size:
            response = session.post(
                f"{base_url}/_search/scroll",
                data=json.dumps({"scroll": "1m", "scroll_id": scroll_id}),
                headers=headers,
                verify=False,
            )
            if response.status_code != 200:
                raise Exception(
                    f"Scroll query failed: {response.status_code} {response.reason}"
                )

            result = response.json()
            scroll_id = result.get("_scroll_id")
            hits = result.get("hits", {}).get("hits", [])
            all_hits.extend(hits)
            logging.info(f"Fetched {len(all_hits)} hits so far (page returned {len(hits)})")
    finally:
        if scroll_id:
            try:
                close_response = session.delete(
                    f"{base_url}/_search/scroll",
                    data=json.dumps({"scroll_id": [scroll_id]}),
                    headers=headers,
                    verify=False,
                )
                if close_response.status_code == 200:
                    logging.info("Cleared scroll successfully")
                else:
                    logging.warning(
                        f"Failed to clear scroll: {close_response.status_code} {close_response.reason}"
                    )
            except Exception as e:
                logging.warning(f"Error clearing scroll: {e}")

    return all_hits


def search_after_scan(session, api_url, query, page_size=10000):
    """
    Paginate through all results using the best available method.

    Tries search_after + PIT first (ES then OpenSearch API), falls back
    to scroll API if PIT is not supported. Deep-copies the query to
    avoid mutation.

    @param session: requests.Session with auth configured
    @param api_url: full search URL (e.g. https://host/mozart_es/logstash-*/_search)
    @param query: the query dict (will not be mutated)
    @param page_size: number of hits per page (default 10000)
    @return: flat list of all hit dicts
    """
    query = copy.deepcopy(query)
    base_url, index = parse_es_search_url(api_url)
    headers = {"Content-Type": "application/json"}

    pit_id, backend = _open_pit(session, base_url, index, headers)

    if pit_id:
        all_hits = _search_with_pit(session, base_url, query, pit_id, backend, headers, page_size)
    else:
        all_hits = _search_with_scroll(session, base_url, index, query, headers, page_size)

    logging.info(f"Total hits fetched: {len(all_hits)}")
    return all_hits
