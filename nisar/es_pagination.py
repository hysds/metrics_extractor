"""
Elasticsearch search_after + PIT (Point In Time) pagination utility.

Provides reliable pagination beyond the 10,000-hit default limit by using
the search_after API with a Point In Time snapshot for consistent results.
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


def search_after_scan(session, api_url, query, page_size=10000):
    """
    Paginate through all Elasticsearch results using search_after + PIT.

    Deep-copies the query to avoid mutation. Opens a PIT, pages through
    all results using search_after, and closes the PIT in a finally block.

    @param session: requests.Session with auth configured
    @param api_url: full ES search URL (e.g. https://host/mozart_es/logstash-*/_search)
    @param query: the ES query dict (will not be mutated)
    @param page_size: number of hits per page (default 10000)
    @return: flat list of all hit dicts
    """
    query = copy.deepcopy(query)
    base_url, index = parse_es_search_url(api_url)
    headers = {"Content-Type": "application/json"}

    # Open PIT
    pit_response = session.post(
        f"{base_url}/{index}/_pit?keep_alive=1m",
        headers=headers,
        verify=False,
    )
    if pit_response.status_code != 200:
        raise Exception(
            f"Failed to open PIT: {pit_response.status_code} {pit_response.reason}"
        )

    pit_id = pit_response.json()["id"]
    logging.info(f"Opened PIT: {pit_id[:40]}...")

    try:
        # Add PIT to query
        query["pit"] = {"id": pit_id, "keep_alive": "1m"}

        # Add sort if not present
        if "sort" not in query:
            query["sort"] = [{"@timestamp": "asc"}, {"_id": "asc"}]

        # Set page size
        query["size"] = page_size

        all_hits = []

        while True:
            response = session.post(
                f"{base_url}/_search",
                data=json.dumps(query),
                headers=headers,
                verify=False,
            )

            if response.status_code != 200:
                raise Exception(
                    f"ES query failed: {response.status_code} {response.reason}"
                )

            result = response.json()
            hits = result.get("hits", {}).get("hits", [])
            all_hits.extend(hits)

            logging.info(f"Fetched {len(all_hits)} hits so far (page returned {len(hits)})")

            if len(hits) < page_size:
                break

            # Set search_after from last hit's sort values
            query["search_after"] = hits[-1]["sort"]

    finally:
        # Close PIT
        try:
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

    logging.info(f"Total hits fetched: {len(all_hits)}")
    return all_hits
