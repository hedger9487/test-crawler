"""HTML link extractor.

Extracts and normalizes all <a href> links from HTML content.
"""

from __future__ import annotations

import logging
from urllib.parse import urljoin, urlparse

from selectolax.parser import HTMLParser as SelectolaxParser

from crawler.parser.url_normalizer import normalize_url

logger = logging.getLogger(__name__)

# Schemes we don't want to follow
_SKIP_SCHEMES = frozenset({
    "mailto", "tel", "javascript", "data", "ftp", "file",
    "magnet", "irc", "ssh", "git",
})


def extract_links(html: str, base_url: str) -> list[str]:
    """Extract and normalize all crawlable links from HTML.

    Args:
        html: Raw HTML string.
        base_url: The URL of the page (for resolving relative links).

    Returns:
        List of normalized, deduplicated absolute URLs.
    """
    if not html:
        return []

    try:
        tree = SelectolaxParser(html)
    except Exception as e:
        logger.debug("Parse error for %s: %s", base_url, e)
        return []

    seen: set[str] = set()
    results: list[str] = []

    for node in tree.css("a[href]"):
        href = node.attributes.get("href", "")
        if not href:
            continue

        href = href.strip()

        # Skip empty, fragments-only, and non-http schemes
        if not href or href.startswith("#"):
            continue

        # Quick check for bad schemes
        if ":" in href:
            scheme = href.split(":", 1)[0].lower()
            if scheme in _SKIP_SCHEMES:
                continue

        # Resolve relative URL
        try:
            absolute = urljoin(base_url, href)
        except Exception:
            continue

        # Normalize
        normalized = normalize_url(absolute)
        if normalized and normalized not in seen:
            seen.add(normalized)
            results.append(normalized)

    return results
