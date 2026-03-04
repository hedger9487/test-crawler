"""URL normalization and canonicalization.

Ensures that semantically identical URLs have the same string representation
to avoid duplicate crawls.
"""

from __future__ import annotations

from urllib.parse import (
    urlparse,
    urlunparse,
    parse_qs,
    urlencode,
    unquote,
    quote,
)
import re

# Common tracking / session parameters to strip
_STRIP_PARAMS = frozenset({
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "utm_cid",
    "fbclid", "gclid", "gclsrc", "dclid", "msclkid",
    "ref", "referer", "referrer",
    "sessionid", "session_id", "sid",
    "jsessionid",
    "phpsessid",
    "_ga", "_gl", "_hsenc", "_hsmi",
    "mc_cid", "mc_eid",
})

# Default ports per scheme
_DEFAULT_PORTS = {"http": 80, "https": 443}


def normalize_url(url: str, *, strip_tracking: bool = True) -> str | None:
    """Return a canonicalized URL string, or None if the URL is not crawlable.

    Canonicalization steps:
      1. Lowercase scheme and host
      2. Remove default ports (80 for http, 443 for https)
      3. Remove fragment (#...)
      4. Decode unreserved percent-encoded characters
      5. Remove dot segments in path (/../, /./)
      6. Remove trailing slash (except root /)
      7. Sort query parameters
      8. Strip common tracking parameters
    """
    if not url or not isinstance(url, str):
        return None

    url = url.strip()

    # Quick reject non-HTTP(S) — case-insensitive per RFC 3986
    url_lower = url.lower()
    if not url_lower.startswith(("http://", "https://", "//")):
        return None

    # Handle protocol-relative URLs
    if url.startswith("//"):
        url = "https:" + url

    try:
        parsed = urlparse(url)
    except Exception:
        return None

    scheme = parsed.scheme.lower()
    if scheme not in ("http", "https"):
        return None

    host = parsed.hostname
    if not host:
        return None
    host = host.lower().rstrip(".")

    # Remove default port
    port = parsed.port
    if port and port == _DEFAULT_PORTS.get(scheme):
        port = None
    netloc = host if not port else f"{host}:{port}"

    # Normalize path: resolve dot segments, decode unreserved
    path = parsed.path or "/"
    path = _normalize_path(path)

    # Remove trailing slash (unless root)
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    # Sort and filter query params
    query = ""
    if parsed.query:
        params = parse_qs(parsed.query, keep_blank_values=True)
        if strip_tracking:
            params = {
                k: v for k, v in params.items()
                if k.lower() not in _STRIP_PARAMS
            }
        if params:
            # Sort by key, then produce stable query string
            sorted_params = sorted(params.items())
            parts = []
            for k, values in sorted_params:
                for v in sorted(values):
                    parts.append((k, v))
            query = urlencode(parts)

    # No fragment
    return urlunparse((scheme, netloc, path, "", query, ""))


def get_domain(url: str) -> str | None:
    """Extract the domain (host) from a URL."""
    try:
        parsed = urlparse(url)
        host = parsed.hostname
        if host:
            return host.lower().rstrip(".")
    except Exception:
        pass
    return None


# ---- internal helpers ----

# Unreserved characters per RFC 3986
_UNRESERVED = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
)

_PCT_RE = re.compile(r"%([0-9A-Fa-f]{2})")


def _decode_unreserved(match: re.Match) -> str:
    """Decode percent-encoded char if it's unreserved."""
    char = chr(int(match.group(1), 16))
    return char if char in _UNRESERVED else match.group(0).upper()


def _normalize_path(path: str) -> str:
    """Resolve dot segments and normalize percent encoding in path."""
    # Decode unreserved characters
    path = _PCT_RE.sub(_decode_unreserved, path)

    # Resolve . and ..
    segments = path.split("/")
    resolved: list[str] = []
    for seg in segments:
        if seg == ".":
            continue
        elif seg == "..":
            if resolved and resolved[-1] != "":
                resolved.pop()
        else:
            resolved.append(seg)

    result = "/".join(resolved)
    if not result.startswith("/"):
        result = "/" + result
    return result
