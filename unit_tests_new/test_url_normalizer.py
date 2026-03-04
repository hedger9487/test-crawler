from crawler.parser.url_normalizer import get_domain, normalize_url


def test_normalize_url_canonicalizes_scheme_host_port_path_and_fragment():
    url = "HTTP://Example.COM:80/a/./b/../c/%7Euser/?b=2&a=1#frag"
    assert normalize_url(url) == "http://example.com/a/c/~user?a=1&b=2"


def test_normalize_url_strips_tracking_params_and_sorts_values():
    url = "https://example.com/path?utm_source=x&b=2&a=3&a=1&ref=abc"
    assert normalize_url(url) == "https://example.com/path?a=1&a=3&b=2"


def test_normalize_url_rejects_non_http_and_invalid_inputs():
    assert normalize_url("mailto:test@example.com") is None
    assert normalize_url("ftp://example.com/file") is None
    assert normalize_url("not a url") is None
    assert normalize_url("") is None


def test_normalize_url_supports_protocol_relative_urls():
    assert normalize_url("//Example.com/path/") == "https://example.com/path"


def test_get_domain_normalizes_case_and_trailing_dot():
    assert get_domain("https://Sub.Example.com./x") == "sub.example.com"
    assert get_domain("invalid-url") is None
