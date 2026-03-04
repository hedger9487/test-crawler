from crawler.parser.html_parser import extract_links


def test_extract_links_resolves_relative_and_deduplicates():
    html = """
    <html><body>
      <a href="/about">About</a>
      <a href="https://example.com/about#team">About Fragment</a>
      <a href="https://example.com/about/">About Slash</a>
    </body></html>
    """
    links = extract_links(html, "https://example.com/home")
    assert links == ["https://example.com/about"]


def test_extract_links_skips_non_crawlable_schemes_and_fragments():
    html = """
    <a href="#section">Fragment</a>
    <a href="mailto:a@b.com">Mail</a>
    <a href="javascript:void(0)">JS</a>
    <a href="tel:123">Phone</a>
    <a href="https://example.com/ok">OK</a>
    """
    links = extract_links(html, "https://example.com")
    assert links == ["https://example.com/ok"]


def test_extract_links_returns_empty_for_empty_html():
    assert extract_links("", "https://example.com") == []
