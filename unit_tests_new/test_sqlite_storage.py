from pathlib import Path

import pytest

from crawler.storage.sqlite_storage import SQLiteStorage


@pytest.mark.asyncio
async def test_save_and_query_stats(tmp_path: Path):
    db_path = tmp_path / "crawl.db"
    storage = SQLiteStorage(db_path=str(db_path), batch_size=2)
    await storage.init()

    await storage.save_result(
        url="https://a.com/1",
        domain="a.com",
        status=200,
        depth=0,
        content_type="text/html",
    )
    await storage.save_result(
        url="https://b.com/1",
        domain="b.com",
        status=404,
        depth=1,
        error="not found",
    )

    assert await storage.count_discovered() == 2
    assert await storage.count_successful() == 1

    stats = await storage.get_stats()
    assert stats["total_crawled"] == 2
    assert stats["successful"] == 1
    assert stats["errors"] == 1
    assert stats["unique_domains"] == 2

    await storage.close()


@pytest.mark.asyncio
async def test_duplicate_url_is_ignored(tmp_path: Path):
    db_path = tmp_path / "crawl.db"
    storage = SQLiteStorage(db_path=str(db_path), batch_size=10)
    await storage.init()

    await storage.save_result(
        url="https://dup.com/1",
        domain="dup.com",
        status=200,
        depth=0,
    )
    await storage.save_result(
        url="https://dup.com/1",
        domain="dup.com",
        status=500,
        depth=0,
        error="later duplicate",
    )
    await storage.flush()

    assert await storage.count_discovered() == 1
    assert await storage.count_successful() == 1

    await storage.close()
