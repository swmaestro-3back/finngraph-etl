"""본문 수집(fetch_article_body) 병렬 처리 테스트."""

from __future__ import annotations

import threading
import time

from pipelines.news.extractors import text_fetcher as tf


def _items(n: int) -> list[dict]:
    return [
        {"title": f"기사 {i}", "link": f"https://news.example.com/{i}", "originallink": ""}
        for i in range(n)
    ]


def test_fetch_article_body_runs_in_parallel_and_keeps_order(monkeypatch):
    monkeypatch.setenv("NEWS_BODY_FETCH_WORKERS", "4")
    tf.get_news_settings.cache_clear()

    active = 0
    peak = 0
    lock = threading.Lock()

    def fake_fetch(url: str) -> str:
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.05)
        with lock:
            active -= 1
        return f"본문 {url.rsplit('/', 1)[-1]} " + "가" * 100

    monkeypatch.setattr(tf, "fetch_article_body_from_url", fake_fetch)

    started = time.monotonic()
    result = tf.fetch_article_body(_items(8))
    elapsed = time.monotonic() - started

    assert [item["title"] for item in result] == [f"기사 {i}" for i in range(8)]
    assert all(item["_text"].startswith(f"본문 {i}") for i, item in enumerate(result))
    assert all(
        item["_body_source_url"] == f"https://news.example.com/{i}" for i, item in enumerate(result)
    )
    assert peak > 1
    assert elapsed < 8 * 0.05


def test_fetch_article_body_marks_failed_items_without_dropping(monkeypatch):
    monkeypatch.setenv("NEWS_BODY_FETCH_WORKERS", "2")
    tf.get_news_settings.cache_clear()

    def fake_fetch(url: str) -> str:
        return "" if url.endswith("/1") else "본문 " + "나" * 100

    monkeypatch.setattr(tf, "fetch_article_body_from_url", fake_fetch)

    result = tf.fetch_article_body(_items(3))

    assert len(result) == 3
    assert result[0]["_text"]
    assert result[1]["_text"] == ""
    assert result[1]["_body_source_url"] == ""
    assert result[2]["_text"]


def test_fetch_article_body_skips_fetch_when_text_present(monkeypatch):
    tf.get_news_settings.cache_clear()
    calls: list[str] = []

    def fake_fetch(url: str) -> str:
        calls.append(url)
        return "본문 " + "다" * 100

    monkeypatch.setattr(tf, "fetch_article_body_from_url", fake_fetch)

    items = _items(2)
    items[0]["_text"] = "이미 있는 본문 " + "라" * 100

    result = tf.fetch_article_body(items)

    assert calls == ["https://news.example.com/1"]
    assert result[0]["_text"].startswith("이미 있는 본문")
