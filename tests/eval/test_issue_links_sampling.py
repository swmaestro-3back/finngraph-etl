"""이슈 연결 평가 후보 생성·층화 추출·임베딩 캐시 단위 테스트. DB·Bedrock 은 부르지 않는다."""

from __future__ import annotations

import json
import random
from datetime import UTC, datetime, timedelta

import numpy as np
import pytest
from dump_candidates import (
    DAY_SECONDS,
    build_sample,
    no_company_pairs,
    sample_no_shared_pairs,
    shared_company_pairs,
    stratified_sample,
    window_pair_count,
)
from linkeval import (
    BAND_LABELS,
    NO_COMPANY_BAND_LABELS,
    NO_SHARED_BAND,
    EmbeddingCache,
    band_of,
    embed_with_cache,
    embedding_text,
    read_csv,
    read_jsonl,
)

# 정렬된 클러스터 6개: (시작일, 회사)
CLUSTERS = [
    (0, {1}),
    (1, {1, 2}),
    (1, {1}),  # 1번과 같은 시각 — 서로 '먼저'가 아니다
    (3, set()),
    (5, {2}),
    (20, {1}),  # lookback 10일 밖
]
TIMES = np.asarray([day * DAY_SECONDS for day, _ in CLUSTERS], dtype=np.float64)
COMPANIES = [companies for _, companies in CLUSTERS]
LOOKBACK = 10 * DAY_SECONDS
N = len(CLUSTERS)


def _pairs(codes) -> set[tuple[int, int]]:
    return {(int(code) // N, int(code) % N) for code in codes}


def test_band_of_boundaries():
    assert band_of(-0.2) == "0.0-0.4"
    assert band_of(0.39999) == "0.0-0.4"
    assert band_of(0.4) == "0.4-0.5"
    assert band_of(0.75) == "0.7-0.8"
    assert band_of(0.8) == "0.8-1.0"
    assert band_of(1.0000001) == "0.8-1.0"


def test_shared_company_pairs_require_strictly_earlier_within_lookback():
    codes = shared_company_pairs(TIMES, COMPANIES, LOOKBACK, since_ts=0.0)

    assert _pairs(codes) == {(0, 1), (0, 2), (1, 4)}
    assert list(codes) == sorted(codes)


def test_shared_company_pairs_since_filters_b_only():
    # B 는 2일 이후 시작만, A 는 그 전에 시작했어도 된다.
    codes = shared_company_pairs(TIMES, COMPANIES, LOOKBACK, since_ts=2 * DAY_SECONDS)

    assert _pairs(codes) == {(1, 4)}


def test_lookback_boundary_is_inclusive():
    times = np.asarray([0.0, 10 * DAY_SECONDS])
    companies = [{7}, {7}]

    assert len(shared_company_pairs(times, companies, 10 * DAY_SECONDS, 0.0)) == 1
    assert len(shared_company_pairs(times, companies, 9.99 * DAY_SECONDS, 0.0)) == 0


def test_no_shared_sample_excludes_shared_and_stays_in_window():
    shared = shared_company_pairs(TIMES, COMPANIES, LOOKBACK, 0.0)

    assert window_pair_count(TIMES, LOOKBACK, 0.0) == 9
    everything = sample_no_shared_pairs(TIMES, shared, LOOKBACK, 0.0, 100, random.Random(1))
    assert _pairs(everything) == {(0, 3), (1, 3), (2, 3), (0, 4), (2, 4), (3, 4)}

    first = sample_no_shared_pairs(TIMES, shared, LOOKBACK, 0.0, 3, random.Random(7))
    again = sample_no_shared_pairs(TIMES, shared, LOOKBACK, 0.0, 3, random.Random(7))
    assert len(first) == 3
    assert list(first) == list(again)
    assert _pairs(first) <= _pairs(everything)


def test_stratified_sample_caps_per_band_and_records_population():
    scores = np.concatenate([np.full(50, 0.2), np.full(20, 0.55), np.full(30, 0.9)])
    codes = np.arange(100, dtype=np.int64)

    sampled, populations = stratified_sample(codes, scores, 10, random.Random(3))

    assert populations == {
        "0.0-0.4": 50,
        "0.4-0.5": 0,
        "0.5-0.6": 20,
        "0.6-0.7": 0,
        "0.7-0.8": 0,
        "0.8-1.0": 30,
    }
    per_band = {band: [c for c, b, _ in sampled if b == band] for band in BAND_LABELS}
    assert {band: len(v) for band, v in per_band.items() if v} == {
        "0.0-0.4": 10,
        "0.5-0.6": 10,
        "0.8-1.0": 10,
    }
    assert all(0 <= c < 50 for c in per_band["0.0-0.4"])
    assert all(70 <= c < 100 for c in per_band["0.8-1.0"])

    again, _ = stratified_sample(codes, scores, 10, random.Random(3))
    assert again == sampled


def test_build_sample_assigns_band_population_to_each_pair():
    # 0·1·2 는 같은 방향(코사인 1), 4 는 직교(코사인 0) → (0,1)(0,2) 는 0.8-1.0, (1,4) 는 0.0-0.4
    embeddings = np.asarray(
        [[1, 0, 0], [1, 0, 0], [1, 0, 0], [0, 0, 1], [0, 1, 0], [1, 0, 0]], dtype=np.float32
    )

    pairs, stats = build_sample(
        TIMES,
        COMPANIES,
        embeddings,
        lookback_days=10,
        since_ts=0.0,
        per_band=1,
        per_band_no_company=1,
        no_shared=2,
        seed=5,
    )

    assert stats["shared_pairs"] == 3
    # 회사 없는 클러스터가 3 하나뿐이라 회사 없는 쌍은 없다
    assert stats["no_company_pairs"] == 0
    assert stats["band_population"]["0.8-1.0"] == 2
    assert stats["band_population"]["0.0-0.4"] == 1
    assert stats["band_population"][NO_SHARED_BAND] == 6
    assert stats["band_sampled"] == {
        "0.0-0.4": 1,
        "0.4-0.5": 0,
        "0.5-0.6": 0,
        "0.6-0.7": 0,
        "0.7-0.8": 0,
        "0.8-1.0": 1,
        NO_SHARED_BAND: 2,
        **dict.fromkeys(NO_COMPANY_BAND_LABELS, 0),
    }
    for pair in pairs:
        assert pair.band_population == stats["band_population"][pair.band]
        assert TIMES[pair.a] < TIMES[pair.b]
    high = next(p for p in pairs if p.band == "0.8-1.0")
    assert high.score == pytest.approx(1.0)
    assert (high.a, high.b) in {(0, 1), (0, 2)}


def test_no_company_pairs_form_their_own_stratified_population():
    # 0·2·3 은 회사 없음, 1 은 회사 있음. 3 은 2 보다 28일 뒤라 lookback 10일 밖
    times = np.asarray([0, 1, 2, 30], dtype=np.float64) * DAY_SECONDS
    companies = [set(), {1}, set(), set()]
    embeddings = np.asarray([[1, 0, 0], [0, 1, 0], [1, 0, 0], [0, 0, 1]], dtype=np.float32)

    assert {
        (int(c) // 4, int(c) % 4) for c in no_company_pairs(times, companies, LOOKBACK, 0.0)
    } == {(0, 2)}

    pairs, stats = build_sample(
        times,
        companies,
        embeddings,
        lookback_days=10,
        since_ts=0.0,
        per_band=5,
        per_band_no_company=5,
        no_shared=10,
        seed=1,
    )

    assert (stats["shared_pairs"], stats["no_company_pairs"]) == (0, 1)
    [no_company] = [p for p in pairs if p.band in NO_COMPANY_BAND_LABELS]
    assert (no_company.a, no_company.b, no_company.band) == (0, 2, "no_company:0.8-1.0")
    assert no_company.band_population == 1
    # 회사 없는 쌍은 운영 후보라 no_shared 에서 빠진다: (0,1)(1,2) 만 남는다
    assert stats["band_population"][NO_SHARED_BAND] == 2
    assert {(p.a, p.b) for p in pairs if p.band == NO_SHARED_BAND} == {(0, 1), (1, 2)}


def test_embedding_text_is_production_recipe():
    from pipelines.news.transformers.issue_linker import build_embedding_text

    # 평가와 운영이 같은 함수를 써야 고른 임계값이 운영 점수에 맞는다
    assert embedding_text is build_embedding_text
    assert embedding_text(" 미국 8월  건설지출 ", None, ["t1", "t2", "t3", "t4"]) == (
        "미국 8월 건설지출\nt1\nt2\nt3"
    )
    assert embedding_text("이름", "요약 문장이에요.", ["t1"]) == "이름\n요약 문장이에요.\nt1"
    assert embedding_text("이름", "   ", []) == "이름"


def test_embedding_cache_skips_cached_and_reembeds_changed_text(tmp_path):
    calls: list[list[str]] = []

    def fake_embed(texts: list[str]) -> list[list[float]]:
        calls.append(texts)
        return [[float(len(text)), 1.0, 0.0] for text in texts]

    path = tmp_path / "cache.jsonl"
    items = [(1, "가"), (2, "나나"), (3, "다다다")]

    first, embedded = embed_with_cache(items, EmbeddingCache(path), fake_embed, "m", dim=3)
    assert embedded == 3
    assert np.allclose(np.linalg.norm(first, axis=1), 1.0)

    second, embedded = embed_with_cache(items, EmbeddingCache(path), fake_embed, "m", dim=3)
    assert embedded == 0
    assert np.allclose(first, second)

    changed = [(1, "가"), (2, "나나 요약 추가"), (3, "다다다")]
    _, embedded = embed_with_cache(changed, EmbeddingCache(path), fake_embed, "m", dim=3)
    assert embedded == 1
    assert calls[-1] == ["나나 요약 추가"]

    # 모델이 바뀌면 키가 달라져 전부 다시 임베딩한다.
    _, embedded = embed_with_cache(items, EmbeddingCache(path), fake_embed, "other", dim=3)
    assert embedded == 3


def test_embedding_cache_ignores_truncated_line(tmp_path):
    path = tmp_path / "cache.jsonl"
    embed_with_cache([(1, "가")], EmbeddingCache(path), lambda t: [[1.0, 0.0]] * len(t), "m", 2)
    with path.open("a", encoding="utf-8") as fh:
        fh.write('{"key": "2:abc", "vec')

    assert len(EmbeddingCache(path)) == 1


def test_dump_main_writes_pairs_with_stubbed_db_and_bedrock(tmp_path, monkeypatch, request):
    import dump_candidates

    from pipelines.common.clients import bedrock
    from pipelines.news import config

    now = datetime.now(UTC)
    clusters = [
        dump_candidates.Cluster(
            id=10 + i,
            title=f"이슈 {i}",
            summary="요약이에요." if i % 2 else None,
            first_published_at=now - timedelta(days=day),
            last_published_at=now - timedelta(days=day),
            original_size=3,
            lead="본문 리드",
            member_titles=[f"기사 {i}-1", f"기사 {i}-2"],
            companies=companies,
        )
        for i, (day, companies) in enumerate(
            [(40, {1: "마이크론"}), (30, {1: "마이크론", 2: "SK하이닉스"}), (2, {1: "마이크론"})]
        )
    ]
    monkeypatch.setattr(dump_candidates, "load_clusters", lambda start, lead: (clusters, True))
    # 운영 이슈 연결과 같은 뉴스 전용 모델을 써야 한다(테마용 BEDROCK_EMBEDDING_MODEL 이 아니라)
    monkeypatch.setenv("NEWS_ISSUE_EMBEDDING_MODEL", "news-titan-test")
    monkeypatch.setenv("BEDROCK_EMBEDDING_MODEL", "theme-titan-test")
    config.get_news_settings.cache_clear()
    request.addfinalizer(config.get_news_settings.cache_clear)
    embedded: list[str] = []
    models: set[str | None] = set()

    def fake_embed(texts, dim, model=None):
        embedded.extend(texts)
        models.add(model)
        return [[1.0] + [0.0] * (dim - 1) for _ in texts]

    monkeypatch.setattr(bedrock, "embed_texts", fake_embed)

    dump_candidates.main(["--since-days", "60", "--lookback-days", "90", "--out", str(tmp_path)])

    records = read_jsonl(tmp_path / "pairs.jsonl")
    assert {r["pair_id"] for r in records} == {"10_11", "10_12", "11_12"}
    first = next(r for r in records if r["pair_id"] == "10_11")
    assert first["band"] == "0.8-1.0"
    assert first["band_population"] == 3
    assert first["shared_companies"] == ["마이크론"]
    assert first["a_member_titles"] == ["기사 0-1", "기사 0-2"]
    assert "이슈 1\n요약이에요.\n기사 1-1\n기사 1-2" in embedded
    assert len(read_csv(tmp_path / "pairs.csv")) == 3
    assert (tmp_path / ".gitignore").read_text() == "*\n"
    meta = json.loads((tmp_path / "meta.json").read_text(encoding="utf-8"))
    assert meta["band_population"][NO_SHARED_BAND] == 0
    assert models == {"news-titan-test"}
    assert meta["embedding_model"] == "news-titan-test"

    # 재실행은 캐시만 쓴다.
    embedded.clear()
    dump_candidates.main(["--since-days", "60", "--lookback-days", "90", "--out", str(tmp_path)])
    assert embedded == []
