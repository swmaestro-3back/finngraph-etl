"""이슈 연결 임계값 평가용 클러스터 쌍을 뽑는다 (읽기 전용).

이름이 붙은 클러스터를 Titan V2 로 임베딩하고, 운영 연결 규칙과 같은 후보(B 보다 먼저 시작,
lookback 일 이내)를 두 모집단으로 만든다 — 회사를 1개 이상 공유하는 쌍, 둘 다 회사가 없는 쌍.
운영은 둘을 다른 임계값으로 잇으므로 각각 코사인 점수 밴드별로 층화 추출한다. 밴드마다 모집단
크기를 같이 적어 score.py 가 표본을 모집단 기준으로 되돌려 채점할 수 있게 한다. 운영 후보가 아닌
나머지 쌍은 소량만 무작위로 뽑아 별도 층(no_shared)으로 둔다.

DB 에는 SELECT 만 하고 트랜잭션을 READ ONLY 로 연다. 임베딩은 클러스터 id + 입력 텍스트
해시로 디스크에 캐시해 재실행 시 Bedrock 을 다시 부르지 않는다.

실행: .venv/bin/python scripts/eval/issue_links/dump_candidates.py --since-days 90
"""

from __future__ import annotations

import argparse
import json
import logging
import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
from linkeval import (
    BAND_EDGES,
    BAND_LABELS,
    DEFAULT_OUT_DIR,
    EMBEDDING_DIM,
    MEMBER_TITLE_COUNT,
    NO_COMPANY_BAND_LABELS,
    NO_SHARED_BAND,
    EmbeddingCache,
    collapse,
    embed_with_cache,
    embedding_text,
    protect_output_dir,
    write_csv,
    write_jsonl,
)

log = logging.getLogger("issue_links.dump")

SEOUL = ZoneInfo("Asia/Seoul")
DAY_SECONDS = 86400.0
# 대표 기사 본문은 리드만 쓰므로 DB 에서 이만큼만 잘라 온다.
LEAD_FETCH_CHARS = 2000
SCORE_CHUNK = 16384

# news_clusters.summary 는 이슈 타임라인 마이그레이션에서 생긴다. 적용 전 DB 에서도 돌도록
# 컬럼이 있을 때만 읽는다.
SUMMARY_COLUMN_EXISTS_SQL = """
    SELECT EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = ANY (current_schemas(false))
           AND table_name = 'news_clusters'
           AND column_name = 'summary'
    )
"""

# {summary} 는 코드 상수("nc.summary" 또는 "NULL::text")로만 채운다.
SELECT_CLUSTERS_SQL = """
    SELECT nc.id,
           nc.title,
           {summary} AS summary,
           nc.first_published_at,
           nc.last_published_at,
           nc.original_size,
           substr(rn.text, 1, :lead_fetch_chars) AS lead
      FROM news_clusters nc
      LEFT JOIN news rn ON rn.id = nc.representative_news_id
     WHERE nc.title IS NOT NULL
       AND nc.first_published_at >= :load_start
     ORDER BY nc.first_published_at, nc.id
"""

# 클러스터별 최신 멤버 기사 제목. 빈 제목을 순위 매기기 전에 빼는 것까지 운영 연결과 같게 둔다.
SELECT_MEMBER_TITLES_SQL = """
    SELECT cluster_id, title
      FROM (
            SELECT n.cluster_id,
                   n.title,
                   ROW_NUMBER() OVER (
                       PARTITION BY n.cluster_id
                       ORDER BY n.published_at DESC NULLS LAST, n.id DESC
                   ) AS rn
              FROM news n
             WHERE n.cluster_id = ANY (:cluster_ids)
               AND n.title IS NOT NULL
               AND BTRIM(n.title) <> ''
           ) ranked
     WHERE rn <= :limit
     ORDER BY cluster_id, rn
"""

# 클러스터의 회사 = 멤버 기사에 연결된 회사의 합집합
SELECT_CLUSTER_COMPANIES_SQL = """
    SELECT DISTINCT n.cluster_id, c.id, c.name
      FROM news n
      JOIN news_companies ncm ON ncm.news_id = n.id
      JOIN companies c ON c.id = ncm.company_id
     WHERE n.cluster_id = ANY (:cluster_ids)
"""


@dataclass
class Cluster:
    id: int
    title: str
    summary: str | None
    first_published_at: datetime
    last_published_at: datetime
    original_size: int
    lead: str = ""
    member_titles: list[str] = field(default_factory=list)
    companies: dict[int, str] = field(default_factory=dict)

    @property
    def date(self) -> str:
        return self.first_published_at.astimezone(SEOUL).date().isoformat()

    @property
    def text(self) -> str:
        return embedding_text(self.title, self.summary, self.member_titles)


@dataclass(frozen=True)
class SampledPair:
    a: int
    b: int
    band: str
    band_population: int
    score: float


# ── DB 읽기 ──────────────────────────────────────────────────────────────────


def load_clusters(load_start: datetime, lead_chars: int) -> tuple[list[Cluster], bool]:
    """load_start 이후 시작한 이름 있는 클러스터. 두 번째 값은 summary 컬럼 존재 여부."""

    from sqlalchemy import text

    from pipelines.common.clients.postgres import session_scope

    with session_scope() as session:
        # 평가 스크립트가 공유 DB 를 건드릴 일이 없도록 트랜잭션 자체를 읽기 전용으로 연다.
        session.execute(text("SET TRANSACTION READ ONLY"))
        has_summary = bool(session.execute(text(SUMMARY_COLUMN_EXISTS_SQL)).scalar())
        summary_expr = "nc.summary" if has_summary else "NULL::text"
        rows = session.execute(
            text(SELECT_CLUSTERS_SQL.format(summary=summary_expr)),
            {"load_start": load_start, "lead_fetch_chars": LEAD_FETCH_CHARS},
        ).fetchall()

        clusters = [
            Cluster(
                id=int(row.id),
                title=collapse(row.title),
                summary=collapse(row.summary) or None,
                first_published_at=row.first_published_at,
                last_published_at=row.last_published_at,
                original_size=int(row.original_size),
                lead=collapse(row.lead)[:lead_chars] if lead_chars > 0 else "",
            )
            for row in rows
        ]
        by_id = {cluster.id: cluster for cluster in clusters}
        cluster_ids = list(by_id)
        if not cluster_ids:
            return [], has_summary

        for cluster_id, title in session.execute(
            text(SELECT_MEMBER_TITLES_SQL),
            {"cluster_ids": cluster_ids, "limit": MEMBER_TITLE_COUNT},
        ):
            by_id[int(cluster_id)].member_titles.append(collapse(title))

        for cluster_id, company_id, name in session.execute(
            text(SELECT_CLUSTER_COMPANIES_SQL), {"cluster_ids": cluster_ids}
        ):
            by_id[int(cluster_id)].companies[int(company_id)] = name

    return clusters, has_summary


# ── 후보 쌍 ──────────────────────────────────────────────────────────────────
# 클러스터는 (first_published_at, id) 순으로 정렬돼 있고, 쌍은 인덱스로 다룬다. 쌍 (a, b) 는
# a * n + b 정수 코드로 바꿔 np.unique·isin 으로 중복과 소속을 판정한다.


def _window_bounds(
    times: np.ndarray, lookback_s: float, since_ts: float
) -> tuple[np.ndarray, np.ndarray]:
    """정렬된 시각마다 [시작, 끝) 인덱스: lookback 이내이면서 엄격히 먼저 시작한 것들.

    since_ts 이전에 시작한 B 는 평가 대상이 아니라 창 크기를 0 으로 둔다(A 로는 쓰인다).
    """

    lo = np.searchsorted(times, times - lookback_s, side="left")
    hi = np.searchsorted(times, times, side="left")
    hi = np.where(times >= since_ts, hi, lo)
    return lo, hi


def _expand_windows(lo: np.ndarray, hi: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """각 위치 j 의 창 [lo_j, hi_j) 를 (a 위치, b 위치) 배열로 펼친다."""

    counts = hi - lo
    total = int(counts.sum())
    b_pos = np.repeat(np.arange(len(lo)), counts)
    offsets = np.arange(total) - np.repeat(np.cumsum(counts) - counts, counts)
    a_pos = np.repeat(lo, counts) + offsets
    return a_pos, b_pos


def _group_pair_codes(
    indices: list[int], times: np.ndarray, lookback_s: float, since_ts: float
) -> np.ndarray:
    """정렬된 인덱스 묶음 안에서만 만든 후보 쌍 코드."""

    idx = np.asarray(indices, dtype=np.int64)
    lo, hi = _window_bounds(times[idx], lookback_s, since_ts)
    a_pos, b_pos = _expand_windows(lo, hi)
    return idx[a_pos] * len(times) + idx[b_pos]


def shared_company_pairs(
    times: np.ndarray, company_sets: list[set[int]], lookback_s: float, since_ts: float
) -> np.ndarray:
    """회사를 하나 이상 공유하는 후보 쌍의 정렬된 코드 배열."""

    members: dict[int, list[int]] = defaultdict(list)
    for index, companies in enumerate(company_sets):
        for company_id in companies:
            members[company_id].append(index)

    codes = [
        _group_pair_codes(indices, times, lookback_s, since_ts)
        for indices in members.values()
        if len(indices) >= 2
    ]
    if not codes:
        return np.empty(0, dtype=np.int64)
    return np.unique(np.concatenate(codes))


def no_company_pairs(
    times: np.ndarray, company_sets: list[set[int]], lookback_s: float, since_ts: float
) -> np.ndarray:
    """둘 다 회사가 없는 후보 쌍의 정렬된 코드 배열. 운영의 회사 없는 클러스터 연결 후보와 같다."""

    indices = [index for index, companies in enumerate(company_sets) if not companies]
    if len(indices) < 2:
        return np.empty(0, dtype=np.int64)
    return np.sort(_group_pair_codes(indices, times, lookback_s, since_ts))


def window_pair_count(times: np.ndarray, lookback_s: float, since_ts: float) -> int:
    lo, hi = _window_bounds(times, lookback_s, since_ts)
    return int((hi - lo).sum())


def sample_no_shared_pairs(
    times: np.ndarray,
    candidate_codes: np.ndarray,
    lookback_s: float,
    since_ts: float,
    k: int,
    rng: random.Random,
) -> np.ndarray:
    """lookback 안의 쌍 전체에서 균등하게 뽑되 운영 후보 쌍은 버린다(기각 표집).

    candidate_codes 는 회사 공유 쌍과 회사 없는 쌍을 합친 정렬된 코드 배열이다.
    """

    n = len(times)
    lo, hi = _window_bounds(times, lookback_s, since_ts)
    counts = hi - lo
    cumulative = np.cumsum(counts)
    total = int(cumulative[-1]) if n else 0
    population = total - len(candidate_codes)
    if k <= 0 or population <= 0:
        return np.empty(0, dtype=np.int64)

    k = min(k, population)
    chosen: set[int] = set()
    attempts = 0
    # 후보 쌍이 대부분이면 기각이 길어진다. 무한 루프를 막는 상한.
    max_attempts = 200 * k + 10_000
    while len(chosen) < k and attempts < max_attempts:
        attempts += 1
        r = rng.randrange(total)
        b = int(np.searchsorted(cumulative, r, side="right"))
        a = int(lo[b] + (r - (cumulative[b] - counts[b])))
        code = a * n + b
        position = np.searchsorted(candidate_codes, code)
        if position < len(candidate_codes) and candidate_codes[position] == code:
            continue
        chosen.add(code)

    if len(chosen) < k:
        log.warning("no_shared 쌍을 %d개만 뽑았다 (요청 %d)", len(chosen), k)
    return np.asarray(sorted(chosen), dtype=np.int64)


def pair_scores(embeddings: np.ndarray, codes: np.ndarray) -> np.ndarray:
    """정규화된 임베딩 행렬에서 쌍 코드별 코사인. 쌍이 많아도 메모리가 터지지 않게 나눠 계산한다."""

    n = len(embeddings)
    scores = np.empty(len(codes), dtype=np.float64)
    for start in range(0, len(codes), SCORE_CHUNK):
        chunk = codes[start : start + SCORE_CHUNK]
        a, b = chunk // n, chunk % n
        scores[start : start + len(chunk)] = np.einsum("ij,ij->i", embeddings[a], embeddings[b])
    return scores


def stratified_sample(
    codes: np.ndarray,
    scores: np.ndarray,
    per_band: int,
    rng: random.Random,
    labels: tuple[str, ...] = BAND_LABELS,
) -> tuple[list[tuple[int, str, float]], dict[str, int]]:
    """점수 밴드마다 per_band 개를 비복원 균등 추출한다. 밴드별 모집단 크기도 돌려준다.

    labels 는 BAND_EDGES 순서의 밴드 이름이다. codes 가 정렬돼 있으면 같은 시드에서 같은 표본이
    나온다.
    """

    band_index = np.digitize(scores, BAND_EDGES)
    sampled: list[tuple[int, str, float]] = []
    populations: dict[str, int] = {}
    for index, band in enumerate(labels):
        positions = np.flatnonzero(band_index == index).tolist()
        populations[band] = len(positions)
        for position in sorted(rng.sample(positions, min(per_band, len(positions)))):
            sampled.append((int(codes[position]), band, float(scores[position])))
    return sampled, populations


def build_sample(
    times: np.ndarray,
    company_sets: list[set[int]],
    embeddings: np.ndarray,
    *,
    lookback_days: float,
    since_ts: float,
    per_band: int,
    per_band_no_company: int,
    no_shared: int,
    seed: int,
) -> tuple[list[SampledPair], dict[str, Any]]:
    """후보 생성 → 점수 → 층화 추출. 밴드별 모집단과 표본 수를 통계로 같이 돌려준다."""

    n = len(times)
    lookback_s = lookback_days * DAY_SECONDS
    rng = random.Random(seed)

    shared = shared_company_pairs(times, company_sets, lookback_s, since_ts)
    shared_scores = pair_scores(embeddings, shared)
    sampled, populations = stratified_sample(shared, shared_scores, per_band, rng)

    # 회사 공유 쌍과 회사 없는 쌍은 겹치지 않는다. 둘 다 운영 후보라 no_shared 에서 뺀다.
    no_company = no_company_pairs(times, company_sets, lookback_s, since_ts)
    candidates = np.union1d(shared, no_company)
    window_total = window_pair_count(times, lookback_s, since_ts)
    no_shared_codes = sample_no_shared_pairs(
        times, candidates, lookback_s, since_ts, no_shared, rng
    )
    no_shared_scores = pair_scores(embeddings, no_shared_codes)
    populations[NO_SHARED_BAND] = window_total - len(candidates)

    # 회사 없는 쌍은 no_shared 다음에 뽑는다 — 같은 시드의 회사 공유·no_shared 표본을 그대로 둔다.
    no_company_sampled, no_company_populations = stratified_sample(
        no_company,
        pair_scores(embeddings, no_company),
        per_band_no_company,
        rng,
        NO_COMPANY_BAND_LABELS,
    )
    sampled.extend(no_company_sampled)
    populations.update(no_company_populations)

    pairs = [
        SampledPair(code // n, code % n, band, populations[band], score)
        for code, band, score in sampled
    ]
    pairs.extend(
        SampledPair(int(code) // n, int(code) % n, NO_SHARED_BAND, populations[NO_SHARED_BAND], s)
        for code, s in zip(no_shared_codes.tolist(), no_shared_scores.tolist(), strict=True)
    )
    sampled_counts: dict[str, int] = defaultdict(int)
    for pair in pairs:
        sampled_counts[pair.band] += 1

    stats = {
        "window_pairs": window_total,
        "shared_pairs": len(shared),
        "no_company_pairs": len(no_company),
        "band_population": populations,
        "band_sampled": {band: sampled_counts.get(band, 0) for band in populations},
    }
    return pairs, stats


# ── 출력 ─────────────────────────────────────────────────────────────────────


def pair_record(pair: SampledPair, a: Cluster, b: Cluster) -> dict[str, Any]:
    shared = sorted(set(a.companies) & set(b.companies))
    gap = (b.first_published_at - a.first_published_at).total_seconds() / DAY_SECONDS
    return {
        "pair_id": f"{a.id}_{b.id}",
        "band": pair.band,
        "band_population": pair.band_population,
        "score": round(pair.score, 6),
        "a_id": a.id,
        "a_date": a.date,
        "a_title": a.title,
        "a_summary": a.summary or "",
        "a_member_titles": a.member_titles,
        "b_id": b.id,
        "b_date": b.date,
        "b_title": b.title,
        "b_summary": b.summary or "",
        "b_member_titles": b.member_titles,
        "shared_companies": [a.companies[c] for c in shared],
        "llm_label": "",
        "llm_reason": "",
        "human_label": "",
        "human_note": "",
        "a_lead": a.lead,
        "b_lead": b.lead,
        # 아래는 JSONL 에만 남는다 (prelabel 입력·사후 분석용).
        "gap_days": round(gap, 2),
        "a_original_size": a.original_size,
        "b_original_size": b.original_size,
        "a_companies": sorted(a.companies.values()),
        "b_companies": sorted(b.companies.values()),
        "a_first_published_at": a.first_published_at.isoformat(),
        "b_first_published_at": b.first_published_at.isoformat(),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="이슈 연결 평가용 클러스터 쌍 추출 (읽기 전용)")
    parser.add_argument("--since-days", type=int, default=90, help="B 클러스터 시작 범위(일)")
    parser.add_argument("--lookback-days", type=int, default=90, help="A 가 B 보다 앞설 최대 일수")
    parser.add_argument("--per-band", type=int, default=60, help="점수 밴드당 표본 수")
    parser.add_argument(
        "--per-band-no-company",
        type=int,
        default=30,
        help="둘 다 회사 없는 쌍의 점수 밴드당 표본 수 (0 이면 생략)",
    )
    parser.add_argument(
        "--no-shared", type=int, default=30, help="회사 비공유 쌍 표본 수 (0 이면 생략)"
    )
    parser.add_argument(
        "--lead-chars", type=int, default=200, help="대표 기사 본문 리드 길이 (0 이면 생략)"
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument(
        "--cache", type=Path, default=None, help="임베딩 캐시 (기본: <out>/embedding_cache.jsonl)"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args(argv)

    from pipelines.common.clients.bedrock import embed_texts
    from pipelines.news.config import get_news_settings

    # 운영 link_issues 와 같은 설정을 읽는다. 테마용 BEDROCK_EMBEDDING_MODEL 과는 별개다.
    model = get_news_settings().issue_embedding_model
    log.info("[임베딩] 모델 %s (NEWS_ISSUE_EMBEDDING_MODEL)", model)

    now = datetime.now(UTC)
    since_start = now - timedelta(days=args.since_days)
    load_start = since_start - timedelta(days=args.lookback_days)
    clusters, has_summary = load_clusters(load_start, args.lead_chars)
    if not clusters:
        log.warning("대상 클러스터가 없다 (since=%s)", load_start.isoformat())
        return

    in_scope = sum(1 for c in clusters if c.first_published_at >= since_start)
    no_company = sum(1 for c in clusters if not c.companies)
    with_summary = sum(1 for c in clusters if c.summary)
    log.info(
        "[클러스터] 전체 %d (B 범위 %d), 회사 없음 %d, 요약 있음 %d%s",
        len(clusters),
        in_scope,
        no_company,
        with_summary,
        "" if has_summary else " — summary 컬럼 없음(마이그레이션 전): 이름+기사 제목만 임베딩",
    )

    protect_output_dir(args.out)
    cache = EmbeddingCache(args.cache or args.out / "embedding_cache.jsonl")
    embeddings, embedded = embed_with_cache(
        [(c.id, c.text) for c in clusters],
        cache,
        lambda texts: embed_texts(texts, EMBEDDING_DIM, model=model),
        model,
    )
    log.info("[임베딩] 새로 %d건, 캐시 %d건", embedded, len(clusters) - embedded)

    times = np.asarray([c.first_published_at.timestamp() for c in clusters], dtype=np.float64)
    pairs, stats = build_sample(
        times,
        [set(c.companies) for c in clusters],
        embeddings,
        lookback_days=args.lookback_days,
        since_ts=since_start.timestamp(),
        per_band=args.per_band,
        per_band_no_company=args.per_band_no_company,
        no_shared=args.no_shared,
        seed=args.seed,
    )
    for band, population in stats["band_population"].items():
        log.info("[밴드] %-20s 모집단 %7d / 표본 %d", band, population, stats["band_sampled"][band])

    records = [pair_record(pair, clusters[pair.a], clusters[pair.b]) for pair in pairs]
    # 검수자가 행 순서로 점수를 짐작하지 않도록 섞는다.
    random.Random(args.seed).shuffle(records)

    write_jsonl(args.out / "pairs.jsonl", records)
    write_csv(args.out / "pairs.csv", records)
    meta = {
        "generated_at": now.isoformat(),
        "since_days": args.since_days,
        "lookback_days": args.lookback_days,
        "per_band": args.per_band,
        "per_band_no_company": args.per_band_no_company,
        "no_shared": args.no_shared,
        "seed": args.seed,
        "embedding_model": model,
        "embedding_dim": EMBEDDING_DIM,
        "summary_column": has_summary,
        "clusters": len(clusters),
        "clusters_in_scope": in_scope,
        "clusters_without_company": no_company,
        "clusters_with_summary": with_summary,
        **stats,
    }
    (args.out / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info("[완료] 쌍 %d개 → %s", len(records), args.out)


if __name__ == "__main__":
    main()
