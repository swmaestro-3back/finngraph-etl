"""검색어 하나로 네이버 검색 → 필터 → 관련성 판정 → 본문 크롤링 → 삼중항 추출까지 돌린다.

collect_articles.collect 의 2~10 단계와 extract_triples 의 LangGraph 실행을 같은 함수로 이어
붙인 것이다. DB·Neo4j 에는 아무것도 쓰지 않는다 — 저장, 클러스터 기록, 기업 연결, search_history
마킹, relation_sources 적재, 간선 동기화, triple_extracted 마킹을 모두 뺐다. 읽기는 개체 사전
(entity_gazetteer) 한 번뿐이다.

운영 런과 다른 점:
- 검색어를 서식(NEWS_SEARCH_QUERY_TEMPLATES)으로 만들지 않고 받은 그대로 한 번만 검색한다.
  검색 종목은 검색어의 첫 쉼표 앞 이름이다.
- 1페이지의 상위 --top 건만 본다. 워터마크·lookback 컷오프는 적용하지 않는다.
- DB 에 이미 저장된 URL 을 거르지 않는다 (이미 수집된 기사도 다시 돌려 본다).
- 클러스터 판정을 하지 않는다 — cap 없이 관련성을 통과한 기사 전부를 크롤링·추출한다.

네이버 검색 API 와 Bedrock 을 실제로 부르므로 pytest 가 잡지 않는 스크립트로 둔다.

실행: uv run python scripts/probe_news_to_triples.py "두산에너빌리티,특징주"
로그: logs/probe_news_to_triples_<시각>.log (--log-file 로 변경, 확장자는 항상 .log)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import textwrap
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_KEYWORD = "두산에너빌리티,특징주"
NOISY_LOGGERS = ("botocore", "boto3", "urllib3", "httpx", "httpcore", "langchain_aws", "asyncio")
LOGGER_NAME = "probe_news_to_triples"
RULE_WIDTH = 100
BODY_WIDTH = 60

log = logging.getLogger(LOGGER_NAME)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("keyword", nargs="?", default=DEFAULT_KEYWORD, help="네이버 검색어")
    parser.add_argument("--top", type=int, default=10, help="1페이지에서 볼 상위 기사 수")
    parser.add_argument("--log-file", type=Path, default=None, help="로그 파일 경로 (.log)")
    return parser.parse_args()


# ==============================================================================
# 로그 서식
# ==============================================================================


class ReportFormatter(logging.Formatter):
    """이 스크립트의 줄은 메시지만, 파이프라인 내부 로그는 출처를 붙여 들여 쓴다.

    보고서 줄마다 시각·레벨·로거명이 붙으면 표가 읽히지 않는다. 시각은 단계 머리줄에만 둔다.
    """

    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        if record.exc_info:
            message = f"{message}\n{self.formatException(record.exc_info)}"

        if record.name == LOGGER_NAME:
            return message

        prefix = f"      · {record.levelname:<7} {record.name}: "
        return prefix + message.replace("\n", "\n" + " " * len(prefix))


def configure_logging(log_file: Path) -> None:
    """파일에는 DEBUG 전부, 콘솔에는 INFO 만. pipelines import 보다 먼저 불러야 한다.

    pipelines 모듈들이 import 시점에 logging.basicConfig 를 부르는데, 루트에 핸들러가 이미
    있으면 아무 일도 하지 않는다 — 그래서 파이프라인 내부 로그(logging.debug 포함)가 이 파일로
    모인다.
    """

    log_file.parent.mkdir(parents=True, exist_ok=True)
    formatter = ReportFormatter()

    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(file_handler)
    root.addHandler(console_handler)

    for name in NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)


def banner(lines: list[str]) -> None:
    log.info("=" * RULE_WIDTH)
    for line in lines:
        log.info("  %s", line)
    log.info("=" * RULE_WIDTH)


def section(title: str) -> None:
    head = f"── {title} "
    stamp = f" {datetime.now():%H:%M:%S} ──"
    log.info("")
    log.info("%s%s%s", head, "─" * max(4, RULE_WIDTH - len(head) - len(stamp)), stamp)


def article_list(label: str, items: list[dict[str, Any]], notes: list[str] | None = None) -> None:
    """기사 목록 한 묶음. notes 는 기사마다 제목 아래에 붙일 한 줄이다."""

    log.info("")
    log.info("  %s (%d건)", label, len(items))
    if not items:
        log.info("      (없음)")
        return

    for index, item in enumerate(items):
        log.info("    %2d. %s", index + 1, item.get("title", ""))
        if notes:
            log.info("        → %s", notes[index])
        log.debug("        %s | %s", item.get("pubDate", ""), item.get("link", ""))


def field(label: str, value: Any, indent: int = 8) -> None:
    """'라벨 : 값' 한 줄. 긴 값은 값 시작 위치에 맞춰 줄바꿈한다."""

    head = f"{' ' * indent}{label:<10}: "
    wrapped = textwrap.wrap(str(value), width=BODY_WIDTH) or [""]
    log.info("%s%s", head, wrapped[0])
    for line in wrapped[1:]:
        log.info("%s%s", " " * len(head), line)


def body_block(text: str) -> None:
    for paragraph in text.splitlines():
        for line in textwrap.wrap(paragraph, width=BODY_WIDTH) or [""]:
            log.debug("        │ %s", line)


def company_names(companies: list[dict[str, Any]] | None) -> str:
    return ", ".join(
        company["name"]
        if company.get("surface") in (None, company["name"])
        else f"{company['surface']}→{company['name']}"
        for company in companies or []
    )


# ==============================================================================
# 수집 · 필터
# ==============================================================================


def resolve_query_company(name: str) -> dict[str, Any]:
    """검색 종목 이름을 개체 사전으로 company_id 에 잇는다. 제목 기업 매치가 id 로 비교한다."""

    from pipelines.common.gazetteer import get_company_matcher

    matches = get_company_matcher().extract(name)
    if not matches:
        raise SystemExit(f"개체 사전에 없는 종목명: {name!r} — 검색어 첫 쉼표 앞이 종목명이어야 함")

    entry = matches[0].entry
    return {"company_id": entry.company_id, "name": entry.canonical}


def search(keyword: str, top: int, query_company: dict[str, Any]) -> list[dict[str, Any]]:
    from pipelines.news.extractors.search_collector import (
        iter_search_news_pages,
        validate_search_settings,
    )

    validate_search_settings()

    items: list[dict[str, Any]] = []
    for page_items in iter_search_news_pages(keyword=keyword, max_pages=1, display=top):
        items.extend(page_items)

    for item in items:
        item["_query_company"] = dict(query_company)
        item["_search_keyword"] = keyword

    return items[:top]


def collect_and_filter(keyword: str, top: int) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """collect_articles.collect 의 2~10 단계. (본문이 있는 기사, 단계별 건수) 를 돌려준다."""

    from pipelines.news.config import get_news_settings
    from pipelines.news.extractors.text_fetcher import fetch_article_body
    from pipelines.news.repositories.postgres.news import has_article_body
    from pipelines.news.transformers.company_matches import match_title_companies
    from pipelines.news.transformers.filters.duplicate_filter import remove_duplicate_by_url
    from pipelines.news.transformers.filters.news_type_filter import filter_official_source_news
    from pipelines.news.transformers.filters.relevance_filter import filter_relevant_news
    from pipelines.news.utils.text_utils import remove_leading_title_brackets

    settings = get_news_settings()

    query_company = resolve_query_company(keyword.split(",")[0].strip())
    banner(
        [
            f"probe_news_to_triples   {datetime.now():%Y-%m-%d %H:%M:%S}",
            f"검색어    : {keyword}",
            f"검색 종목 : {query_company['name']} (company_id={query_company['company_id']})",
            f"범위      : 1페이지 상위 {top}건 · DB 저장 없음",
        ]
    )

    # 2. 네이버 기사 수집 (1페이지, 상위 top 건)
    section("[1] 네이버 검색")
    collected = search(keyword, top, query_company)
    article_list("수집", collected)

    # 3. 제목 기업 매치
    section("[2] 제목 기업 매치")
    matched = match_title_companies(collected)
    article_list(
        "통과",
        matched.kept,
        [f"판정 기업: {company_names(item['_title_companies'])}" for item in matched.kept],
    )
    article_list("제거 — 제목에 검색 종목 없음", matched.removed)

    # 4. 배치 내 URL 중복 제거
    section("[3] URL 중복 제거")
    unique_items, duplicates = remove_duplicate_by_url(matched.kept)
    log.info("")
    log.info("  남음 %d건 / 제거 %d건", len(unique_items), len(duplicates))

    # 5. 기사 유형 필터
    section("[4] 기사 유형 필터")
    typed_items, type_removed = filter_official_source_news(
        unique_items,
        pipeline_input={},
        official_source_threshold=settings.official_source_threshold,
    )
    article_list("통과", typed_items)
    article_list(
        "제거",
        [removed["removed_item"] for removed in type_removed],
        [
            f"score={removed['official_source_score']} {removed['debug_info'].get('decision', '')}"
            for removed in type_removed
        ],
    )

    # 6. 제목 폴리싱 (선두 브라켓 제거)
    for item in typed_items:
        item["title"] = remove_leading_title_brackets(item.get("title", ""))

    # 7. DB 기존 URL 제거는 건너뛴다 (테스트 — 이미 수집된 기사도 다시 본다)

    # 8. LLM 관련성 필터
    section("[5] LLM 관련성 판정")
    relevance = filter_relevant_news(typed_items, max_concurrency=settings.news_llm_max_concurrency)
    article_list(
        "통과",
        relevance.passed,
        [f"연결 기업: {company_names(item.get('_linked_companies'))}" for item in relevance.passed],
    )
    article_list("무효 — 기업 자체 사건·관계 아님", relevance.invalid)
    article_list("실패 — LLM 판정 실패", relevance.failed)

    # 9. 클러스터 판정은 건너뛴다 — 관련성을 통과한 기사 전부를 크롤링한다

    # 10. 본문 크롤링
    section("[6] 본문 크롤링")
    fetched = fetch_article_body(relevance.passed)
    storable = [item for item in fetched if has_article_body(item)]
    for index, item in enumerate(fetched, start=1):
        text = item.get("_text") or ""
        log.info("")
        log.info("    %2d. %s", index, item["title"])
        log.info(
            "        %s · %d자 · %s",
            "성공" if text else "실패",
            len(text),
            item.get("_body_source_url") or "-",
        )
        body_block(text)

    counts = {
        "수집": len(collected),
        "제목 매치": len(matched.kept),
        "URL 중복 제거": len(unique_items),
        "유형 필터": len(typed_items),
        "관련성 통과": len(relevance.passed),
        "본문 성공": len(storable),
    }
    return storable, counts


# ==============================================================================
# 삼중항 추출
# ==============================================================================


def edge_text(subject: str, predicate: str, object_: str) -> str:
    return f"({subject}) -[{predicate}]-> ({object_})"


def log_candidate_frames(frames: list[Any]) -> None:
    log.debug("")
    log.debug("      후보 프레임 상세")
    for index, frame in enumerate(frames, start=1):
        log.debug(
            "      %2d. %s",
            index,
            edge_text(frame.subject.text, frame.predicate, frame.object.text),
        )
        log.debug("          item   : %s", frame.item)
        log.debug("          문장   : %s", frame.source_sentence)
        log.debug("          clause : %s", frame.clause)


def log_triplet(index: int, triplet: Any) -> None:
    log.info("")
    log.info(
        "      %2d. %s",
        index,
        edge_text(triplet.subject.canonical, triplet.predicate, triplet.object.canonical),
    )
    field("item", triplet.item, indent=10)
    field("polarity", triplet.polarity, indent=10)
    field("tense", triplet.tense, indent=10)
    field(
        "impact",
        f"subject={triplet.subject_impact} / object={triplet.object_impact}",
        indent=10,
    )
    field("evidence", triplet.evidence, indent=10)
    field("sentence", triplet.source_sentence, indent=10)


async def extract_triples(items: list[dict[str, Any]]) -> dict[str, int]:
    """extract_triples._process_item 에서 LangGraph 실행만. 원장 적재·그래프 동기화·마킹은 없다."""

    from pipelines.triples.workflow import GraphRunner

    stats = {"has_triples": 0, "no_triples": 0, "failed": 0, "triplets": 0}
    if not items:
        return stats

    runner = GraphRunner()

    for number, item in enumerate(items, start=1):
        section(f"[7] 삼중항 추출 {number}/{len(items)}")
        log.info("")
        log.info("    %s", item["title"])
        log.debug("    %s", item.get("link", ""))

        try:
            state = await runner.ainvoke(str(number), item["_text"])
        except Exception:
            log.exception("    추출 실패")
            stats["failed"] += 1
            continue

        gazetteer_entities = state.get("gazetteer_entities") or []
        field(
            "사전 매치",
            ", ".join(
                e.text if e.text == e.canonical else f"{e.text}→{e.canonical}"
                for e in gazetteer_entities
            )
            or "(없음)",
        )
        if "entities" in state:
            kept = {e.text for e in state["entities"]}
            field("검증 통과", ", ".join(e.text for e in state["entities"]) or "(없음)")
            field(
                "검증 탈락",
                ", ".join(e.text for e in gazetteer_entities if e.text not in kept) or "(없음)",
            )
        else:
            field("검증", "생략 — 사전 매치 엔티티 2개 미만")

        if "candidate_frames" in state:
            field("후보 프레임", f"{len(state['candidate_frames'])}개")
        else:
            field("관계 추출", "생략 — 검증 통과 엔티티 2개 미만")
        if "annotated_frames" in state:
            field("주석 프레임", f"{len(state['annotated_frames'])}개")
        if "triplet_stats" in state:
            field("통계", state["triplet_stats"])

        if state.get("candidate_frames"):
            log_candidate_frames(state["candidate_frames"])

        triplets = state.get("triplets") or []
        log.info("")
        log.info("      삼중항 %d개", len(triplets))
        for index, triplet in enumerate(triplets, start=1):
            log_triplet(index, triplet)

        stats["has_triples" if triplets else "no_triples"] += 1
        stats["triplets"] += len(triplets)

    return stats


def main() -> None:
    args = parse_args()
    log_file = args.log_file or (
        ROOT_DIR / "logs" / f"probe_news_to_triples_{datetime.now():%Y%m%d_%H%M%S}.log"
    )
    log_file = log_file.with_suffix(".log")
    configure_logging(log_file)

    items, counts = collect_and_filter(args.keyword, args.top)
    stats = asyncio.run(extract_triples(items))

    section("요약")
    log.info("")
    log.info("  수집·필터 : %s", " → ".join(f"{name} {count}" for name, count in counts.items()))
    log.info(
        "  삼중항    : 대상 %d건 → 관계있음 %d / 관계없음 %d / 실패 %d (삼중항 %d개)",
        len(items),
        stats["has_triples"],
        stats["no_triples"],
        stats["failed"],
        stats["triplets"],
    )
    log.info("  로그 파일 : %s", log_file)


if __name__ == "__main__":
    main()
