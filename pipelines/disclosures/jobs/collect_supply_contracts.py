"""단일판매ㆍ공급계약체결 공시 수집 본체. 백필·일일 job 이 구간만 달리해 이걸 부른다.

원문 HTML 은 디스크에 저장하지 않는다 — 조회 → 파싱 → 적재가 한 공시의 메모리 안에서
끝난다. 재개 상태는 disclosures.rcept_no 다: 구간을 다시 돌면 목록만 재조회하고(월당
목록 API 몇 번이라 싸다) 테이블에 없는 접수번호만 원문을 받는다. 그래서 한도 소진·중단·
재실행이 전부 같은 경로로 수렴한다.
"""

from __future__ import annotations

from datetime import date

from pipelines.common.clients.dart import get_dart_client
from pipelines.common.clients.postgres import session_scope
from pipelines.common.config import get_settings
from pipelines.common.logging import get_logger
from pipelines.common.utils.batching import chunked
from pipelines.disclosures.extractors.dart import (
    QuotaExceeded,
    fetch_document_html,
    fetch_target_filings,
    month_windows,
)
from pipelines.disclosures.loaders.postgres import (
    fetch_existing_rcept_nos,
    upsert_disclosures,
)
from pipelines.disclosures.models import DisclosureRecord
from pipelines.disclosures.references.companies import fetch_corp_master, fetch_counterparty_aliases
from pipelines.disclosures.transformers.counterparty import CorpMaster
from pipelines.disclosures.transformers.parser import build_disclosure

logger = get_logger(__name__)

# 원문 조회+적재 커밋 단위. 한 트랜잭션에 수천 건을 몰면 실패 시 전부 되돌아가고,
# 건별 커밋은 왕복이 너무 잦다.
CHUNK_SIZE = 100


class _FetchLimitReached(Exception):
    """이번 회차 원문 조회 상한(batch_size)에 도달했다."""


def run(start: date, end: date, batch_size: int | None = None) -> None:
    """[start, end] 접수분의 단일판매ㆍ공급계약체결 공시를 수집·파싱·적재한다.

    Args:
        start (date): 접수일자 시작(포함).
        end (date): 접수일자 끝(포함).
        batch_size (int | None): 이번 회차의 원문 API 호출 수 상한.
            없으면 settings.disclosure_fetch_batch_size.
    """

    settings = get_settings()
    limit = batch_size or settings.disclosure_fetch_batch_size

    client = get_dart_client()
    with session_scope() as session:
        corp_master = CorpMaster(fetch_corp_master(session), fetch_counterparty_aliases(session))

    listed_total = fetched = written = missing = 0
    failed: list[str] = []

    try:
        for w_start, w_end in month_windows(start, end):
            targets = fetch_target_filings(client, w_start, w_end)
            listed_total += len(targets)
            if not targets:
                continue

            with session_scope() as session:
                existing = fetch_existing_rcept_nos(session, [str(f["rcept_no"]) for f in targets])
            todo = [f for f in targets if str(f["rcept_no"]) not in existing]
            logger.info("[%s~%s] 대상 %d건 중 신규 %d건", w_start, w_end, len(targets), len(todo))

            for chunk in chunked(todo, CHUNK_SIZE):
                records: list[DisclosureRecord] = []
                try:
                    for filing in chunk:
                        if fetched >= limit:
                            raise _FetchLimitReached
                        rcept_no = str(filing["rcept_no"])
                        # 결측·파싱실패여도 원문 API 1회를 쓴 것이라 시도 기준으로 센다.
                        fetched += 1
                        try:
                            html = fetch_document_html(client, rcept_no)
                        except QuotaExceeded:
                            raise
                        except Exception:
                            logger.exception("원문 조회 실패: rcept_no=%s", rcept_no)
                            failed.append(rcept_no)
                            continue

                        if not html:
                            missing += 1
                            continue

                        try:
                            records.append(build_disclosure(filing, html, corp_master))
                        except Exception:
                            logger.exception("파싱 실패: rcept_no=%s", rcept_no)
                            failed.append(rcept_no)
                finally:
                    # 상한·한도로 끊겨도 이미 받은 분량은 적재하고 나간다.
                    if records:
                        with session_scope() as session:
                            written += upsert_disclosures(session, records)
    except _FetchLimitReached:
        logger.info("회차 원문 조회 상한(%d) 도달, 다음 실행이 이어감", limit)
    except QuotaExceeded as exc:
        logger.warning("일 API 한도 도달, 중단(다음 실행이 이어감): %s", exc)

    logger.info(
        "공시 수집 완료: 목록 %d건, 원문조회 %d건, 적재 %d건, 원문없음 %d건, 실패 %d건 %s",
        listed_total,
        fetched,
        written,
        missing,
        len(failed),
        failed[:10],
    )
