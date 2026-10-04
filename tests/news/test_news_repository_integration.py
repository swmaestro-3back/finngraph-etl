"""news repository 통합 테스트 — 본문 포함 저장, 저장된 URL 대조, 요약 대상.

로컬 DB 필요: docker compose up -d db 후 0000_schema.sql 적용 상태.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.repositories.postgres.news import (
    fetch_unsummarized_news_items,
    remove_stored_by_url,
    save_news,
    save_news_summaries,
)

pytestmark = pytest.mark.integration

BODY = "엘앤에프가 삼성SDI 에 양극재를 공급하는 계약을 체결했다고 밝혔다. " * 5


def _item(link: str, title: str = "엘앤에프, 양극재 공급 계약") -> dict:
    return {
        "title": title,
        "description": "",
        "link": link,
        "originallink": link,
        "pubDate": "Wed, 09 Sep 2026 10:00:00 +0900",
        "_text": BODY,
    }


@pytest.fixture
def link():
    marker = uuid.uuid4().hex
    yield f"https://example.com/news/{marker}"
    with session_scope() as session:
        session.execute(text("DELETE FROM news WHERE link LIKE :prefix"), {"prefix": f"%{marker}%"})


def _stored_text(news_id: int):
    with session_scope() as session:
        return session.execute(
            text("SELECT text FROM news WHERE id = :id"), {"id": news_id}
        ).scalar_one()


def test_save_news_inserts_with_body_then_skips_same_link(link):
    first = _item(link)
    result = save_news([first])

    assert result == {
        "inserted_count": 1,
        "skipped_existing_count": 0,
        "failed_count": 0,
        "linked_count": 0,
    }
    assert first["_save_action"] == "inserted"
    assert _stored_text(first["_news_id"]).startswith("엘앤에프가")

    again = _item(link)
    again["_text"] = "다른 본문 " * 20
    result = save_news([again])

    # 기존 행은 건드리지 않는다
    assert result["skipped_existing_count"] == 1
    assert again["_save_action"] == "skipped_existing"
    assert again["_news_id"] == first["_news_id"]
    assert _stored_text(first["_news_id"]).startswith("엘앤에프가")


def test_save_news_treats_query_string_and_trailing_slash_as_same_article(link):
    save_news([_item(link)])

    assert save_news([_item(f"{link}/?utm_source=x")])["skipped_existing_count"] == 1


def test_save_news_counts_article_without_body_as_failed(link):
    empty = _item(link)
    empty["_text"] = ""

    result = save_news([empty, _item(f"{link}-ok")])

    # 본문 없는 기사는 저장하지 않고, 그 실패가 다음 기사를 막지 않는다
    assert (result["inserted_count"], result["failed_count"]) == (1, 1)
    assert "_news_id" not in empty


@pytest.fixture
def company_ids(link):
    marker = link.rsplit("/", 1)[-1]
    with session_scope() as session:
        ids = [
            int(
                session.execute(
                    text(
                        """
                        INSERT INTO companies (name, is_listed, country)
                        VALUES (:name, true, 'KR')
                        RETURNING id;
                        """
                    ),
                    {"name": f"저장연결{marker[:8]}{suffix}"},
                ).scalar_one()
            )
            for suffix in "AB"
        ]

    yield ids

    with session_scope() as session:
        # news_companies 는 FK CASCADE 로 함께 삭제된다
        session.execute(text("DELETE FROM news WHERE link LIKE :p"), {"p": f"%{marker}%"})
        session.execute(text("DELETE FROM companies WHERE id = ANY(:ids)"), {"ids": ids})


def _linked_ids(news_id: int) -> list[int]:
    with session_scope() as session:
        rows = session.execute(
            text("SELECT company_id FROM news_companies WHERE news_id = :id ORDER BY company_id"),
            {"id": news_id},
        ).fetchall()
    return [int(row[0]) for row in rows]


def _news_id_of(link: str):
    with session_scope() as session:
        return session.execute(
            text("SELECT id FROM news WHERE link = :link"), {"link": link}
        ).scalar_one_or_none()


def test_save_news_links_companies_with_the_article(link, company_ids):
    first, second = company_ids
    item = _item(link)
    item["_linked_companies"] = [
        {"company_id": first, "name": "A"},
        {"company_id": second, "name": "B"},
    ]

    result = save_news([item])

    assert (result["inserted_count"], result["linked_count"]) == (1, 2)
    assert _linked_ids(item["_news_id"]) == sorted(company_ids)

    # 이미 있는 기사는 다시 연결하지 않는다 — 첫 저장 때 연결이 끝났다
    again = _item(link)
    again["_linked_companies"] = [{"company_id": first, "name": "A"}]
    assert save_news([again])["linked_count"] == 0


def test_save_news_does_not_keep_an_article_whose_companies_could_not_be_linked(link, company_ids):
    broken = _item(link)
    broken["_linked_companies"] = [{"company_id": -1, "name": "없는 기업"}]  # FK 위반
    ok = _item(f"{link}-ok")
    ok["_linked_companies"] = [{"company_id": company_ids[0], "name": "A"}]

    result = save_news([broken, ok])

    # 기사와 연결은 함께 저장되거나 함께 버려진다 — 기업 연결 없는 기사가 남지 않는다
    assert (result["inserted_count"], result["failed_count"], result["linked_count"]) == (1, 1, 1)
    assert _news_id_of(link) is None
    assert "_news_id" not in broken
    assert _linked_ids(ok["_news_id"]) == [company_ids[0]]


def test_remove_stored_by_url_drops_stored_and_keeps_new(link):
    save_news([_item(link)])
    stored_variant = _item(f"{link}?ref=y")
    new = _item(f"{link}-other")

    kept = remove_stored_by_url([stored_variant, new])

    assert kept == [new]


def test_only_cluster_representative_is_summarize_target(link):
    """삼중항 결과와 무관하게, 클러스터의 대표 기사만 요약 대상이다."""
    with session_scope() as session:
        representative, member = [
            int(
                session.execute(
                    text(
                        """
                        INSERT INTO news (title, text, link)
                        VALUES ('요약 대상 테스트', :body, :link)
                        RETURNING id;
                        """
                    ),
                    {"body": BODY, "link": f"{link}/{suffix}"},
                ).scalar_one()
            )
            for suffix in ("rep", "member")
        ]
        cluster_id = int(
            session.execute(
                text(
                    """
                    INSERT INTO news_clusters (
                        representative_news_id, original_size, member_count,
                        first_published_at, last_published_at
                    )
                    VALUES (:representative, 2, 2, now(), now())
                    RETURNING id;
                    """
                ),
                {"representative": representative},
            ).scalar_one()
        )
        session.execute(
            text("UPDATE news SET cluster_id = :c WHERE id = ANY(:ids)"),
            {"c": cluster_id, "ids": [representative, member]},
        )

    def targets(promote_size: int = 2):
        return [item["_news_id"] for item in fetch_unsummarized_news_items(promote_size)]

    try:
        # 삼중항 추출을 시도하지 않은(triple_extracted NULL) 상태에서도 대표는 요약 대상이다
        assert representative in targets()
        # 본문이 있어도 대표가 아니면 대상이 아니다
        assert member not in targets()
        # 옛 로직의 클러스터(대표는 있지만 후보 수가 승격 기준 미만)는 대상이 아니다 — 배포 직후
        # 옛 대표 기사 수천 건을 한꺼번에 요약하지 않는다
        assert representative not in targets(promote_size=3)

        save_news_summaries([(representative, "엘앤에프가 삼성SDI 와 공급 계약을 맺었다.")])
        assert representative not in targets()
    finally:
        with session_scope() as session:
            session.execute(text("DELETE FROM news_clusters WHERE id = :id"), {"id": cluster_id})
