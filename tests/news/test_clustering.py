"""클러스터링 모듈 단위 테스트 (DB/외부 인프라 불필요)."""

from __future__ import annotations

from datetime import datetime, timedelta

from pipelines.news.utils.date_utils import SEOUL_TIMEZONE


def test_tokenize_builds_compound_nouns():
    from pipelines.news.transformers.clustering.preprocess import tokenize

    terms = dict(tokenize("LG에너지솔루션 유상증자 결정"))

    # 복합명사 복원: 붙어 있던 명사 조각이 하나로 이어져 가중치 3.0
    assert terms.get("lg에너지솔루션") == 3.0


def test_tokenize_drops_single_character_fragments():
    from pipelines.news.transformers.clustering.preprocess import tokenize

    terms = dict(tokenize("D램 가격 인상"))

    # "D램"이 쪼개진 "d" 조각은 버린다 — 복합명사 "d램"이 이미 그 정보를 담는다
    assert "d" not in terms
    assert terms.get("d램") == 3.0


def test_tokenize_drops_hanja():
    from pipelines.news.transformers.clustering.preprocess import tokenize

    terms = dict(tokenize("中 CXMT 반도체 증산"))

    # 한자는 매체마다 표기가 갈려 토큰으로 쓰지 않는다
    assert "中" not in terms
    assert set(terms) == {"cxmt", "반도체", "증산"}


def test_tokenize_does_not_build_compounds_across_hanja():
    from pipelines.news.transformers.clustering.preprocess import tokenize

    terms = dict(tokenize("철강株 수주"))

    # 한자가 명사 묶음을 끊어 "철강株" 복합명사가 만들어지지 않는다
    assert "철강株" not in terms
    assert "株" not in terms
    assert "철강" in terms


def test_tokenize_keeps_two_letter_abbreviations():
    from pipelines.news.transformers.clustering.preprocess import tokenize

    terms = dict(tokenize("SK하이닉스 실적 개선"))

    # 두 글자 영문 약어는 한 글자 규칙과 무관하다
    assert "sk" in terms
    assert terms.get("sk하이닉스") == 3.0


# ── batch.py: 저장 행 → 문서, 후보 조회 하한 ──────────────────────────


def test_row_documents_pass_stored_term_weights_through():
    from pipelines.news.transformers.clustering.batch import row_documents

    published = datetime(2026, 9, 2, 9, tzinfo=SEOUL_TIMEZONE)
    rows = [
        {
            "term_weights": {
                "title": {"삼성전자": 2.0},
                "body": {"유상증자": 1.0},
                "entities": {"5": 1.0},
            },
            "published_at": published,
        },
        {
            "term_weights": {"title": {"현대차": 2.0}, "body": {}, "entities": {}},
            "published_at": published + timedelta(hours=1),
        },
    ]

    documents, published_ats = row_documents(rows)

    assert documents[0] == rows[0]["term_weights"]
    assert documents[0] is not rows[0]["term_weights"]  # 사본
    assert published_ats == [published, published + timedelta(hours=1)]


def test_row_documents_tolerate_missing_fields():
    from pipelines.news.transformers.clustering.batch import row_documents

    at = datetime(2026, 9, 2, tzinfo=SEOUL_TIMEZONE)
    # 이관 기사: entities 없음. 필드가 전부 빠진 행도 빈 dict 로 채운다
    documents, _ = row_documents(
        [
            {"term_weights": {"title": {"a": 1.0}}, "published_at": at},
            {"term_weights": None, "published_at": at},
        ]
    )
    assert documents[0] == {"title": {"a": 1.0}, "body": {}, "entities": {}}
    assert documents[1] == {"title": {}, "body": {}, "entities": {}}


def test_candidate_since_is_the_earliest_article_minus_candidate_days():
    from pipelines.news.transformers.clustering.batch import candidate_since

    early = datetime(2026, 9, 1, 9, tzinfo=SEOUL_TIMEZONE)
    late = datetime(2026, 9, 10, 12, tzinfo=SEOUL_TIMEZONE)
    assert candidate_since([late, early], 10) == early - timedelta(days=10)


def test_tokenize_normalizes_compatibility_characters():
    from pipelines.news.transformers.clustering.preprocess import tokenize

    # "㎿" 같은 호환 문자는 NFKC 로 "MW" 가 돼 같은 토큰으로 모인다
    assert tokenize("500㎿급 해상변전소") == tokenize("500MW급 해상변전소")
