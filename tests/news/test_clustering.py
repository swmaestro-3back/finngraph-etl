"""클러스터링 모듈 단위 테스트 (DB/외부 인프라 불필요)."""

from __future__ import annotations

from datetime import datetime, timedelta

from pipelines.news.utils.date_utils import SEOUL_TIMEZONE


def test_document_terms_builds_compound_nouns():
    from pipelines.news.transformers.clustering.preprocess import document_terms

    terms = dict(document_terms("LG에너지솔루션 유상증자 결정"))

    # 복합명사 복원: 붙어 있던 명사 조각이 하나로 이어져 가중치 3.0
    assert terms.get("lg에너지솔루션") == 3.0


def test_document_terms_drops_single_character_fragments():
    from pipelines.news.transformers.clustering.preprocess import document_terms

    terms = dict(document_terms("D램 가격 인상"))

    # "D램"이 쪼개진 "d" 조각은 버린다 — 복합명사 "d램"이 이미 그 정보를 담는다
    assert "d" not in terms
    assert terms.get("d램") == 3.0


def test_document_terms_drops_hanja():
    from pipelines.news.transformers.clustering.preprocess import document_terms

    terms = dict(document_terms("中 CXMT 반도체 증산"))

    # 한자는 매체마다 표기가 갈려 토큰으로 쓰지 않는다
    assert "中" not in terms
    assert set(terms) == {"cxmt", "반도체", "증산"}


def test_document_terms_does_not_build_compounds_across_hanja():
    from pipelines.news.transformers.clustering.preprocess import document_terms

    terms = dict(document_terms("철강株 수주"))

    # 한자가 명사 묶음을 끊어 "철강株" 복합명사가 만들어지지 않는다
    assert "철강株" not in terms
    assert "株" not in terms
    assert "철강" in terms


def test_document_terms_keeps_two_letter_abbreviations():
    from pipelines.news.transformers.clustering.preprocess import document_terms

    terms = dict(document_terms("SK하이닉스 실적 개선"))

    # 두 글자 영문 약어는 한 글자 규칙과 무관하다
    assert "sk" in terms
    assert terms.get("sk하이닉스") == 3.0


# ── batch.py: 기사 → 문서 변환, 시드 창 ──────────────────────────


def test_row_documents_use_title_and_body_lead():
    from pipelines.news.transformers.clustering import row_documents

    published = datetime(2026, 9, 2, 9, tzinfo=SEOUL_TIMEZONE)
    rows = [
        {
            "title": "삼성전자 유상증자 결정",
            "text": "삼성전자가 유상증자를 결정했다. " + "현대차 리콜 " * 50,
            "published_at": published,
        },
        {
            "title": "현대차 미국 리콜 확대",
            "text": "",
            "published_at": published + timedelta(hours=1),
        },
    ]

    documents, published_ats = row_documents(rows, lead_chars=20, description_weight=0.4)

    assert len(documents) == 2
    assert dict(documents[0])["삼성전자"] > 0
    # 리드 20자 밖의 본문 토큰은 들지 않는다
    assert "리콜" not in dict(documents[0])
    assert "리콜" in dict(documents[1])
    assert published_ats == [published, published + timedelta(hours=1)]


def test_seed_window_spans_batch_publish_range_minus_window():
    from pipelines.news.transformers.clustering import seed_window

    early = datetime(2026, 9, 1, 9, tzinfo=SEOUL_TIMEZONE)
    late = datetime(2026, 9, 10, 12, tzinfo=SEOUL_TIMEZONE)

    # 가장 이른 기사가 붙을 수 있는 가장 오래된 시드부터, 가장 늦은 기사 시각까지
    assert seed_window([late, early], 7) == (early - timedelta(days=7), late)
    # 뒤쪽 여유를 주면 가장 늦은 기사보다 그만큼 늦게 시작한 시드까지 읽는다
    assert seed_window([late, early], 7, 1) == (early - timedelta(days=7), late + timedelta(days=1))


def test_document_terms_normalizes_compatibility_characters():
    from pipelines.news.transformers.clustering.preprocess import document_terms

    # "㎿" 같은 호환 문자는 NFKC 로 "MW" 가 돼 같은 토큰으로 모인다
    assert document_terms("500㎿급 해상변전소") == document_terms("500MW급 해상변전소")


def test_add_batch_frequency_counts_each_document_once_on_top_of_table():
    from pipelines.news.transformers.clustering.batch import add_batch_frequency
    from pipelines.news.transformers.clustering.vectorize import IdfTable

    table = IdfTable(document_frequency={"삼성전자": 90, "해상변전소": 2}, document_count=100)
    documents = [
        [("삼성전자", 3.0), ("삼성전자", 1.0), ("파운드리", 1.0)],
        [("삼성전자", 3.0), ("해상변전소", 1.0)],
    ]

    merged = add_batch_frequency(table, documents)

    # 한 문서에 같은 토큰이 여러 번 나와도 문서 수는 1 만 오른다
    assert merged.document_frequency == {"삼성전자": 92, "해상변전소": 3, "파운드리": 1}
    assert merged.document_count == 102
    # 원래 표는 바뀌지 않는다
    assert table.document_frequency == {"삼성전자": 90, "해상변전소": 2}


def test_add_batch_frequency_on_empty_table_is_batch_count():
    from pipelines.news.transformers.clustering.batch import add_batch_frequency
    from pipelines.news.transformers.clustering.vectorize import IdfTable

    merged = add_batch_frequency(IdfTable(), [[("엘앤에프", 3.0)], [("엘앤에프", 1.0)], []])

    assert merged.document_frequency == {"엘앤에프": 2}
    assert merged.document_count == 3
