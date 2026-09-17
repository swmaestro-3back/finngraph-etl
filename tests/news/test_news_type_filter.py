"""news_type_filter의 해외 특징주 태그 필터 + 제목 선두 브라켓 제거 단위 테스트."""

from __future__ import annotations


def _item(title: str) -> dict:
    return {"title": title, "description": "", "link": f"https://x.com/{hash(title)}"}


def _filter(items: list[dict]) -> tuple[list[dict], list[dict]]:
    from pipelines.news.transformers.filters.news_type_filter import filter_official_source_news

    return filter_official_source_news(items, pipeline_input={}, official_source_threshold=0)


def test_foreign_featured_stock_tags_are_filtered():
    items = [
        _item("[미국 특징주] 테슬라 급등"),
        _item("[일본 특징주] 도요타 상승"),
        _item("[중국 특징주] 비야디 강세"),
        _item("[이란 특징주] 정유주 급등"),
        _item("[홍콩 특징주] 텐센트 반등"),
        _item("[유럽 특징주] ASML 신고가"),
    ]

    kept, removed = _filter(items)

    assert kept == []
    assert len(removed) == 6


def test_featured_stock_tag_without_space_is_filtered():
    kept, removed = _filter([_item("[미국특징주] 테슬라 급등")])

    assert kept == []
    assert len(removed) == 1


def test_featured_stock_tag_with_html_title_is_filtered():
    # 네이버 검색 API 제목에는 <b> 태그가 섞여 온다
    kept, removed = _filter([_item("[미국 특징주] <b>테슬라</b> 급등")])

    assert kept == []
    assert len(removed) == 1


def test_bare_featured_stock_tag_is_kept():
    # 국가 수식어 없는 "[특징주]"는 국내 개별 종목 뉴스라 걸러내지 않는다
    kept, removed = _filter([_item("[특징주] 삼성전자 급등")])

    assert len(kept) == 1
    assert removed == []


def test_normal_title_is_kept():
    kept, removed = _filter([_item("삼성전자, 3분기 영업이익 컨센서스 상회")])

    assert len(kept) == 1
    assert removed == []


def test_remove_leading_title_brackets():
    from pipelines.news.utils.text_utils import remove_leading_title_brackets

    assert remove_leading_title_brackets("[속보] 삼성전자, 신제품 공개") == "삼성전자, 신제품 공개"
    # 연속된 선두 태그는 전부 제거한다
    assert remove_leading_title_brackets("[속보][마켓뷰] 코스피 반등") == "코스피 반등"
    # 제목 중간의 브라켓은 건드리지 않는다
    assert remove_leading_title_brackets("삼성전자 [공식] 입장 발표") == "삼성전자 [공식] 입장 발표"
    assert remove_leading_title_brackets("브라켓 없는 제목") == "브라켓 없는 제목"
    assert remove_leading_title_brackets("") == ""
    # 태그만 있고 본문이 없으면 빈 문자열이 된다
    assert remove_leading_title_brackets("[포토]") == ""


def test_bot_price_notes_are_filtered():
    # sim 검색에서 30% 를 차지하던 봇 생성 시세 단신 (PRD 2026-09-10 실측)
    items = [
        _item("엘앤에프 주가, 9월 9일 장중 114,900원 1.06% 상승"),
        _item("비에이치 주가, 9월 9일 19,940원 1.37% 상승 마감"),
        _item("삼성전자 주가,10월 1일 장중 26만9500원 보합"),
    ]

    kept, removed = _filter(items)

    assert kept == []
    assert len(removed) == 3
    assert removed[0]["debug_info"]["excluded_keywords"][0]["position"] == "price_note"


def test_price_mention_without_date_note_is_kept():
    # 날짜 단신 형식이 아닌 시세 언급은 다른 단계(LLM)가 판단한다
    kept, removed = _filter(
        [
            _item("엘앤에프 12만7600원, 12%대 급등…LFP·ESS 성장 기대에 매수세"),
            _item("삼성전자, 자사주 매입 결정…주가 방어 나서"),
        ]
    )

    assert len(kept) == 2
    assert removed == []
