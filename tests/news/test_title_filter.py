"""title_filter의 제외 패턴 탈락 + 제목 선두 브라켓 제거 단위 테스트."""

from __future__ import annotations


def _item(title: str) -> dict:
    return {"title": title, "description": "", "link": f"https://x.com/{hash(title)}"}


def _filter(items: list[dict]) -> tuple[list[dict], list[dict]]:
    from pipelines.news.transformers.filters.title_filter import filter_titles

    return filter_titles(items)


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
    assert removed[0]["excluded_by"] == "주가, 9월 9일"


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


def test_kept_items_have_leading_brackets_removed():
    kept, removed = _filter(
        [
            _item("[특징주] 삼성전자 급등"),
            _item("[속보][마켓뷰] 코스피 반등"),
            _item("삼성전자 [공식] 입장 발표"),
        ]
    )

    assert [item["title"] for item in kept] == [
        "삼성전자 급등",
        "코스피 반등",
        "삼성전자 [공식] 입장 발표",
    ]
    assert removed == []


def test_removed_items_keep_original_title():
    # 선두 브라켓으로 판정하므로 제거는 판정 뒤에 하고, 제거된 기사는 원본 제목을 남긴다
    kept, removed = _filter([_item("[포토] 이재용 회장 출근"), _item("[미국 특징주] 테슬라 급등")])

    assert kept == []
    assert [entry["removed_item"]["title"] for entry in removed] == [
        "[포토] 이재용 회장 출근",
        "[미국 특징주] 테슬라 급등",
    ]


def test_each_exclusion_rule_removes_on_its_own():
    # 점수 합산 없이 패턴 하나만 걸려도 탈락한다
    kept, removed = _filter(
        [
            _item("[ 표 ] 외국인 순매수 상위"),
            _item("[단독] 삼성전자, 신규 투자"),
            _item("SK하이닉스 HBM 시장 분석"),
        ]
    )

    assert kept == []
    assert [entry["excluded_by"] for entry in removed] == ["[ 표 ]", "단독", "분석"]


def _listed(title: str, listing: int) -> dict:
    # _title_listing 은 제목 기업 매치(match_title_companies)가 붙인다
    return {**_item(title), "_title_listing": listing}


def test_titles_listing_four_or_more_companies_are_filtered():
    kept, removed = _filter(
        [
            _listed("삼성전자, 하이브, 계양전기, 이노진 강세", 4),
            _listed("농심·동국제강·삼성화재·삼성생명·한화오션 '신고가'", 5),
        ]
    )

    assert kept == []
    assert [entry["excluded_by"] for entry in removed] == ["종목 나열 4개", "종목 나열 5개"]


def test_three_company_listing_with_content_is_kept():
    # 3개 나열은 다자 계약·협력 기사가 많다
    kept, removed = _filter(
        [
            _listed("SKT·카카오·KT, 10월 전국민 무료 '모두의 AI' 첫선", 3),
            _listed("마이크론, 퀄컴·현대모비스와 차량용 메모리 공급 장기계약", 3),
            _listed("현대차·기아, 서울시 자율주행 시내버스 구축", 2),
        ]
    )

    assert len(kept) == 3
    assert removed == []


def test_titles_that_are_nothing_but_a_listing_are_filtered():
    # 공시·시황 모음 기사의 제목. 사전에 없는 이름이 섞여도(_title_listing 이 작아도) 걸린다
    kept, removed = _filter(
        [
            _listed("삼성화재·삼성생명·LG 등", 3),
            _listed("일동제약·LS전선·현대모비스 등", 1),
            _listed("포스코인터내셔널ㆍ가온칩스 등", 2),
            _listed("SKT·LGU+·KT", 3),
            _listed("[공시] 한국콜마ㆍ한국타이어ㆍ코웨이 등", 3),
        ]
    )

    assert kept == []
    assert {entry["excluded_by"] for entry in removed} == {"나열뿐인 제목"}


def test_listing_followed_by_words_is_not_a_list_only_title():
    kept, removed = _filter(
        [
            _listed("삼성전기·LG이노텍 급등", 2),
            _listed("삼성전자 실적 발표 등", 1),
            _listed("KB금융·현대모비스·KT 등 코스피 38개사, 13일 자사주 매수", 3),
        ]
    )

    assert len(kept) == 3
    assert removed == []


def test_restore_truncated_title():
    from pipelines.news.utils.text_utils import restore_truncated_title

    page = "[단독] LG엔솔, 북미 ESS 대규모 수주 성공 | 머니투데이"
    assert restore_truncated_title("LG엔솔, 북미 ESS 대규모...", page) == (
        "LG엔솔, 북미 ESS 대규모 수주 성공"
    )
    assert (
        restore_truncated_title("LG엔솔, 북미 ESS 대규모…", page)
        == "LG엔솔, 북미 ESS 대규모 수주 성공"
    )
    # 꼬리를 지우면 앞부분이 안 맞으면 꼬리째 쓴다
    assert restore_truncated_title("A - B 합병...", "A - B 합병 완료") == "A - B 합병 완료"
    # 잘리지 않은 제목, 앞부분이 다른 페이지 제목, 말줄임이 본래 문구인 제목은 그대로다
    assert restore_truncated_title("삼성전자 실적 발표", page) == "삼성전자 실적 발표"
    assert restore_truncated_title("삼성전자 실적...", page) == "삼성전자 실적..."
    assert restore_truncated_title("삼성전자 결국...", "삼성전자 결국...") == "삼성전자 결국..."
    assert restore_truncated_title("...", page) == "..."
