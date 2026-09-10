from __future__ import annotations


def test_merge_reason_joins_both_sentences() -> None:
    from pipelines.themes.transformers.merger import _merge_reason

    assert _merge_reason("배터리 셀 생산", "전기차용 중대형 배터리 매출 비중") == (
        "배터리 셀 생산\n전기차용 중대형 배터리 매출 비중"
    )


def test_merge_reason_takes_the_only_available_side() -> None:
    from pipelines.themes.transformers.merger import _merge_reason

    assert _merge_reason(None, "양극재 공급") == "양극재 공급"
    assert _merge_reason("양극재 공급", None) == "양극재 공급"
    assert _merge_reason("  ", "양극재 공급") == "양극재 공급"


def test_merge_reason_without_any_text_is_none() -> None:
    from pipelines.themes.transformers.merger import _merge_reason

    assert _merge_reason(None, None) is None
    assert _merge_reason("  ", None) is None


def test_merge_batch_unions_stocks_and_merges_shared_reasons() -> None:
    from pipelines.themes.models import Company, Theme
    from pipelines.themes.transformers.merger import merge_batch

    kept = Theme(
        name="2차전지",
        source="judal",
        companies=[
            Company(name="삼성SDI", ticker="006400", reason="배터리 셀 생산"),
            Company(name="LG에너지솔루션", ticker="373220", reason=None),
        ],
    )
    # 정규화하면 이름이 같아 중복으로 걸린다
    duplicate = Theme(
        name="2차 전지",
        source="naver",
        companies=[
            Company(name="삼성SDI", ticker="006400", reason="전기차용 중대형 배터리 매출 비중"),
            Company(name="LG에너지솔루션", ticker="373220", reason="북미 합작 공장 증설"),
            Company(name="에코프로비엠", ticker="247540", reason="양극재 공급"),
        ],
    )

    [result] = merge_batch([kept, duplicate])

    assert result is kept
    reasons = {c.ticker: c.reason for c in result.companies}

    # 양쪽에 다 있는 종목은 두 문장이 이어붙는다
    assert reasons["006400"] == "배터리 셀 생산\n전기차용 중대형 배터리 매출 비중"
    # 채택된 쪽에 사유가 없었으면 중복 테마의 문장으로 채워진다
    assert reasons["373220"] == "북미 합작 공장 증설"
    # 중복 테마에만 있던 종목도 합집합으로 편입되고, 그쪽 사유를 그대로 가져온다
    assert reasons["247540"] == "양극재 공급"
    assert [c.ticker for c in result.companies] == ["006400", "373220", "247540"]


def test_merge_batch_keeps_first_source_order() -> None:
    from pipelines.themes.models import Company, Theme
    from pipelines.themes.transformers.merger import merge_batch

    first = Theme(
        name="반도체", source="judal", companies=[Company(name="삼성전자", ticker="005930")]
    )
    second = Theme(
        name="반도체", source="naver", companies=[Company(name="삼성전자", ticker="005930")]
    )

    [result] = merge_batch([first, second])

    # SOURCES 순서상 먼저 온 테마가 남는다
    assert result.source == "judal"
    # 버려진 테마의 소스는 채택된 테마에 쌓인다
    assert result.sources == ["judal", "naver"]


def test_merge_batch_does_not_repeat_the_same_source() -> None:
    from pipelines.themes.models import Company, Theme
    from pipelines.themes.transformers.merger import merge_batch

    stock = [Company(name="삼성전자", ticker="005930")]
    themes = [
        Theme(name="반도체", source="judal", companies=stock),
        Theme(name="반 도체", source="naver", companies=stock),
        Theme(name="반도체", source="naver", companies=stock),
    ]

    [result] = merge_batch(themes)

    assert result.sources == ["judal", "naver"]


def test_merge_existing_renames_theme_to_db_name_when_normalized_name_matches() -> None:
    from pipelines.themes.models import Company, Theme
    from pipelines.themes.transformers.merger import merge_existing

    crawled = Theme(
        name="2차 전지", source="naver", companies=[Company(name="삼성SDI", ticker="006400")]
    )
    existing = {"2차전지": {"006400"}}

    [result] = merge_existing([crawled], existing)

    # 로더가 기존 행·노드에 붙을 수 있도록 DB 쪽 이름으로 맞춘다
    assert result.name == "2차전지"
    assert [c.ticker for c in result.companies] == ["006400"]


def test_merge_existing_renames_contained_name_with_heavy_stock_overlap() -> None:
    from pipelines.themes.models import Company, Theme
    from pipelines.themes.transformers.merger import merge_existing

    tickers = ["005930", "000660", "035420", "035720", "051910"]
    crawled = Theme(
        name="반도체 소재",
        source="naver",
        companies=[Company(name=f"종목{t}", ticker=t) for t in tickers + ["247540"]],
    )
    # 배치 내 중복 판정과 같은 규칙: 이름 포함 관계 + 종목 90% 이상 겹침
    existing = {"반도체소재": set(tickers)}

    [result] = merge_existing([crawled], existing)

    assert result.name == "반도체소재"
    # 종목은 그대로 통과한다. 기존에 없던 종목은 로더가 추가한다
    assert "247540" in {c.ticker for c in result.companies}


def test_merge_existing_keeps_new_theme_untouched() -> None:
    from pipelines.themes.models import Company, Theme
    from pipelines.themes.transformers.merger import merge_existing

    new = Theme(
        name="우주항공", source="judal", companies=[Company(name="한화에어로", ticker="012450")]
    )
    existing = {"반도체": {"005930"}}

    assert merge_existing([new], existing) == [new]
    assert new.name == "우주항공"


def test_merge_existing_merges_two_themes_that_map_to_the_same_db_theme() -> None:
    from pipelines.themes.models import Company, Theme
    from pipelines.themes.transformers.merger import merge_existing

    first = Theme(
        name="2차 전지", source="judal", companies=[Company(name="삼성SDI", ticker="006400")]
    )
    second = Theme(
        name="2차전지", source="naver", companies=[Company(name="에코프로비엠", ticker="247540")]
    )
    existing = {"2차전지": {"006400"}}

    [result] = merge_existing([first, second], existing)

    assert result.name == "2차전지"
    assert [c.ticker for c in result.companies] == ["006400", "247540"]
    assert result.sources == ["judal", "naver"]


def test_merge_existing_passes_everything_through_when_db_is_empty() -> None:
    from pipelines.themes.models import Company, Theme
    from pipelines.themes.transformers.merger import merge_existing

    themes = [
        Theme(name="반도체", source="judal", companies=[Company(name="삼성전자", ticker="005930")]),
        Theme(name="2차전지", source="naver", companies=[Company(name="삼성SDI", ticker="006400")]),
    ]

    assert merge_existing(themes, {}) == themes
