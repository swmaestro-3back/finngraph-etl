"""KIS 재무 응답 정규화.

원본을 그대로 넣으면 세 가지가 조용히 틀린다.

1. **단위.** KIS 재무제표 금액은 억원이다. DART는 원으로 준다. 두 원천을 한 화면에서
   비교하려면 적재 전에 원으로 맞춰야 한다.
2. **`99.99` sentinel.** 값이 없다는 뜻이지 실제 수치가 아니다. 그대로 저장하면
   "영업이익 99.99억"이라는 그럴듯한 오염이 남는다.
3. **연간 시리즈에 섞인 최신 분기.** FID_DIV_CLS_CODE=0으로 부르면 첫 행이 결산월이
   아닌 최근 분기다(2026-08 기준 202603). 이걸 연간으로 저장하면 "2026년 연간 실적"이
   1분기 값으로 잡힌다.
"""

from __future__ import annotations

from collections import Counter
from decimal import Decimal, InvalidOperation
from typing import Any

from pipelines.companies.models import CompanyFinancial

# KIS 재무제표 금액 단위(억원) → 원.
KIS_AMOUNT_UNIT = 100_000_000

# "값 없음"을 뜻하는 sentinel. 실제 값이 정확히 99.99인 경우와 구분할 수 없지만,
# 금액 필드에서 99.99억이 정확히 나올 확률보다 sentinel일 확률이 압도적으로 높다.
SENTINEL = Decimal("99.99")

# 주당 지표의 현실적 상한. 넘으면 원천 오염으로 본다.
#
# KIS 과거 재무에 비현실적인 값이 섞여 온다. 남광토건 2010년 EPS 가 -143,841,822 원
# (주당 -1.4억원), BPS 가 74,358,429 원이다. 워크아웃·감자를 겪은 건설사에서 나타나며
# 2016년 이후는 정상이다. **KIS 원본이 그렇다** — 파싱은 정확하다.
#
# 실측 837행 중 17행 / 7법인(0.13%)이 걸린다. 국내 상장사의 EPS 가 100만원을 넘거나
# BPS 가 1,000만원을 넘는 경우는 없다.
EPS_LIMIT = Decimal("1000000")
BPS_LIMIT = Decimal("10000000")

# period_type 값.
#
# **`Q`가 분기 개별값이다.** KIS가 주는 원본은 연초부터의 누적이라 `QC`로 따로 담는다.
# 이름이 읽히는 대로의 값을 갖게 해야 소비하는 쪽이 실수하지 않는다 — 예전에는 `Q`가
# 누적이라 밸류에이션이 1분기 EPS를 1년치로 나눠 PER이 최대 9배 부풀었다.
PERIOD_ANNUAL = "A"
PERIOD_QUARTER_CUMULATIVE = "QC"
PERIOD_QUARTER = "Q"

# 차분해야 하는 기간(flow) 항목.
#
# 잔액(stock) 항목과 섞으면 안 된다. 자산·부채·자본과 BPS는 "그 시점의 잔액"이라
# 누적 개념이 없다. 차감하면 분기 중 증감액이 되어 자산총계 자리에 엉뚱한 값이 들어간다.
FLOW_FIELDS = ("revenue", "operating_income", "net_income", "eps")


def build_kis_financials(
    company_id: int,
    period_type: str,
    balance_rows: list[dict[str, Any]],
    income_rows: list[dict[str, Any]],
    ratio_rows: list[dict[str, Any]],
) -> list[CompanyFinancial]:
    """세 응답을 결산기준월로 합쳐 적재 단위로 만든다.

    Args:
        company_id (int): 대상 법인 id.
        period_type (str): 'A'(연간) 또는 'Q'(분기 누적).
        balance_rows (list[dict]): 대차대조표 원본 행.
        income_rows (list[dict]): 손익계산서 원본 행.
        ratio_rows (list[dict]): 재무비율 원본 행.

    Returns:
        list[CompanyFinancial]: fiscal_yymm 오름차순.
    """

    balances = _by_fiscal_yymm(balance_rows)
    incomes = _by_fiscal_yymm(income_rows)
    ratios = _by_fiscal_yymm(ratio_rows)

    fiscal_yymms = set(balances) | set(incomes) | set(ratios)
    if period_type == "A":
        fiscal_yymms = _keep_fiscal_year_ends(fiscal_yymms)

    financials = [
        CompanyFinancial(
            company_id=company_id,
            source="KIS",
            fs_div="CFS",  # KIS는 연결만 준다. fid_fncl_cls_code를 넣어도 무시된다.
            fiscal_yymm=fiscal_yymm,
            period_type=period_type,
            revenue=_amount(incomes.get(fiscal_yymm, {}).get("sale_account")),
            # 영업이익은 bsop_prti다. op_prfi는 경상이익(영업외손익까지 반영)이라 다르다.
            operating_income=_amount(incomes.get(fiscal_yymm, {}).get("bsop_prti")),
            net_income=_amount(incomes.get(fiscal_yymm, {}).get("thtr_ntin")),
            total_assets=_amount(balances.get(fiscal_yymm, {}).get("total_aset")),
            total_liabilities=_amount(balances.get(fiscal_yymm, {}).get("total_lblt")),
            total_equity=_amount(balances.get(fiscal_yymm, {}).get("total_cptl")),
            roe=_reported_roe(ratios.get(fiscal_yymm, {}).get("roe_val")),
            eps=_sane_per_share(_ratio(ratios.get(fiscal_yymm, {}).get("eps")), EPS_LIMIT),
            bps=_sane_per_share(_ratio(ratios.get(fiscal_yymm, {}).get("bps")), BPS_LIMIT),
        )
        for fiscal_yymm in sorted(fiscal_yymms)
    ]

    # 세 응답 모두 값이 없는 기간은 만들지 않는다.
    return [financial for financial in financials if _has_any_value(financial)]


def infer_fiscal_month(annual: list[CompanyFinancial]) -> str | None:
    """연간 시리즈에서 결산월을 추론한다.

    **분기 시리즈로 추론하면 틀린다.** 분기는 결산월 기준 3·6·9·12개월째에 고르게 찍히는데,
    시리즈가 시작·끝나는 위치에 따라 특정 월이 하나 더 많아진다. 카카오(12월 결산)는
    2018-03에 시작해 2026-03에 끝나서 `03`이 8회로 최빈이 되고 3월 결산으로 오판된다.

    연간 시리즈는 결산월에만 행이 생기므로 안전하다(카카오 A는 12월 22회).

    Args:
        annual (list[CompanyFinancial]): 연간 재무 목록.

    Returns:
        str | None: 'MM' 형태의 결산월. 판단할 근거가 없으면 None.
    """

    months = Counter(f.fiscal_yymm[4:6] for f in annual if len(f.fiscal_yymm) == 6)
    return months.most_common(1)[0][0] if months else None


def build_quarterly_deltas(
    cumulative: list[CompanyFinancial],
    fiscal_month: str | None,
) -> list[CompanyFinancial]:
    """분기 누적을 분기 개별값으로 바꾼다.

    KIS 분기 재무는 연초부터의 누적이다. 유한양행 2025년 매출이
    4,916억 → 10,706억 → 16,406억 → 21,866억으로 커지다 새 회계연도에 5,268억으로 떨어진다.

    회계연도 첫 분기는 누적 = 개별이므로 그대로 두고, 나머지는 3개월 전 누적을 뺀다.
    직전 분기 행이 없으면 **그 분기를 만들지 않는다** — 틀린 값보다 없는 값이 낫다.

    잔액 항목(자산·부채·자본·BPS)은 차감하지 않고 복사한다. ROE는 차감이 성립하지 않아
    비운다. 필요하면 순이익 ÷ 자본으로 다시 만들면 된다.

    Args:
        cumulative (list[CompanyFinancial]): period_type이 QC인 누적 분기 목록.
        fiscal_month (str | None): 'MM' 결산월. 없으면 12월 결산으로 본다.

    Returns:
        list[CompanyFinancial]: period_type이 Q인 분기 개별 목록. fiscal_yymm 오름차순.
    """

    fye = fiscal_month if fiscal_month and len(fiscal_month) == 2 else "12"
    # 회계연도 첫 분기는 결산월 3개월 뒤다. 12월 결산이면 03, 3월 결산이면 06,
    # 11월 결산이면 02다. 실제로 02/05/08/11 시리즈를 쓰는 법인이 있어 상수로 못 박는다.
    first_quarter_month = f"{(int(fye) + 2) % 12 + 1:02d}"

    by_yymm = {f.fiscal_yymm: f for f in cumulative}
    deltas: list[CompanyFinancial] = []

    for fiscal_yymm in sorted(by_yymm):
        current = by_yymm[fiscal_yymm]

        if fiscal_yymm[4:6] == first_quarter_month:
            deltas.append(_as_quarter(current, current))
            continue

        previous = by_yymm.get(_shift_months(fiscal_yymm, -3))
        if previous is None:
            continue

        deltas.append(_as_quarter(current, current, previous))

    return [d for d in deltas if _has_any_value(d)]


def _as_quarter(
    base: CompanyFinancial,
    current: CompanyFinancial,
    previous: CompanyFinancial | None = None,
) -> CompanyFinancial:
    """분기 개별 행을 만든다. previous가 있으면 flow 항목을 차감한다."""

    values = {field: getattr(current, field) for field in FLOW_FIELDS}
    if previous is not None:
        for field in FLOW_FIELDS:
            now, before = getattr(current, field), getattr(previous, field)
            values[field] = None if now is None or before is None else now - before

    return CompanyFinancial(
        company_id=base.company_id,
        source=base.source,
        fiscal_yymm=base.fiscal_yymm,
        period_type=PERIOD_QUARTER,
        fs_div=base.fs_div,
        disclosed_at=base.disclosed_at,
        rcept_no=base.rcept_no,
        # 잔액 항목은 그대로 — 시점 값이라 차감하면 뜻이 달라진다.
        total_assets=base.total_assets,
        total_liabilities=base.total_liabilities,
        total_equity=base.total_equity,
        bps=base.bps,
        # 비율은 차감이 성립하지 않는다.
        roe=None,
        **values,
    )


def _shift_months(fiscal_yymm: str, months: int) -> str:
    year, month = int(fiscal_yymm[:4]), int(fiscal_yymm[4:6])
    total = year * 12 + (month - 1) + months
    return f"{total // 12:04d}{total % 12 + 1:02d}"


def _by_fiscal_yymm(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        fiscal_yymm = str(row.get("stac_yymm") or "").strip()
        if len(fiscal_yymm) == 6 and fiscal_yymm.isdigit():
            indexed.setdefault(fiscal_yymm, row)
    return indexed


def _keep_fiscal_year_ends(fiscal_yymms: set[str]) -> set[str]:
    """연간 시리즈에서 결산월 행만 남긴다.

    결산월은 시리즈에서 가장 많이 등장하는 월로 판정한다. 12월 결산이 대부분이지만
    3월·6월 결산 법인도 있어 상수로 박으면 그 법인의 연간 데이터가 통째로 사라진다.
    """

    if len(fiscal_yymms) < 2:
        return fiscal_yymms

    months = Counter(fiscal_yymm[4:6] for fiscal_yymm in fiscal_yymms)
    fiscal_month, _ = months.most_common(1)[0]
    return {fiscal_yymm for fiscal_yymm in fiscal_yymms if fiscal_yymm[4:6] == fiscal_month}


def _has_any_value(financial: CompanyFinancial) -> bool:
    """적재할 값이 있는 행인지. 껍데기는 여기서 걸러진다.

    **KIS 는 보유하지 않는 기간에도 행을 만들어 준다.** 모든 필드가 `"0.00"` 으로 채워져
    오는데, 99.99 sentinel 과 달리 파싱하면 유효한 0 이라 그냥 두면 적재된다.

        주성엔지니어링  2004~2008   매출·자산·자본·EPS 전부 0.00   (API 소급 한계)
        에코프로비엠    2016~2019   〃                            (상장 전후)

    두 경우 모두 **DART 에도 없다** — fnlttSinglAcntAll 은 2015년 이후만 주고,
    에코프로비엠은 상장 첫해(2019)조차 조회되지 않는다. 폴백으로 메울 수 없다.

    `0` 을 그대로 저장하면 화면에 "그 해 매출 0원"으로 찍히는데 사실은 "모른다"다.
    둘은 다른 정보라 행을 만들지 않는다.

    **판정 기준은 자산총계다.** 자산이 0인 상장사는 존재할 수 없다. 적자 기업의 영업이익
    0이나 무차입 기업의 부채 0은 개별 필드만 0이라 여기 걸리지 않는다.
    """

    if financial.total_assets == 0:
        return False

    return any(
        value is not None
        for value in (
            financial.revenue,
            financial.operating_income,
            financial.net_income,
            financial.total_assets,
            financial.total_liabilities,
            financial.total_equity,
            financial.roe,
            financial.eps,
            financial.bps,
        )
    )


def _sane_per_share(value: Decimal | None, limit: Decimal) -> Decimal | None:
    """주당 지표의 이상값을 걷어낸다. 상한을 넘으면 원천 오염이라 비운다."""

    return None if value is not None and abs(value) > limit else value


def _decimal(value: object) -> Decimal | None:
    text = str(value or "").strip().replace(",", "")
    if not text:
        return None
    try:
        parsed = Decimal(text)
    except InvalidOperation:
        return None
    return None if parsed == SENTINEL else parsed


def _amount(value: object) -> int | None:
    """억원 단위 금액을 원으로 바꾼다."""

    parsed = _decimal(value)
    return None if parsed is None else int(parsed * KIS_AMOUNT_UNIT)


def _ratio(value: object) -> Decimal | None:
    """비율·주당 지표. 단위 변환 없이 그대로 쓴다."""

    return _decimal(value)


def _reported_roe(value: object) -> Decimal | None:
    """KIS가 준 ROE. **0은 "값 없음"으로 본다.**

    과거 구간에서 순이익·자본·EPS 는 멀쩡한데 roe_val 만 0.00 으로 오는 행이 있다.

        SK하이닉스 2004   순이익 17,213억 · 자본 44,836억 · EPS 3,871 → roe_val 0.00
        DB하이텍   2008   순이익 -2,993억 · 자본  8,443억            → roe_val 0.00

    SK하이닉스 2004 는 실제로 38% 수준이라 0 일 수 없다. 전 필드가 0 인 껍데기 행과 달리
    다른 값이 살아 있어 자산 기준 필터에 걸리지 않으므로 여기서 따로 비운다.

    **직접 계산해 채우지 않는다.** KIS 값과 산식이 다르다 — 순이익 ÷ 기말자본으로 역산하면
    SK하이닉스 2025 가 35.59 인데 KIS 는 44.15 를 준다. 분모를 평균자본으로 쓰는 것으로
    보이나 문서화돼 있지 않다(task.md 9절 3번). 정의가 다른 값을 한 컬럼에 섞으면 기간 간
    비교가 깨지므로, 정의를 확정하기 전까지는 비워 둔다.

    순이익이 정확히 0원인 상장사는 사실상 없어 실제 0 과 혼동될 여지는 없다.
    """

    parsed = _ratio(value)
    return None if parsed == 0 else parsed
