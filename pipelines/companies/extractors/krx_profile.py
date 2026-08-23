"""KRX 상장법인 기업개요 수집.

FinanceDataReader의 `KRX-DESC` 목록이다. KIND(상장회사 목록)에서 오는 자료로,
한 번 호출에 2,800여 행이 오고 종목당 API가 아니라 비용이 사실상 없다.

쓰는 값은 둘이다.
- `Industry` → `companies.industry_name`. 업종명은 화면에도 쓰이고 설명 폴백의 재료다.
- `Products` → 설명 폴백 문장의 "주요 제품·서비스".

DART 기업개황(company.json)에는 업종 **코드**(induty_code)만 있고 이름이 없다. 코드→이름
표를 따로 들고 있지 않으므로 업종명은 이쪽에서 받는다.
"""

from __future__ import annotations

from pipelines.common.logging import get_logger
from pipelines.common.utils.retry import retry_external_call

logger = get_logger(__name__)

LISTING_NAME = "KRX-DESC"


@retry_external_call()
def fetch_krx_profiles() -> dict[str, tuple[str | None, str | None]]:
    """단축코드 → (업종명, 주요제품).

    Returns:
        dict[str, tuple[str | None, str | None]]: 종목코드를 키로 한 매핑.
            조회에 실패하면 예외가 아니라 빈 dict를 돌려준다 — 설명 생성 job이
            이것 때문에 통째로 멈출 이유는 없다.
    """

    try:
        import FinanceDataReader as fdr

        frame = fdr.StockListing(LISTING_NAME)
    except Exception:
        logger.exception("KRX 기업개요 조회 실패")
        return {}

    if frame is None or frame.empty:
        return {}

    columns = {str(name).strip().lower(): name for name in frame.columns}
    code_column = columns.get("code")
    if code_column is None:
        logger.warning("KRX 기업개요 응답에 Code 컬럼이 없다: %s", list(frame.columns))
        return {}

    industry_column = columns.get("industry")
    products_column = columns.get("products")

    profiles: dict[str, tuple[str | None, str | None]] = {}
    for row in frame.to_dict("records"):
        code = str(row.get(code_column) or "").strip()
        if not code:
            continue
        profiles[code] = (
            _clean(row.get(industry_column)) if industry_column else None,
            _clean(row.get(products_column)) if products_column else None,
        )

    logger.info("KRX 기업개요 수집: %d종목", len(profiles))
    return profiles


def _clean(value: object) -> str | None:
    text = str(value or "").strip()
    if not text or text.lower() == "nan":
        return None
    return text
