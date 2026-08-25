"""서비스 대상 적재."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

# theme_stocks 에 편입된 종목의 법인을 수집 대상으로 올린다.
#
# 편입만 하고 제외는 하지 않는다. 크롤링 테마는 하루 만에 구성이 바뀌는데, 그때마다
# 수집을 끊으면 시계열에 구멍이 생기고 테마 지수도 이어지지 않는다. 빼는 것은 사람이 정한다.
#
# 종류는 여기서 판정하지 않는다. 수집 경계(kis_stock_master.is_collectible)가
# 보통주만 들이므로 stocks 에 있는 것은 이미 다룰 종목이다.
SYNC_SERVICE_COMPANIES_SQL = text(
    """
    INSERT INTO service_companies (company_id, name)
    SELECT DISTINCT s.company_id, c.name
      FROM theme_stocks AS ts
      JOIN stocks AS s ON s.id = ts.stock_id
                      AND s.is_active
      JOIN companies AS c ON c.id = s.company_id
    ON CONFLICT (company_id) DO NOTHING
    """
)


def sync_service_companies(session: Session) -> int:
    """테마 편입 종목의 법인을 수집 대상에 더한다. 새로 들어온 수를 반환한다."""

    result = session.execute(SYNC_SERVICE_COMPANIES_SQL)
    return result.rowcount or 0


def count_service_companies(session: Session) -> int:
    """수집 대상 법인 수."""

    return session.execute(text("SELECT count(*) FROM service_companies")).scalar() or 0
