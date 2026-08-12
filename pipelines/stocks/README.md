# Stocks Pipeline

국내 KOSPI/KOSDAQ 종목의 마스터와 시세를 수집합니다.

## Required Data

- KOSPI/KOSDAQ 활성 종목 목록과 상태 플래그(거래정지·관리종목·우선주·ETP·SPAC)
- 과거 일봉 OHLCV, 이후 일별 갱신
- 주봉·월봉

## Source Decision

| Source | Role | Decision |
| --- | --- | --- |
| 한국투자증권 Open API | REST 공식 API | 마스터·일별 시세·주월봉의 메인 소스 |
| FinanceDataReader | 공개 데이터 리더 | 과거 일봉 초기 백필, 상장폐지·관리종목 이력 보조 |
| 키움 API | 국내 주식 시세 API | Windows/OCX 운영 부담 때문에 제외 |
| pykrx | KRX 화면 스크래핑 | KRX가 로그인을 걸어 전 호출 실패. 제외 |

## Implementation

- 종목 마스터는 KIS master 파일에서 동기화합니다. **상장주식수는 천주 단위로 오므로
  ×1000 해서 주 단위로 저장합니다.** 그대로 쓰면 시가총액·EPS·BPS가 1,000배 어긋납니다.
- master의 시가총액 필드는 전일 기준가 기반이라 저장하지 않고 `종가 × 상장주식수`로
  계산합니다.
- 과거 일봉은 FinanceDataReader로 한 번에 백필하고, 이후는 KIS 기간별시세로 매일
  덧붙입니다. 같은 `(stock_id, trade_date)`는 upsert로 덮어써서 수정주가 반영도 같은
  경로로 처리됩니다.
- **주봉·월봉은 집계하지 않습니다.** `inquire-daily-itemchartprice`의
  `FID_PERIOD_DIV_CODE`에 `W`/`M`을 넣으면 KIS가 직접 줍니다.
- 시세 테이블의 키는 `symbol`이 아니라 `stock_id`입니다. 단축코드는 상장폐지 후
  재사용될 수 있지만 `stocks.id`는 종목 하나를 영구히 가리킵니다.
- 기간 조회는 한 번에 최대 100건만 옵니다. 더 긴 구간은 가장 오래된 날짜 직전으로 커서를
  옮겨 다시 부릅니다.

### 분봉(1·5분)은 아직 수집하지 않습니다

장중 5분 주기로 종목당 1회를 부르면 다른 종목별 조회 API와 KIS 호출 한도를 나눠 쓰게
되는데, 일봉·주월봉이 자리 잡기 전에는 한도 배분을 설계할 기준이 없습니다. 서비스
스펙에서의 우선순위도 높지 않습니다.

운영 시간대(장중)와 보관 정책(1분봉 60일, 5분봉 3년)이 일봉과 별개라 별도 작업으로
다룹니다. `stocks/intraday_1m`·`stocks/aggregate_candles` DAG와 `upsert_minute_candles`
로더는 그때까지 스텁으로 둡니다.
