# Stocks Pipeline

국내 KOSPI/KOSDAQ 주가 데이터를 수집합니다.

## Required Data

- KOSPI/KOSDAQ 활성 종목 목록
- 과거 일봉 OHLCV
- 오늘 이후 장중 1분봉 OHLCV
- 서비스 제공용 5분봉, 일봉, 주봉, 월봉
- 거래량

## Source Decision

| Source | Role | Decision |
| --- | --- | --- |
| 키움 API | 국내 주식 시세 API | Windows/OCX 운영 부담 때문에 초기 백엔드 수집기에서는 제외 |
| 한국투자증권 Open API | REST/WebSocket 공식 API | 오늘 이후 장중 1분봉 수집의 메인 소스 |
| FinanceDataReader | 공개 데이터 리더/크롤링 기반 라이브러리 | 과거 일봉 초기 적재와 보조 검증용 |

## Implementation

- FinanceDataReader로 과거 일봉을 적재합니다.
- 한국투자 API로 오늘 이후 1분봉을 누적합니다.
- 5분봉은 1분봉을 집계해 생성합니다.
- 주봉/월봉은 일봉을 집계해 생성하거나 캐시합니다.
- 전 종목을 1분 안에 수집하는 목표는 버리고 순환 큐로 수집합니다.
- 사용자-facing 차트는 최대 5분 지연을 허용합니다.

