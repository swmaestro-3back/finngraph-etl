-- 상장주식수·액면가·자본금 컬럼 승격 (반복 적용 안전)
--
-- 시가총액 = 종가 × 상장주식수 이므로 listed_shares가 밸류에이션 계산의 분모다.
-- master의 시가총액 필드는 전일 기준가 기반이라 당일 값과 어긋나므로 저장하지 않고 계산한다.
--
-- 단위 주의:
--   listed_shares — master 원본은 천주 단위. 파서에서 1000을 곱해 **주 단위로 정규화**해 넣는다
--                   (pipelines/stocks/extractors/kis_stock_master.py 의 LISTED_SHARES_UNIT).
--   par_value     — 원
--   capital       — 원. 액면가 × 발행주식수인 명목 자본금이며,
--                   자산 − 부채인 자기자본(company_financials.total_equity)과 다르다.
--                   ROE 분모로 쓰면 값이 수백 배로 나온다.
ALTER TABLE stocks ADD COLUMN IF NOT EXISTS listed_shares bigint;
ALTER TABLE stocks ADD COLUMN IF NOT EXISTS par_value     numeric;
ALTER TABLE stocks ADD COLUMN IF NOT EXISTS capital       bigint;
