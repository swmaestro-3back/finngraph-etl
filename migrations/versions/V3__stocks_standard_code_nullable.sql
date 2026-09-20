-- US 종목은 표준코드가 없다(위키피디아 인덱스 원천). stocks.standard_code를 NULL 허용으로 바꾼다.
-- KR 종목은 계속 KIS 표준코드를 채운다. 유니크 인덱스는 NULL을 서로 다른 값으로 보므로 그대로 둔다.
ALTER TABLE stocks ALTER COLUMN standard_code DROP NOT NULL;
