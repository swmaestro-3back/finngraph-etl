-- 뉴스 요약 핵심 포인트 ───────────────────────────────────────────────────────
-- 요약 문단(news.summary) 아래 붙는 항목별 한 줄. summarize_articles 가 문단과 함께 채운다.
-- 형식: [{"kind": "CHANGE", "text": "…"}] — 고정된 표시 순서로 2~3개, 사건이 없는 기사는 [].
-- kind 는 CHANGE | AFFECTED | SCALE | CAUSE | RIPPLE 이고 화면 라벨은 키로 매핑한다.
-- NULL 은 아직 새 형식으로 요약하지 않은 기사다(요약 대상).
ALTER TABLE news ADD COLUMN IF NOT EXISTS summary_points JSONB;
