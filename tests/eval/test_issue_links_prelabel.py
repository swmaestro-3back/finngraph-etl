"""이슈 연결 LLM 사전 라벨의 입력·재개·사람 라벨 보존 단위 테스트. Bedrock 은 부르지 않는다."""

from __future__ import annotations

import json

from linkeval import read_csv, read_jsonl, write_csv, write_jsonl
from prelabel import LABEL_LOG_NAME, PairLabel, build_pair_input, load_guideline, run_prelabel


def _record(pair_id: str, a_title: str = "미국 7월 건설지출", b_title: str = "미국 8월 건설지출"):
    return {
        "pair_id": pair_id,
        "band": "0.8-1.0",
        "band_population": 12,
        "score": 0.91,
        "a_id": 1,
        "a_date": "2026-09-01",
        "a_title": a_title,
        "a_summary": "",
        "a_member_titles": ["美 7월 건설지출 0.1% 감소"],
        "b_id": 2,
        "b_date": "2026-10-01",
        "b_title": b_title,
        "b_summary": "미국 8월 건설지출이 전월 대비 0.9% 늘었어요.",
        "b_member_titles": ["美 8월 건설지출 0.9% 증가", "건설지출 반등"],
        "shared_companies": [],
        "llm_label": "",
        "llm_reason": "",
        "human_label": "",
        "human_note": "",
        "a_lead": "",
        "b_lead": "미국 상무부는 1일 ...",
        "gap_days": 30.0,
        "a_original_size": 4,
        "b_original_size": 6,
    }


def test_guideline_prompt_section_has_tie_break_rules_only():
    prompt = load_guideline()

    assert "같은 기업이라는 이유만으로는 SAME_STORY 가 아니다" in prompt
    assert "주가 반응만 같은 것은 DIFFERENT" in prompt
    assert "시리즈 지표의 다음 발표는 SAME_STORY" in prompt
    assert "사람 검수 방법" not in prompt
    assert "prompt:start" not in prompt


def test_build_pair_input_hides_score_and_includes_context():
    text = build_pair_input(_record("1_2"))

    assert "0.91" not in text
    assert "[이슈 A] 2026-09-01 · 기사 4건" in text
    assert "요약: (없음)" in text
    assert "- 美 8월 건설지출 0.9% 증가" in text
    assert "대표 기사 리드: 미국 상무부는 1일 ..." in text
    assert "공통 기업: (없음)" in text
    assert "B 는 A 보다 30일 뒤에 시작했다." in text


def test_run_prelabel_resumes_and_keeps_human_labels(tmp_path):
    records = [_record("1_2"), _record("3_4", b_title="실패할 쌍"), _record("5_6")]
    write_jsonl(tmp_path / "pairs.jsonl", records)
    # 검수자가 이미 CSV 에 사람 라벨을 적어 둔 상태
    reviewed = [dict(r) for r in records]
    reviewed[2]["human_label"] = "S"
    reviewed[2]["human_note"] = "다음 달 발표"
    write_csv(tmp_path / "pairs.csv", reviewed)

    asked: list[str] = []
    fail = {"on": True}

    async def fake_labeler(text: str) -> PairLabel:
        asked.append(text)
        if "실패할 쌍" in text and fail["on"]:
            raise RuntimeError("throttled")
        return PairLabel(label="SAME_STORY", reason="같은 지표의\n다음 발표")

    summary = run_prelabel(tmp_path, lambda: fake_labeler, max_concurrency=2)

    assert summary["labeled_now"] == 2
    assert summary["failed"] == 1
    assert summary["remaining"] == 1
    assert summary["human_kept"] == 1
    log_lines = (tmp_path / LABEL_LOG_NAME).read_text(encoding="utf-8").splitlines()
    assert {json.loads(line)["pair_id"] for line in log_lines} == {"1_2", "5_6"}

    rows = {row["pair_id"]: row for row in read_csv(tmp_path / "pairs.csv")}
    assert rows["1_2"]["llm_label"] == "SAME_STORY"
    assert rows["1_2"]["llm_reason"] == "같은 지표의 다음 발표"
    assert rows["5_6"]["human_label"] == "S"
    assert rows["5_6"]["human_note"] == "다음 달 발표"
    assert rows["3_4"]["llm_label"] == ""

    # 재실행: 실패했던 쌍만 다시 묻는다.
    asked.clear()
    fail["on"] = False
    summary = run_prelabel(tmp_path, lambda: fake_labeler)
    assert len(asked) == 1 and "실패할 쌍" in asked[0]
    assert summary["remaining"] == 0
    assert all(r["llm_label"] == "SAME_STORY" for r in read_jsonl(tmp_path / "pairs.jsonl"))
    assert {r["pair_id"]: r["human_label"] for r in read_jsonl(tmp_path / "pairs.jsonl")}[
        "5_6"
    ] == "S"

    # 더 물을 게 없으면 labeler 를 만들지도 않는다.
    def must_not_build():
        raise AssertionError("labeler 생성 금지")

    assert run_prelabel(tmp_path, must_not_build)["labeled_now"] == 0
