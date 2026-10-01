"""dump_candidates 가 뽑은 클러스터 쌍에 LLM 사전 라벨(SAME_EVENT/SAME_STORY/DIFFERENT)을 단다.

GUIDELINE.md 의 prompt 구간을 시스템 프롬프트로 써서 사람 검수자와 같은 기준으로 판정한다.
코사인 점수는 입력에 넣지 않는다 — 라벨이 점수를 따라가면 임계값 평가가 순환 논증이 된다.

결과는 llm_labels.jsonl 에 한 쌍씩 바로 덧붙이므로 중간에 끊겨도 다시 돌리면 남은 쌍만 묻는다.
끝나면 pairs.jsonl · pairs.csv 의 llm_label/llm_reason 을 채우되, pairs.csv 에 이미 적힌
human_label/human_note 는 보존한다.

실행: .venv/bin/python scripts/eval/issue_links/prelabel.py --limit 10
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from collections import Counter
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Literal

from linkeval import (
    DEFAULT_OUT_DIR,
    LIST_SEPARATOR,
    collapse,
    normalize_label,
    read_csv,
    read_jsonl,
    write_csv,
    write_jsonl,
)
from pydantic import BaseModel, Field

log = logging.getLogger("issue_links.prelabel")

GUIDELINE_PATH = Path(__file__).with_name("GUIDELINE.md")
PROMPT_START = "<!-- prompt:start -->"
PROMPT_END = "<!-- prompt:end -->"
LABEL_LOG_NAME = "llm_labels.jsonl"

DEFAULT_MAX_TOKENS = 256
RETRY_ATTEMPTS = 2
# 회사가 수십 개 붙은 클러스터도 있어 입력이 불어나지 않게 자른다.
MAX_COMPANY_NAMES = 8


class PairLabel(BaseModel):
    label: Literal["SAME_EVENT", "SAME_STORY", "DIFFERENT"] = Field(
        description="두 이슈의 관계. 가이드의 [라벨] 정의와 [판정 규칙]을 따른다."
    )
    reason: str = Field(description="판정 이유. 한국어 한 줄로 두 사건을 잇는 고리나 다른 점.")


Labeler = Callable[[str], Awaitable[PairLabel]]


def load_guideline(path: Path = GUIDELINE_PATH) -> str:
    text = path.read_text(encoding="utf-8")
    start, end = text.find(PROMPT_START), text.find(PROMPT_END)
    if start < 0 or end < start:
        raise ValueError(f"{path}: {PROMPT_START} ~ {PROMPT_END} 구간이 없다")
    return text[start + len(PROMPT_START) : end].strip()


def _as_list(value: Any) -> list[str]:
    # CSV 에서 되읽은 레코드면 목록이 구분자로 이어진 문자열이다.
    if isinstance(value, str):
        return [v for v in value.split(LIST_SEPARATOR) if v]
    return list(value or [])


def _cluster_block(record: dict[str, Any], side: str) -> str:
    size = record.get(f"{side}_original_size")
    header = f"[이슈 {side.upper()}] {record.get(f'{side}_date', '')}"
    lines = [header + (f" · 기사 {size}건" if size else "")]
    lines.append(f"이름: {record.get(f'{side}_title', '')}")
    lines.append(f"요약: {record.get(f'{side}_summary') or '(없음)'}")
    titles = _as_list(record.get(f"{side}_member_titles"))
    if titles:
        lines.append("최근 기사 제목:")
        lines.extend(f"- {title}" for title in titles)
    if record.get(f"{side}_lead"):
        lines.append(f"대표 기사 리드: {record[f'{side}_lead']}")
    companies = _as_list(record.get(f"{side}_companies"))
    if companies:
        extra = len(companies) - MAX_COMPANY_NAMES
        more = f" 외 {extra}곳" if extra > 0 else ""
        lines.append(f"관련 기업: {', '.join(companies[:MAX_COMPANY_NAMES])}{more}")
    return "\n".join(lines)


def build_pair_input(record: dict[str, Any]) -> str:
    shared = _as_list(record.get("shared_companies"))
    footer = [f"공통 기업: {', '.join(shared) if shared else '(없음)'}"]
    gap = record.get("gap_days")
    if gap is not None:
        footer.append(f"B 는 A 보다 {float(gap):g}일 뒤에 시작했다.")
    return "\n\n".join(
        [_cluster_block(record, "a"), _cluster_block(record, "b"), "\n".join(footer)]
    )


class PairLabeler:
    """Bedrock 구조화 출력 체인. news 의 cluster_titler · relevance_filter 와 같은 구성."""

    def __init__(self, max_tokens: int = DEFAULT_MAX_TOKENS):
        from langchain_aws import ChatBedrockConverse
        from langchain_core.messages import SystemMessage
        from langchain_core.prompts import ChatPromptTemplate

        from pipelines.common.clients.bedrock import ensure_bedrock_token
        from pipelines.common.config import get_settings

        settings = get_settings()
        if not settings.bedrock_chat_model:
            raise SystemExit("BEDROCK_CHAT_MODEL 이 비어 있다")
        ensure_bedrock_token()
        logging.getLogger("langchain_aws").setLevel(logging.WARNING)

        model = ChatBedrockConverse(
            model=settings.bedrock_chat_model,
            region_name=settings.bedrock_region,
            temperature=0,
            max_tokens=max_tokens,
            timeout=settings.bedrock_request_timeout,
        )
        prompt = ChatPromptTemplate.from_messages(
            [SystemMessage(content=load_guideline()), ("human", "{pair}")]
        )
        structured = model.with_structured_output(schema=PairLabel, method="json_schema")
        self._chain = (prompt | structured).with_retry(stop_after_attempt=RETRY_ATTEMPTS)

    async def label(self, pair_text: str) -> PairLabel:
        return await self._chain.ainvoke({"pair": pair_text})


def read_label_log(path: Path) -> dict[str, dict[str, str]]:
    """pair_id → {label, reason}. 같은 쌍이 여러 번 있으면 마지막 것."""

    labels: dict[str, dict[str, str]] = {}
    if not path.exists():
        return labels
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            try:
                entry = json.loads(line)
                labels[entry["pair_id"]] = {"label": entry["label"], "reason": entry["reason"]}
            except (ValueError, KeyError):
                # 쓰다가 끊긴 줄. 그 쌍은 다음 실행에서 다시 묻는다.
                continue
    return labels


def apply_labels(records: list[dict[str, Any]], labels: dict[str, dict[str, str]]) -> None:
    for record in records:
        found = labels.get(record["pair_id"])
        if found:
            record["llm_label"] = found["label"]
            record["llm_reason"] = found["reason"]


def merge_human_columns(records: list[dict[str, Any]], csv_rows: list[dict[str, str]]) -> int:
    """기존 CSV 의 human_label/human_note 를 레코드로 옮긴다. 검수 중인 CSV 를 덮어써도 안 잃게."""

    by_id = {row.get("pair_id"): row for row in csv_rows}
    kept = 0
    for record in records:
        row = by_id.get(record["pair_id"])
        if not row:
            continue
        for column in ("human_label", "human_note"):
            if (row.get(column) or "").strip():
                record[column] = row[column]
        if (row.get("human_label") or "").strip():
            kept += 1
    return kept


async def label_records(
    records: list[dict[str, Any]],
    labeler: Labeler,
    max_concurrency: int,
    on_result: Callable[[dict[str, Any], str, str], None],
) -> tuple[int, int]:
    """쌍마다 한 번씩 묻는다. 실패한 쌍은 기록하지 않고 세기만 한다(다음 실행에서 재시도)."""

    semaphore = asyncio.Semaphore(max(1, max_concurrency))
    counts = {"done": 0, "failed": 0}

    async def one(record: dict[str, Any]) -> None:
        async with semaphore:
            try:
                result = await labeler(build_pair_input(record))
                label = normalize_label(result.label)
                if label is None:
                    raise ValueError("빈 라벨")
            except Exception as e:
                counts["failed"] += 1
                log.warning(
                    "라벨 실패(다음 실행에 재시도): %s, %s: %s",
                    record["pair_id"],
                    type(e).__name__,
                    e,
                )
                return
            on_result(record, label, collapse(result.reason))
            counts["done"] += 1
            if counts["done"] % 20 == 0:
                log.info("[라벨] %d / %d", counts["done"], len(records))

    await asyncio.gather(*(one(record) for record in records))
    return counts["done"], counts["failed"]


def run_prelabel(
    out_dir: Path,
    labeler_factory: Callable[[], Labeler],
    *,
    limit: int | None = None,
    max_concurrency: int = 4,
) -> dict[str, int]:
    pairs_path = out_dir / "pairs.jsonl"
    csv_path = out_dir / "pairs.csv"
    log_path = out_dir / LABEL_LOG_NAME

    records = read_jsonl(pairs_path)
    apply_labels(records, read_label_log(log_path))
    todo = [record for record in records if not record.get("llm_label")]
    if limit is not None:
        todo = todo[:limit]

    done = failed = 0
    if todo:
        labeler = labeler_factory()
        with log_path.open("a", encoding="utf-8") as fh:

            def on_result(record: dict[str, Any], label: str, reason: str) -> None:
                record["llm_label"] = label
                record["llm_reason"] = reason
                entry = {"pair_id": record["pair_id"], "label": label, "reason": reason}
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
                fh.flush()

            done, failed = asyncio.run(label_records(todo, labeler, max_concurrency, on_result))

    human_kept = merge_human_columns(records, read_csv(csv_path)) if csv_path.exists() else 0
    write_jsonl(pairs_path, records)
    write_csv(csv_path, records)
    return {
        "total": len(records),
        "labeled_now": done,
        "failed": failed,
        "remaining": sum(1 for record in records if not record.get("llm_label")),
        "human_kept": human_kept,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="이슈 연결 후보 쌍 LLM 사전 라벨")
    parser.add_argument(
        "--dir", type=Path, default=DEFAULT_OUT_DIR, help="pairs.jsonl 이 있는 폴더"
    )
    parser.add_argument("--limit", type=int, default=None, help="이번 실행에서 물을 최대 쌍 수")
    parser.add_argument("--max-concurrency", type=int, default=4)
    parser.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args(argv)

    summary = run_prelabel(
        args.dir,
        lambda: PairLabeler(max_tokens=args.max_tokens).label,
        limit=args.limit,
        max_concurrency=args.max_concurrency,
    )
    log.info(
        "[완료] 전체 %d, 이번 라벨 %d, 실패 %d, 남은 쌍 %d (보존한 사람 라벨 %d)",
        summary["total"],
        summary["labeled_now"],
        summary["failed"],
        summary["remaining"],
        summary["human_kept"],
    )
    records = read_jsonl(args.dir / "pairs.jsonl")
    by_band = Counter((r["band"], r.get("llm_label") or "-") for r in records)
    for band in sorted({band for band, _ in by_band}):
        dist = ", ".join(f"{label}={n}" for (b, label), n in sorted(by_band.items()) if b == band)
        log.info("[분포] %-20s %s", band, dist)


if __name__ == "__main__":
    main()
