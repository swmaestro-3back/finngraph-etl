"""이슈 연결 평가 스크립트(dump_candidates · prelabel · score)가 함께 쓰는 상수와 입출력.

DB·Bedrock 을 부르지 않는 순수 함수만 둔다. 스크립트끼리 형식(밴드 이름, 라벨 값, CSV 열)이
어긋나면 채점이 조용히 틀어지므로 한곳에서 정의한다.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import json
import logging
import os
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any

import numpy as np

from pipelines.news.transformers.issue_linker import EMBEDDING_ARTICLE_TITLES, build_embedding_text

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUT_DIR = REPO_ROOT / "tmp" / "eval" / "issue_links"

# 운영 연결과 같은 임베딩이어야 점수 분포가 같다. 모델은 dump 가 운영과 같은
# NEWS_ISSUE_EMBEDDING_MODEL 에서 읽고, 캐시 키에 섞어 모델이 바뀌면 다시 임베딩한다.
EMBEDDING_DIM = 1024
MEMBER_TITLE_COUNT = EMBEDDING_ARTICLE_TITLES

# 하한 포함·상한 미포함. 첫 밴드는 음수 코사인까지, 마지막 밴드는 1.0(부동소수 오차 포함)까지
# 받는다.
BAND_EDGES = (0.4, 0.5, 0.6, 0.7, 0.8)
BAND_LABELS = ("0.0-0.4", "0.4-0.5", "0.5-0.6", "0.6-0.7", "0.7-0.8", "0.8-1.0")
# 둘 다 회사가 없는 쌍. 운영은 이 쌍만 NEWS_ISSUE_LINK_NO_COMPANY_THRESHOLD 로 따로 잇으므로 같은
# 밴드로 층화하되 이름을 갈라 둔다 — band_population 이 밴드 이름별로 하나여야 해서다.
NO_COMPANY_BAND_PREFIX = "no_company:"
NO_COMPANY_BAND_LABELS = tuple(f"{NO_COMPANY_BAND_PREFIX}{band}" for band in BAND_LABELS)
# 운영 규칙상 후보가 아닌 쌍(회사를 공유하지 않고, 둘 다 회사 없는 쌍도 아님). 임계값 채점에서는
# 빠지고, 후보 규칙이 놓치는 연결이 얼마나 되는지 가늠하는 데만 쓴다.
NO_SHARED_BAND = "no_shared"

LABELS = ("SAME_EVENT", "SAME_STORY", "DIFFERENT")
POSITIVE_LABELS = frozenset({"SAME_EVENT", "SAME_STORY"})
# 검수자가 엑셀에서 빠르게 입력하도록 줄임말을 받는다.
LABEL_ALIASES = {
    "E": "SAME_EVENT",
    "EVENT": "SAME_EVENT",
    "S": "SAME_STORY",
    "STORY": "SAME_STORY",
    "D": "DIFFERENT",
    "DIFF": "DIFFERENT",
}
# 판단 보류. 빈칸과 같이 채점에서 뺀다.
SKIP_LABELS = frozenset({"?", "UNSURE", "SKIP"})

CSV_COLUMNS = (
    "pair_id",
    "band",
    "band_population",
    "score",
    "a_id",
    "a_date",
    "a_title",
    "a_summary",
    "a_member_titles",
    "b_id",
    "b_date",
    "b_title",
    "b_summary",
    "b_member_titles",
    "shared_companies",
    "llm_label",
    "llm_reason",
    "human_label",
    "human_note",
    # 제목만으로 판단이 어려운 쌍을 위한 대표 기사 본문 리드. 채점에는 쓰지 않는다.
    "a_lead",
    "b_lead",
)
LIST_SEPARATOR = " | "

log = logging.getLogger("issue_links")


def collapse(text: str | None) -> str:
    return " ".join((text or "").split())


# 클러스터 임베딩 입력(이름, 요약, 최신 멤버 기사 제목 3개)은 운영 이슈 연결의 함수를 그대로 쓴다.
# 따로 두면 어긋나도 모른 채 여기서 고른 임계값이 운영 점수와 맞지 않게 된다.
embedding_text = build_embedding_text


def band_of(score: float) -> str:
    return BAND_LABELS[int(np.digitize(score, BAND_EDGES))]


def normalize_label(value: str | None) -> str | None:
    """라벨 문자열을 정규화한다. 빈칸·보류는 None, 알 수 없는 값은 ValueError."""

    cleaned = (value or "").strip().upper()
    if not cleaned or cleaned in SKIP_LABELS:
        return None
    if cleaned in LABELS:
        return cleaned
    if cleaned in LABEL_ALIASES:
        return LABEL_ALIASES[cleaned]
    raise ValueError(f"알 수 없는 라벨: {value!r}")


# ── 임베딩 캐시 ──────────────────────────────────────────────────────────────


def cache_key(cluster_id: int, text: str, model: str, dim: int) -> str:
    digest = hashlib.sha256(f"{model}|{dim}|{text}".encode()).hexdigest()[:16]
    return f"{cluster_id}:{digest}"


def _encode_vector(vector: Sequence[float]) -> str:
    return base64.b64encode(np.asarray(vector, dtype=np.float32).tobytes()).decode("ascii")


def _decode_vector(encoded: str) -> np.ndarray:
    return np.frombuffer(base64.b64decode(encoded), dtype=np.float32)


class EmbeddingCache:
    """클러스터 id + 입력 텍스트 해시 → 벡터. 추가 전용 JSONL 이라 중간에 끊겨도 앞 배치는 남는다.

    키에 모델·차원을 섞으므로 요약이 생기거나 모델이 바뀐 클러스터만 다시 임베딩된다.
    """

    def __init__(self, path: Path):
        self.path = path
        self._vectors: dict[str, np.ndarray] = {}
        if path.exists():
            with path.open(encoding="utf-8") as fh:
                for line_no, line in enumerate(fh, start=1):
                    try:
                        entry = json.loads(line)
                        self._vectors[entry["key"]] = _decode_vector(entry["vector"])
                    except (ValueError, KeyError) as e:
                        # 쓰다가 끊긴 마지막 줄은 버리고 그 클러스터만 다시 임베딩한다.
                        log.warning("임베딩 캐시 %d번째 줄 무시: %s", line_no, e)

    def __len__(self) -> int:
        return len(self._vectors)

    def get(self, key: str) -> np.ndarray | None:
        return self._vectors.get(key)

    def put_many(self, entries: Iterable[tuple[str, int, Sequence[float]]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as fh:
            for key, cluster_id, vector in entries:
                encoded = _encode_vector(vector)
                fh.write(
                    json.dumps({"key": key, "cluster_id": cluster_id, "vector": encoded}) + "\n"
                )
                self._vectors[key] = _decode_vector(encoded)


EmbedFn = Callable[[list[str]], list[list[float]]]


def embed_with_cache(
    items: Sequence[tuple[int, str]],
    cache: EmbeddingCache,
    embed_fn: EmbedFn,
    model: str,
    dim: int = EMBEDDING_DIM,
    batch_size: int = 64,
) -> tuple[np.ndarray, int]:
    """(cluster_id, text) 목록을 L2 정규화된 행렬로. 캐시에 없는 것만 배치로 부른다.

    돌려주는 두 번째 값은 이번에 새로 임베딩한 개수(= Bedrock 호출 수).
    """

    keys = [cache_key(cluster_id, text, model, dim) for cluster_id, text in items]
    missing = [i for i, key in enumerate(keys) if cache.get(key) is None]

    for start in range(0, len(missing), batch_size):
        batch = missing[start : start + batch_size]
        vectors = embed_fn([items[i][1] for i in batch])
        if len(vectors) != len(batch):
            raise RuntimeError(f"임베딩 개수 불일치: {len(vectors)} != {len(batch)}")
        cache.put_many((keys[i], items[i][0], vec) for i, vec in zip(batch, vectors, strict=True))
        log.info("[임베딩] %d / %d", min(start + batch_size, len(missing)), len(missing))

    matrix = np.zeros((len(items), dim), dtype=np.float32)
    for i, key in enumerate(keys):
        vector = cache.get(key)
        assert vector is not None
        matrix[i] = vector
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    return matrix / np.where(norms == 0, 1.0, norms), len(missing)


# ── 파일 입출력 ──────────────────────────────────────────────────────────────


def _atomic_write(path: Path, write: Callable[[Any], None], encoding: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding=encoding, newline="") as fh:
        write(fh)
    os.replace(tmp, path)


def write_jsonl(path: Path, records: Iterable[dict[str, Any]]) -> None:
    def write(fh: Any) -> None:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    _atomic_write(path, write, "utf-8")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def csv_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list | tuple):
        return LIST_SEPARATOR.join(str(v) for v in value)
    return str(value)


def write_csv(path: Path, records: Iterable[dict[str, Any]]) -> None:
    """엑셀이 한글을 깨지 않게 BOM 을 붙인 UTF-8 로 쓴다."""

    def write(fh: Any) -> None:
        writer = csv.DictWriter(fh, fieldnames=list(CSV_COLUMNS), extrasaction="ignore")
        writer.writeheader()
        for record in records:
            writer.writerow({column: csv_value(record.get(column)) for column in CSV_COLUMNS})

    _atomic_write(path, write, "utf-8-sig")


def read_csv(path: Path) -> list[dict[str, str]]:
    """검수된 CSV 를 읽는다. 엑셀이 'CSV UTF-8' 이 아닌 일반 CSV 로 저장하면 cp949 일 수 있다."""

    for encoding in ("utf-8-sig", "cp949"):
        try:
            with path.open(encoding=encoding, newline="") as fh:
                return [dict(row) for row in csv.DictReader(fh)]
        except UnicodeDecodeError:
            continue
    raise ValueError(f"{path}: UTF-8 도 cp949 도 아닌 인코딩이다. 'CSV UTF-8' 로 다시 저장할 것")


def protect_output_dir(out_dir: Path) -> None:
    """출력물에 기사 원문이 들어가므로 실수로 커밋되지 않게 폴더 안에 .gitignore 를 둔다."""

    out_dir.mkdir(parents=True, exist_ok=True)
    ignore = out_dir / ".gitignore"
    if not ignore.exists():
        ignore.write_text("*\n", encoding="utf-8")
