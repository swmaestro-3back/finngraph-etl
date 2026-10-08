"""부모 순위 점수에 쓰는 두 유사도를 계산한다. 순위 점수는 두 값 중 큰 값이다.

content  내용 유사도. 이슈 임베딩의 코사인(제목 + 한 줄 요약 + 기사 제목 3개로 만든 Titan 벡터,
         news_clusters.embedding).
event    사건 유사도. 0.5 × cos(기업명을 가린 A 제목, 기업명을 가린 B 제목)
       + 0.5 × (B 의 기사 제목마다 A 기사 제목과 비교한 최대 코사인의 평균)
         기사 제목도 기업명을 가리고, A 의 기사는 B 의 마지막 시각까지 나온 것만 쓴다. 비교할
         기사가 없으면 제목 코사인만 쓴다.

기업명 가리기: 이슈 연결 기업(멤버 기사 연결 기업 포함)의 이름과 별칭을 긴 것부터 "[기업]" 으로
바꾸고, 연달아 나온 표시는 하나로 줄인다. 같은 기업의 서로 다른 사건이 기업 이름만으로 높은 점수를
받지 않게 하기 위해서다.

벡터는 이슈 임베딩과 같은 Titan 모델·차원으로 만들고, DB 에 저장하지 않고 실행 동안만 메모리에
둔다.
"""

from __future__ import annotations

import re
import threading
from collections.abc import Callable, Sequence

import numpy as np

from pipelines.news.transformers.issue_link_vote.issue import (
    CompanyName,
    Issue,
    collapse,
    company_aliases,
    issue_company_ids,
    last_seen,
)

MASK = "[기업]"

Embedder = Callable[[list[str]], Sequence[Sequence[float]]]


def mask_text(text: str, aliases: list[str]) -> str:
    """별칭(긴 것부터)을 MASK 로 바꾸고 연달아 나온 MASK 를 하나로 줄인다."""

    out = text
    for alias in aliases:
        out = out.replace(alias, "\x00")
    out = re.sub(r"\x00(\s*[·,/&–-]?\s*\x00)+", "\x00", out)
    out = out.replace("\x00", MASK)
    return collapse(out)


def masked_title(issue: Issue, names: dict[int, CompanyName]) -> str:
    return mask_text(collapse(issue.title), company_aliases(issue_company_ids(issue), names))


def masked_members(issue: Issue, names: dict[int, CompanyName]) -> list[tuple]:
    """[(발행 시각, 기업명을 가린 기사 제목)] 를 저장소 순서대로 돌려준다. 빈 제목은 뺀다."""

    aliases = company_aliases(issue_company_ids(issue), names)
    out = []
    for member in issue.members:
        title = collapse(member.title)
        if title:
            out.append((member.published_at, mask_text(title, aliases)))
    return out


class EventScorer:
    """event 점수를 계산한다. 텍스트별 단위 벡터를 실행 동안 메모리에 두며, 스레드 안전하다."""

    def __init__(self, embed: Embedder, names: dict[int, CompanyName]):
        self._embed = embed
        self._names = names
        self._vectors: dict[str, np.ndarray] = {}
        self._lock = threading.Lock()

    def _ensure(self, texts: list[str]) -> None:
        with self._lock:
            todo = list(dict.fromkeys(t for t in texts if t not in self._vectors))
        if not todo:
            return
        vectors = self._embed(todo)
        with self._lock:
            for text, vec in zip(todo, vectors, strict=True):
                arr = np.asarray(vec, dtype=np.float64)
                norm = np.linalg.norm(arr)
                self._vectors[text] = arr / norm if norm else arr

    def score(self, a: Issue, b: Issue) -> float:
        title_a, title_b = masked_title(a, self._names), masked_title(b, self._names)
        members_a = masked_members(a, self._names)
        members_b = masked_members(b, self._names)
        needed = [title_a, title_b, *(t for _, t in members_a), *(t for _, t in members_b)]
        self._ensure(needed)
        with self._lock:
            vec = {t: self._vectors[t] for t in needed}

        title_sim = float(vec[title_a] @ vec[title_b])
        cutoff = last_seen(b)
        keep = [t for when, t in members_a if when <= cutoff]
        if not keep or not members_b:
            return title_sim
        mat_a = np.stack([vec[t] for t in keep])
        mat_b = np.stack([vec[t] for _, t in members_b])
        coverage = float((mat_b @ mat_a.T).max(axis=1).mean())
        return 0.5 * title_sim + 0.5 * coverage
