"""
제목/요약문을 클러스터링용 토큰 목록으로 바꾸는 전처리.

Kiwi 는 "LG에너지솔루션" 같은 복합 고유명사를 LG / 에너지 / 솔루션 으로 쪼갠다.
쪼개진 조각만 쓰면 서로 다른 기업이 겹쳐 보이므로, 원문에서 붙어 있던 명사
토큰들을 다시 이어 붙인 복합명사도 함께 만들어 더 큰 가중치를 준다.
"""

from __future__ import annotations

import html
import re
from functools import lru_cache

from kiwipiepy import Kiwi

# 명사로 취급할 품사. SL 은 영문(LG, SK), SH 는 한자.
NOUN_TAGS = frozenset({"NNG", "NNP", "SL", "SH"})

# 검색어와 기사 형식에서 비롯돼 거의 모든 문서에 나타나는 말들.
# IDF 로도 상당 부분 걸러지지만, 복합명사 결합을 오염시키므로 미리 제거한다.
STOPWORDS = frozenset(
    {
        "특징",
        "특징주",
        "주가",
        "종목",
        "관련주",
        "테마주",
        "수혜주",
        "강세",
        "약세",
        "급등",
        "급락",
        "상승",
        "하락",
        "급등락",
        "상한가",
        "하한가",
        "장중",
        "개장",
        "마감",
        "증시",
        "코스피",
        "코스닥",
        "s&p500",
        "나스닥",
        "지수",
        "소식",
        "기대감",
        "전망",
        "부각",
        "돌파",
        "기록",
        "출발",
        "오늘",
        "어제",
        "이날",
        "기자",
        "뉴스",
        "속보",
        "단독",
        "억원",
        "만원",
        "포인트",
        "퍼센트",
    }
)

# 토큰 가중치. 복합명사(기업명일 가능성이 높다)를 가장 세게 본다.
COMPOUND_WEIGHT = 3.0
PROPER_NOUN_WEIGHT = 2.0
COMMON_NOUN_WEIGHT = 1.0

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def strip_html(text: str) -> str:
    """검색 API 응답에 섞여 오는 <b> 태그와 HTML 엔티티를 제거한다."""
    return _WS_RE.sub(" ", html.unescape(_TAG_RE.sub("", text))).strip()


@lru_cache(maxsize=1)
def _kiwi() -> Kiwi:
    """Kiwi 인스턴스 생성은 무거우므로 프로세스당 하나만 만든다."""
    return Kiwi()


def tokenize(text: str) -> list[tuple[str, float]]:
    """(토큰, 가중치) 목록을 돌려준다. 같은 토큰이 여러 번 나올 수 있다."""
    cleaned = strip_html(text)
    if not cleaned:
        return []

    tokens = [t for t in _kiwi().tokenize(cleaned)]
    weighted: list[tuple[str, float]] = []

    for run in _noun_runs(tokens):
        for token in run:
            surface = token.form.lower()
            if not _keep(surface, token.tag):
                continue
            weight = PROPER_NOUN_WEIGHT if token.tag == "NNP" else COMMON_NOUN_WEIGHT
            weighted.append((surface, weight))

        if len(run) > 1:
            compound = "".join(t.form for t in run).lower()
            if _keep(compound, "NNP"):
                weighted.append((compound, COMPOUND_WEIGHT))

    return weighted


def _noun_runs(tokens) -> list[list]:
    """원문에서 공백 없이 맞붙어 있는 명사 토큰들을 하나의 묶음으로 만든다."""
    runs: list[list] = []
    current: list = []
    prev_end = -1

    for token in tokens:
        if token.tag not in NOUN_TAGS:
            current = []
            prev_end = -1
            continue

        if current and token.start == prev_end:
            current.append(token)
        else:
            current = [token]
            runs.append(current)
        prev_end = token.start + token.len

    return runs


def _keep(surface: str, tag: str) -> bool:
    if surface in STOPWORDS:
        return False
    # 한 글자 한글 명사는 대부분 의미가 없다. 영문/한자는 SK, LG 처럼 짧아도 살린다.
    if len(surface) < 2 and tag not in {"SL", "SH"}:
        return False
    return True


def document_terms(
    title: str,
    description: str = "",
    description_weight: float = 0.4,
) -> list[tuple[str, float]]:
    """제목을 주 신호로, description 을 낮은 가중치의 보조 신호로 합친다."""
    terms = tokenize(title)
    if description and description_weight > 0:
        terms += [(term, weight * description_weight) for term, weight in tokenize(description)]
    return terms
