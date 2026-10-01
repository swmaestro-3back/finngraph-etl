"""검수된 pairs.csv 로 이슈 연결 임계값을 고른다.

human_label 이 있는 행만 쓰고, 양성은 SAME_EVENT 또는 SAME_STORY 다. 'score >= t 이면 연결'
규칙의 쌍 단위 precision/recall/F1 을 t = 0.30..0.90 (0.05 간격)에서 낸다. 운영이 임계값을 따로
쓰는 두 모집단 — 회사 공유 쌍(NEWS_ISSUE_LINK_THRESHOLD), 둘 다 회사 없는 쌍
(NEWS_ISSUE_LINK_NO_COMPANY_THRESHOLD) — 을 각각 채점한다.

표본은 점수 밴드별 층화 추출이라 그대로 세면 높은 점수대가 과대 대표된다. 행마다 가중치
band_population / (그 밴드에서 라벨된 행 수) 를 줘 모집단 기준으로 되돌린다(Horvitz-Thompson).
검수자가 건너뛴 행은 밴드 안에서 무작위로 빠졌다고 보고 라벨된 행 수로 나눈다.

recall 은 각 후보 모집단(둘 다 이름 있음, lookback 이내, 회사 공유 또는 둘 다 회사 없음) 안에서의
재현율이다. 어느 후보에도 들지 못한 연결은 분모에 없고, no_shared 층으로 그 규모만 따로 가늠한다.

실행: .venv/bin/python scripts/eval/issue_links/score.py --csv tmp/eval/issue_links/pairs.csv
"""

from __future__ import annotations

import argparse
import random
from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from linkeval import (
    BAND_LABELS,
    DEFAULT_OUT_DIR,
    LABELS,
    NO_COMPANY_BAND_LABELS,
    NO_SHARED_BAND,
    POSITIVE_LABELS,
    normalize_label,
    read_csv,
)

# (이름, 운영 설정, 밴드) — 운영이 임계값을 따로 쓰는 후보 모집단마다 따로 채점한다.
POPULATIONS = (
    ("회사 공유", "NEWS_ISSUE_LINK_THRESHOLD", BAND_LABELS),
    ("회사 없음", "NEWS_ISSUE_LINK_NO_COMPANY_THRESHOLD", NO_COMPANY_BAND_LABELS),
)
KNOWN_BANDS = frozenset((*BAND_LABELS, *NO_COMPANY_BAND_LABELS, NO_SHARED_BAND))


@dataclass(frozen=True)
class LabeledPair:
    pair_id: str
    band: str
    score: float
    human: str
    llm: str | None = None

    @property
    def positive(self) -> bool:
        return self.human in POSITIVE_LABELS


@dataclass(frozen=True)
class Metrics:
    threshold: float
    n_pred: int  # 라벨된 표본 중 score >= t 인 행 수 (불확실성 가늠용)
    tp: float
    fp: float
    fn: float
    precision: float | None
    recall: float | None
    f1: float | None
    est_pairs: float  # 모집단에서 score >= t 인 후보 쌍 수 추정


@dataclass(frozen=True)
class BandRow:
    band: str
    population: int
    labeled: int
    counts: dict[str, int]
    positive: int
    positive_rate: float | None
    est_positive: float | None  # 모집단 양성 쌍 수 추정 = 모집단 x 표본 양성 비율


def default_thresholds(start: float = 0.30, stop: float = 0.90, step: float = 0.05) -> list[float]:
    count = int(round((stop - start) / step)) + 1
    return [round(start + i * step, 2) for i in range(count)]


def band_populations(rows: Sequence[dict[str, str]]) -> dict[str, int]:
    """라벨 여부와 무관하게 CSV 전체에서 밴드별 모집단 크기를 읽는다."""

    populations: dict[str, int] = {}
    for row in rows:
        band = (row.get("band") or "").strip()
        try:
            population = int(float(row.get("band_population") or ""))
        except ValueError:
            continue
        if band in populations and populations[band] != population:
            raise ValueError(
                f"밴드 {band} 의 band_population 이 행마다 다르다 — CSV 가 섞였는지 확인"
            )
        populations[band] = population
    return populations


def parse_rows(rows: Sequence[dict[str, str]]) -> tuple[list[LabeledPair], list[str]]:
    """human_label 이 있는 행만 LabeledPair 로. 읽을 수 없는 행은 경고로 돌려준다."""

    pairs: list[LabeledPair] = []
    warnings: list[str] = []
    for row in rows:
        pair_id = row.get("pair_id", "?")
        try:
            human = normalize_label(row.get("human_label"))
        except ValueError as e:
            warnings.append(f"{pair_id}: human_label 무시 — {e}")
            continue
        if human is None:
            continue
        try:
            llm = normalize_label(row.get("llm_label"))
        except ValueError:
            llm = None
        try:
            score = float(row["score"])
        except (KeyError, ValueError):
            warnings.append(f"{pair_id}: score 를 읽을 수 없어 제외")
            continue
        band = (row.get("band") or "").strip()
        if band not in KNOWN_BANDS:
            warnings.append(f"{pair_id}: 알 수 없는 band {band!r} 제외")
            continue
        pairs.append(LabeledPair(pair_id=pair_id, band=band, score=score, human=human, llm=llm))
    return pairs, warnings


def band_weights(pairs: Sequence[LabeledPair], populations: dict[str, int]) -> dict[str, float]:
    """밴드별 가중치 = 모집단 / 라벨된 표본 수."""

    counts = Counter(pair.band for pair in pairs)
    return {band: populations[band] / count for band, count in counts.items()}


def metrics_at(
    pairs: Sequence[LabeledPair], weights: dict[str, float], threshold: float
) -> Metrics:
    tp = fp = fn = est = 0.0
    n_pred = 0
    for pair in pairs:
        weight = weights[pair.band]
        predicted = pair.score >= threshold
        if predicted:
            n_pred += 1
            est += weight
            if pair.positive:
                tp += weight
            else:
                fp += weight
        elif pair.positive:
            fn += weight

    precision = tp / (tp + fp) if tp + fp > 0 else None
    recall = tp / (tp + fn) if tp + fn > 0 else None
    if precision is None or recall is None:
        f1 = None
    elif precision + recall == 0:
        f1 = 0.0
    else:
        f1 = 2 * precision * recall / (precision + recall)
    return Metrics(threshold, n_pred, tp, fp, fn, precision, recall, f1, est)


def sweep(
    pairs: Sequence[LabeledPair], populations: dict[str, int], thresholds: Sequence[float]
) -> list[Metrics]:
    weights = band_weights(pairs, populations)
    return [metrics_at(pairs, weights, threshold) for threshold in thresholds]


def pick_best_f1(metrics: Sequence[Metrics]) -> Metrics | None:
    """F1 최대. 동률이면 높은 임계값 — 잘못된 연결이 빠진 연결보다 눈에 띄기 때문이다."""

    candidates = [m for m in metrics if m.f1 is not None]
    return max(candidates, key=lambda m: (m.f1, m.threshold)) if candidates else None


def pick_recall_at_precision(metrics: Sequence[Metrics], min_precision: float) -> Metrics | None:
    """precision >= min_precision 중 recall 최대. 동률이면 precision, 그다음 임계값이 높은 쪽."""

    candidates = [
        m
        for m in metrics
        if m.precision is not None and m.recall is not None and m.precision >= min_precision
    ]
    return (
        max(candidates, key=lambda m: (m.recall, m.precision, m.threshold)) if candidates else None
    )


def bootstrap_interval(
    pairs: Sequence[LabeledPair],
    populations: dict[str, int],
    threshold: float,
    iterations: int,
    seed: int,
) -> dict[str, tuple[float, float] | None]:
    """밴드 안에서 복원 재추출한 95% 구간. 표본이 수백 개라 점추정만 믿지 않도록 같이 보여준다."""

    rng = random.Random(seed)
    groups: dict[str, list[LabeledPair]] = defaultdict(list)
    for pair in pairs:
        groups[pair.band].append(pair)

    values: dict[str, list[float]] = {"precision": [], "recall": [], "f1": []}
    for _ in range(iterations):
        resample = [rng.choice(group) for group in groups.values() for _ in group]
        m = metrics_at(resample, band_weights(resample, populations), threshold)
        for name in values:
            value = getattr(m, name)
            if value is not None:
                values[name].append(value)

    return {
        name: (float(np.percentile(v, 2.5)), float(np.percentile(v, 97.5))) if v else None
        for name, v in values.items()
    }


def cohen_kappa(a: Sequence[str], b: Sequence[str]) -> float | None:
    """두 평가자의 명목 라벨 일치도. 기대 일치가 1 이면(둘 다 한 범주만 씀) 정의되지 않는다."""

    n = len(a)
    if n == 0 or n != len(b):
        return None
    observed = sum(x == y for x, y in zip(a, b, strict=True)) / n
    count_a, count_b = Counter(a), Counter(b)
    expected = sum(count_a[c] * count_b[c] for c in set(a) | set(b)) / (n * n)
    if expected == 1:
        return None
    return (observed - expected) / (1 - expected)


def band_summary(pairs: Sequence[LabeledPair], populations: dict[str, int]) -> list[BandRow]:
    rows = []
    for band in (*BAND_LABELS, *NO_COMPANY_BAND_LABELS, NO_SHARED_BAND):
        group = [pair for pair in pairs if pair.band == band]
        counts = Counter(pair.human for pair in group)
        positive = sum(counts[label] for label in POSITIVE_LABELS)
        rate = positive / len(group) if group else None
        population = populations.get(band, 0)
        rows.append(
            BandRow(
                band=band,
                population=population,
                labeled=len(group),
                counts={label: counts[label] for label in LABELS},
                positive=positive,
                positive_rate=rate,
                est_positive=population * rate if rate is not None else None,
            )
        )
    return rows


# ── 출력 ─────────────────────────────────────────────────────────────────────


def _fmt(value: float | None, digits: int = 3) -> str:
    return "-" if value is None else f"{value:.{digits}f}"


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(headers)]
    lines = ["  ".join(h.rjust(w) for h, w in zip(headers, widths, strict=True))]
    lines.extend("  ".join(c.rjust(w) for c, w in zip(r, widths, strict=True)) for r in rows)
    return "\n".join(lines)


def _describe(name: str, m: Metrics | None, ci: dict[str, tuple[float, float] | None]) -> str:
    if m is None:
        return f"{name}: 해당 임계값 없음"

    def interval(key: str) -> str:
        bounds = ci.get(key)
        return f" [{bounds[0]:.3f}, {bounds[1]:.3f}]" if bounds else ""

    return (
        f"{name}: t={m.threshold:.2f}  precision {_fmt(m.precision)}{interval('precision')}  "
        f"recall {_fmt(m.recall)}{interval('recall')}  F1 {_fmt(m.f1)}{interval('f1')}  "
        f"(표본 n_pred={m.n_pred})"
    )


def report_agreement(pairs: Sequence[LabeledPair]) -> None:
    both = [pair for pair in pairs if pair.llm is not None]
    print(f"\n[LLM vs 사람] 둘 다 있는 행 {len(both)}개")
    if not both:
        return
    human = [pair.human for pair in both]
    llm = [pair.llm or "" for pair in both]
    accuracy = sum(h == m for h, m in zip(human, llm, strict=True)) / len(both)
    human_bin = ["POS" if h in POSITIVE_LABELS else "NEG" for h in human]
    llm_bin = ["POS" if m in POSITIVE_LABELS else "NEG" for m in llm]
    accuracy_bin = sum(h == m for h, m in zip(human_bin, llm_bin, strict=True)) / len(both)
    print(f"3분류  accuracy {accuracy:.3f}  kappa {_fmt(cohen_kappa(human, llm))}")
    print(f"양성/음성 accuracy {accuracy_bin:.3f}  kappa {_fmt(cohen_kappa(human_bin, llm_bin))}")
    confusion = Counter(zip(human, llm, strict=True))
    print("혼동행렬 (행=사람, 열=LLM)")
    print(
        _table(
            ["human\\llm", *LABELS],
            [[h, *(str(confusion[(h, m)]) for m in LABELS)] for h in LABELS],
        )
    )


def report_population(
    name: str,
    setting: str,
    bands: Sequence[str],
    pairs: Sequence[LabeledPair],
    populations: dict[str, int],
    args: argparse.Namespace,
) -> None:
    """후보 모집단 하나의 임계값별 성능과 추천 임계값을 출력한다."""

    print(f"\n[{name}] 임계값별 쌍 단위 성능 — 모집단 기준 가중 추정, 반영할 곳 {setting}")
    labeled_bands = {pair.band for pair in pairs}
    for band in bands:
        if populations.get(band, 0) > 0 and band not in labeled_bands:
            print(
                f"[경고] 밴드 {band} 는 모집단 {populations[band]} 인데 라벨이 없어 추정에서 빠진다"
            )
    if not pairs:
        print(f"채점할 {name} 쌍이 없다")
        return

    metrics = sweep(pairs, populations, default_thresholds())
    print(
        _table(
            ["t", "n_pred", "precision", "recall", "F1", "est_pairs"],
            [
                [
                    f"{m.threshold:.2f}",
                    str(m.n_pred),
                    _fmt(m.precision),
                    _fmt(m.recall),
                    _fmt(m.f1),
                    f"{m.est_pairs:.0f}",
                ]
                for m in metrics
            ],
        )
    )

    best = pick_best_f1(metrics)
    safe = pick_recall_at_precision(metrics, args.min_precision)
    print()
    for label, m in (
        (f"{name} F1 최대", best),
        (f"{name} precision>={args.min_precision:g} 중 recall 최대", safe),
    ):
        ci = (
            bootstrap_interval(pairs, populations, m.threshold, args.bootstrap, args.seed)
            if m is not None and args.bootstrap > 0
            else {}
        )
        print(_describe(label, m, ci))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="이슈 연결 임계값 채점")
    parser.add_argument("--csv", type=Path, default=DEFAULT_OUT_DIR / "pairs.csv")
    parser.add_argument("--min-precision", type=float, default=0.9)
    parser.add_argument("--bootstrap", type=int, default=1000, help="0 이면 신뢰구간 생략")
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rows = read_csv(args.csv)
    populations = band_populations(rows)
    pairs, warnings = parse_rows(rows)
    for warning in warnings[:20]:
        print(f"[경고] {warning}")
    if len(warnings) > 20:
        print(f"[경고] 외 {len(warnings) - 20}건")
    missing = sorted({pair.band for pair in pairs} - set(populations))
    if missing:
        raise SystemExit(f"band_population 이 비어 있는 밴드: {missing} — CSV 를 확인할 것")

    groups = {name: [p for p in pairs if p.band in bands] for name, _, bands in POPULATIONS}
    no_shared_count = sum(1 for pair in pairs if pair.band == NO_SHARED_BAND)
    counts = ", ".join(f"{name} {len(group)}" for name, group in groups.items())
    print(f"라벨된 행 {len(pairs)}개 ({counts}, no_shared {no_shared_count})")
    if not any(groups.values()):
        print("채점할 후보 쌍이 없다. human_label 을 채운 뒤 다시 실행할 것")
        return

    for name, setting, bands in POPULATIONS:
        report_population(name, setting, bands, groups[name], populations, args)

    summary = band_summary(pairs, populations)
    print("\n[밴드별 라벨 분포]")
    print(
        _table(
            ["band", "population", "labeled", *LABELS, "pos_rate", "est_pos"],
            [
                [
                    r.band,
                    str(r.population),
                    str(r.labeled),
                    *(str(r.counts[label]) for label in LABELS),
                    _fmt(r.positive_rate),
                    _fmt(r.est_positive, 0),
                ]
                for r in summary
            ],
        )
    )

    no_shared = summary[-1]
    if no_shared.labeled:
        candidate_positive = sum(r.est_positive or 0 for r in summary[:-1])
        print(
            "\n후보 규칙(회사 공유 / 둘 다 회사 없음) 때문에 빠지는 연결 추정: "
            f"{_fmt(no_shared.est_positive, 0)}쌍 "
            f"(no_shared 표본 {no_shared.labeled}개 중 양성 {no_shared.positive}개, "
            f"후보 안의 양성 추정 {candidate_positive:.0f}쌍)"
        )

    report_agreement(pairs)


if __name__ == "__main__":
    main()
