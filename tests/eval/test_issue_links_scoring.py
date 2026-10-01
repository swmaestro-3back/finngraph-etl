"""이슈 연결 임계값 채점(가중 precision/recall, kappa)과 CSV 입출력 단위 테스트."""

from __future__ import annotations

import csv

import pytest
from linkeval import CSV_COLUMNS, normalize_label, read_csv, write_csv
from score import (
    LabeledPair,
    Metrics,
    band_populations,
    band_summary,
    bootstrap_interval,
    cohen_kappa,
    default_thresholds,
    main,
    metrics_at,
    parse_rows,
    pick_best_f1,
    pick_recall_at_precision,
    sweep,
)


def _pairs(band: str, score: float, positives: int, negatives: int) -> list[LabeledPair]:
    labels = ["SAME_STORY"] * positives + ["DIFFERENT"] * negatives
    return [
        LabeledPair(pair_id=f"{band}-{i}", band=band, score=score, human=label)
        for i, label in enumerate(labels)
    ]


# 모집단: 높은 밴드 1,000쌍(양성 90%), 낮은 밴드 9,000쌍(양성 10%). 표본은 밴드당 10개씩.
POPULATIONS = {"0.8-1.0": 1000, "0.4-0.5": 9000}
SAMPLE = _pairs("0.8-1.0", 0.85, 9, 1) + _pairs("0.4-0.5", 0.45, 1, 9)


def test_default_thresholds():
    thresholds = default_thresholds()

    assert len(thresholds) == 13
    assert thresholds[0] == 0.30
    assert thresholds[-1] == 0.90
    assert 0.45 in thresholds


def test_reweighting_recovers_population_recall():
    m = sweep(SAMPLE, POPULATIONS, [0.8])[0]

    # 모집단 양성: 높은 밴드 900 + 낮은 밴드 900. t=0.8 은 높은 밴드만 연결한다.
    assert m.precision == pytest.approx(0.9)
    assert m.recall == pytest.approx(0.5)
    assert m.f1 == pytest.approx(2 * 0.9 * 0.5 / 1.4)
    assert m.est_pairs == pytest.approx(1000)
    assert m.n_pred == 10

    # 가중치 없이 세면 recall 이 9/10 으로 부풀려진다.
    unweighted = metrics_at(SAMPLE, {"0.8-1.0": 1.0, "0.4-0.5": 1.0}, 0.8)
    assert unweighted.recall == pytest.approx(0.9)


def test_weights_use_labeled_count_when_rows_are_skipped():
    # 낮은 밴드에서 검수자가 5행만 달았다 → 가중치 9000/5.
    sample = _pairs("0.8-1.0", 0.85, 9, 1) + _pairs("0.4-0.5", 0.45, 1, 4)
    m = sweep(sample, POPULATIONS, [0.8])[0]

    assert m.fn == pytest.approx(9000 / 5)
    assert m.recall == pytest.approx(900 / (900 + 1800))


def test_metrics_undefined_when_nothing_predicted_or_no_positive():
    nothing = metrics_at(SAMPLE, {"0.8-1.0": 1.0, "0.4-0.5": 1.0}, 0.95)
    assert nothing.precision is None
    assert nothing.recall == 0.0
    assert nothing.f1 is None

    negatives = _pairs("0.8-1.0", 0.85, 0, 3)
    assert metrics_at(negatives, {"0.8-1.0": 1.0}, 0.5).recall is None


def _m(threshold, precision, recall, f1) -> Metrics:
    return Metrics(threshold, 1, 0, 0, 0, precision, recall, f1, 0)


def test_pick_thresholds():
    metrics = [
        _m(0.50, 0.70, 0.95, 0.81),
        _m(0.55, 0.85, 0.90, 0.87),
        _m(0.60, 0.91, 0.80, 0.85),
        _m(0.65, 0.95, 0.80, 0.87),
        _m(0.70, 1.00, 0.50, 0.67),
        _m(0.95, None, 0.0, None),
    ]

    assert pick_best_f1(metrics).threshold == 0.65  # F1 동률이면 높은 임계값
    assert pick_recall_at_precision(metrics, 0.9).threshold == 0.65  # recall 동률 → precision
    assert pick_recall_at_precision(metrics, 0.99).threshold == 0.70
    assert pick_recall_at_precision(metrics, 1.01) is None


def test_cohen_kappa_known_value():
    # 2x2 표 [[20, 5], [10, 15]]: 관측 일치 0.7, 기대 일치 0.5 → kappa 0.4
    a = ["Y"] * 25 + ["N"] * 25
    b = ["Y"] * 20 + ["N"] * 5 + ["Y"] * 10 + ["N"] * 15

    assert cohen_kappa(a, b) == pytest.approx(0.4)
    assert cohen_kappa(a, a) == pytest.approx(1.0)
    assert cohen_kappa(["Y", "Y"], ["Y", "Y"]) is None
    assert cohen_kappa([], []) is None


def test_normalize_label_aliases():
    assert normalize_label(" s ") == "SAME_STORY"
    assert normalize_label("e") == "SAME_EVENT"
    assert normalize_label("different") == "DIFFERENT"
    assert normalize_label("") is None
    assert normalize_label("?") is None
    with pytest.raises(ValueError):
        normalize_label("SAME")


def _row(pair_id, band, population, score, human, llm=""):
    return {
        "pair_id": pair_id,
        "band": band,
        "band_population": str(population),
        "score": str(score),
        "human_label": human,
        "llm_label": llm,
    }


def test_parse_rows_skips_unlabeled_and_warns_on_typos():
    rows = [
        _row("p1", "0.8-1.0", 10, 0.9, "S", "SAME_STORY"),
        _row("p2", "0.8-1.0", 10, 0.85, ""),
        _row("p3", "0.8-1.0", 10, 0.81, "?"),
        _row("p4", "0.8-1.0", 10, 0.82, "SAEM_STORY"),
        _row("p5", "0.0-0.4", 99, 0.1, "D", "garbage"),
        _row("p6", "no_company:0.8-1.0", 7, 0.9, "E"),
    ]

    pairs, warnings = parse_rows(rows)

    assert [p.pair_id for p in pairs] == ["p1", "p5", "p6"]
    assert pairs[0].llm == "SAME_STORY"
    assert pairs[1].llm is None
    assert len(warnings) == 1 and "p4" in warnings[0]
    assert band_populations(rows) == {"0.8-1.0": 10, "0.0-0.4": 99, "no_company:0.8-1.0": 7}


def test_band_populations_must_be_consistent():
    with pytest.raises(ValueError):
        band_populations([_row("p1", "0.8-1.0", 10, 0.9, ""), _row("p2", "0.8-1.0", 11, 0.9, "")])


def test_band_summary_estimates_positive_count():
    summary = {r.band: r for r in band_summary(SAMPLE, POPULATIONS)}

    assert summary["0.8-1.0"].positive_rate == pytest.approx(0.9)
    assert summary["0.8-1.0"].est_positive == pytest.approx(900)
    assert summary["0.4-0.5"].counts["DIFFERENT"] == 9
    assert summary["0.5-0.6"].positive_rate is None


def test_bootstrap_interval_is_deterministic_and_bounded():
    first = bootstrap_interval(SAMPLE, POPULATIONS, 0.8, 200, seed=1)
    again = bootstrap_interval(SAMPLE, POPULATIONS, 0.8, 200, seed=1)

    assert first == again
    low, high = first["precision"]
    assert 0.0 <= low <= 0.9 <= high <= 1.0


def test_csv_roundtrip_with_bom_and_lists(tmp_path):
    path = tmp_path / "pairs.csv"
    write_csv(path, [{"pair_id": "1_2", "a_member_titles": ["제목 하나", "제목 둘"], "score": 0.5}])

    assert path.read_bytes().startswith(b"\xef\xbb\xbf")
    [row] = read_csv(path)
    assert list(row) == list(CSV_COLUMNS)
    assert row["a_member_titles"] == "제목 하나 | 제목 둘"
    assert row["score"] == "0.5"


def test_read_csv_falls_back_to_cp949(tmp_path):
    path = tmp_path / "excel.csv"
    with path.open("w", encoding="cp949", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["pair_id", "human_note"])
        writer.writeheader()
        writer.writerow({"pair_id": "1_2", "human_note": "클러스터 오염"})

    assert read_csv(path) == [{"pair_id": "1_2", "human_note": "클러스터 오염"}]


def test_main_prints_report(tmp_path, capsys):
    path = tmp_path / "pairs.csv"
    records = [
        {**_row(p.pair_id, p.band, POPULATIONS[p.band], p.score, p.human), "llm_label": p.human}
        for p in SAMPLE
    ]
    records.append(_row("x1", "no_shared", 500, 0.2, "D"))
    # 회사 없는 쌍: 높은 밴드는 전부 양성, 낮은 밴드는 전부 음성
    records.extend(_row(f"n{i}", "no_company:0.8-1.0", 40, 0.85, "S") for i in range(4))
    records.extend(_row(f"m{i}", "no_company:0.5-0.6", 400, 0.55, "D") for i in range(4))
    write_csv(path, records)

    main(["--csv", str(path), "--bootstrap", "50"])
    out = capsys.readouterr().out

    assert "회사 공유 F1 최대" in out
    assert "no_shared" in out
    assert "kappa 1.000" in out
    # 회사 없는 쌍은 따로 채점해 회사 없는 임계값 설정으로 안내한다
    assert "NEWS_ISSUE_LINK_NO_COMPANY_THRESHOLD" in out
    no_company_line = next(
        line for line in out.splitlines() if line.startswith("회사 없음 F1 최대")
    )
    assert "precision 1.000" in no_company_line and "recall 1.000" in no_company_line
