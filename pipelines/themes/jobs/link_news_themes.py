from __future__ import annotations

from pipelines.themes.loaders.news_links import link_news_to_themes

DEFAULT_WINDOW_HOURS = 24


def run(window_hours: int | None = None) -> None:

    window = window_hours or DEFAULT_WINDOW_HOURS
    linked = link_news_to_themes(window_hours=window)

    print("\n" + "=" * 60)
    print("연결 결과")
    print("=" * 60)
    print(f"- 대상 범위 최근 {window}시간, 신규 연결 {linked}건")
    print("작업 완료")
    print("=" * 60)


if __name__ == "__main__":
    import sys

    run(int(sys.argv[1]) if len(sys.argv) > 1 else None)
