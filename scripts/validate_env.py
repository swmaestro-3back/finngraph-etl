from __future__ import annotations

from pipelines.common.config import get_settings


def main() -> None:
    settings = get_settings()
    print("DATABASE_URL configured:", bool(settings.database_url))
    print("KIS_APP_KEY configured:", bool(settings.kis_app_key))
    print("KIS_APP_SECRET configured:", bool(settings.kis_app_secret))


if __name__ == "__main__":
    main()

