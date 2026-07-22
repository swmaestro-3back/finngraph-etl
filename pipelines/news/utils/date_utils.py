from datetime import datetime
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo


SEOUL_TIMEZONE = ZoneInfo("Asia/Seoul")


def parse_news_pub_date(pub_date: str) -> datetime | None:
    if not pub_date:
        return None

    pub_date = pub_date.strip()

    try:
        parsed = parsedate_to_datetime(pub_date)

        if parsed is not None:
            return parsed
    except (TypeError, ValueError, OverflowError):
        pass

    try:
        parsed = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
    except ValueError:
        parsed = None

    if parsed is None:
        for date_format in (
            "%Y.%m.%d. %H:%M",
            "%Y.%m.%d. %H:%M:%S",
        ):
            try:
                parsed = datetime.strptime(pub_date, date_format)
                break
            except ValueError:
                continue

    if parsed is None:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SEOUL_TIMEZONE)

    return parsed
