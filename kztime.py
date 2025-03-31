from datetime import datetime, timedelta, timezone


def get_current_time() -> datetime:
    delta = timedelta(hours=5, minutes=0)
    return datetime.now(timezone.utc) + delta