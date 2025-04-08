import time
from datetime import datetime, timedelta, timezone


def get_current_time() -> datetime:
    delta = timedelta(hours=5, minutes=0)
    return datetime.now(timezone.utc) + delta

def date_from_timestamp(t) -> datetime:
    return datetime.fromtimestamp(t)

def get_timestamp_last_week():
    return int((get_current_time() - timedelta(weeks=1)).replace(hour=0, minute=0, microsecond=0, second=0).timestamp()) 