import time
from datetime import datetime, timedelta, timezone


def get_unix_dt() -> datetime:
    return datetime.now(timezone.utc)

 
def date_from_timestamp(t: int) -> datetime:
    return datetime.fromtimestamp(t, timezone.utc)


def get_local_time(ts: int = None) -> int | datetime:
    if not ts:
        return get_unix_dt() + timedelta(hours=5)
    return ts + 18000


def get_timestamp_last_week() -> int:
    return int((get_unix_dt() - timedelta(weeks=1)).replace(hour=0, minute=0, microsecond=0, second=0).timestamp()) 