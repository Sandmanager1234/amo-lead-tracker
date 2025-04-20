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

def get_today(t: int):
    dt = date_from_timestamp(t)
    today = dt.replace(hour=0, microsecond=0, minute=0, second=0)
    return int(today.timestamp() - 18000)

def get_last_week_list(t) -> list:
    week = []
    td = get_today(t)
    week.append(td)
    for _ in range(7):
        yesterday = td - 86400
        td = yesterday
        week.append(yesterday)
    return week