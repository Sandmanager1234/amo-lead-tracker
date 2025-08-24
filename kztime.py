
from datetime import datetime, timedelta, timezone


def get_local_datetime(ts: int = None):
    if not ts:
        return datetime.now(timezone(timedelta(hours=5)))
    return datetime.fromtimestamp(ts, timezone(timedelta(hours=5)))


def get_today_info(dt: datetime = None):
    if not dt:
        dt = get_local_datetime()
    today = dt.replace(hour=0, microsecond=0, minute=0, second=0)
    start_ts = int(today.timestamp())
    end_ts = start_ts + 86399
    return start_ts, end_ts, today


def get_last_week_list(ts: int = None) -> list:
    week = []
    if ts:
        today = get_local_datetime(ts)
    else:
        today = get_local_datetime()
    week.append(today)
    for _ in range(6):
        today -= timedelta(days=1)
        week.append(today)
    return week


def get_last_month():
    dt = get_local_datetime()
    return get_today_info(dt - timedelta(days=30))


if __name__ == '__main__':
    print(get_last_month())
