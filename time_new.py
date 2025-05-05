import datetime as dt

KZ_TIMEZONE = dt.timezone(dt.timedelta(hours=5))

def get_week_timestamps():
    now_kz = dt.datetime.now(tz=KZ_TIMEZONE)
    today_kz = dt.datetime.combine(now_kz.date(), dt.time(0, 0), tzinfo=KZ_TIMEZONE)
    
    # Начало текущего месяца
    month_start_kz = today_kz.replace(day=1)
    
    # Сколько дней прошло с начала месяца до сегодня (включительно)
    days_since_month_start = (today_kz - month_start_kz).days + 1
    
    # Сколько дней показать
    days_to_return = min(7, days_since_month_start)
    
    # Стартовая дата
    start_day_kz = today_kz - dt.timedelta(days=days_to_return - 1)

    week = []
    for i in range(days_to_return):
        day_start = start_day_kz + dt.timedelta(days=i)
        day_end = day_start + dt.timedelta(hours=23, minutes=59)

        ts_from = int(day_start.timestamp())
        ts_to = int(day_end.timestamp())

        week.append([ts_from, ts_to])

    return week


def get_yesterday():
   
    yesterday = (dt.datetime.now(KZ_TIMEZONE) - dt.timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return int(yesterday.timestamp())


def get_today():
    today = dt.datetime.now(KZ_TIMEZONE).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return int(today.timestamp())


def get_tomorrow():
    tomorrow = (dt.datetime.now(KZ_TIMEZONE) + dt.timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return int(tomorrow.timestamp())
