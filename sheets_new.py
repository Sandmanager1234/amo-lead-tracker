import datetime
from typing import Any, Iterable


import gspread
from loguru import logger
from time_new import KZ_TIMEZONE

MONTHS = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
}

PIPELINE_RANGES = {
        "astana": [46, 61],
        "almaty": [26, 41],
        "online": [3, 22],
    }

def get_range_by_pipeline(pipeline: str) -> list[int]:
    return PIPELINE_RANGES[pipeline] if pipeline in PIPELINE_RANGES else None


def get_letter_by_timestamp(timestamp: int):
    dt = datetime.datetime.fromtimestamp(timestamp, KZ_TIMEZONE)
    day = dt.day
    col_index = day + 1

    result = ""
    while col_index > 0:
        col_index, remainder = divmod(col_index - 1, 26)
        result = chr(65 + remainder) + result
    return result


class SheetWorker:
    def __init__(self):
        self.table = gspread.service_account(filename="test_key.json").open_by_key(
            "1ZyQO0bkGHvph0dxU2rDVOMwzflt7in0KR4yldT6QGVs"
        )

    def insert_col(
        self,
        data: dict[Any],
        range: list[int],
        letter: str,
        sheet: str = "Лист1",
        include_other_city: bool = False,
    ):
        worksheet = self.table.worksheet(sheet)

        range = f"{letter}{range[0]}:{letter}{range[1]}"

        data = [
            [value]
            for group in data.values()
            for key, value in group.items()
            if not (key == "other_city" and not include_other_city)
        ]

        worksheet.update(data, range)


def create_template():
    pipelines = ["АЛМАТА", "АСТАНА", "ОНЛАЙН"]

    total_fields = {
        "all_total": "Кол-во лидов общее",
        "target_total": "Кол-во лидов таргет",
        "zvonobot_total": "Кол-во лидов звонобот",
        "other_total": "Кол-во лидов прочее",
    }

    proccesed_fields = {
        "all_processed": "Кол-во обработано лидов общее",
        "target_processed": "Кол-во обработано лидов таргет",
        "zvonobot_processed": "Кол-во обработано лидов звонобот",
        "other_processed": "Кол-во обработано лидов прочее",
    }

    qualified_fields = {
        "all_qualified": "Кол-во квал лидов общее",
        "target_qualified": "Кол-во квал лидов таргет",
        "zvonobot_qualified": "Кол-во квал лидов звонобот",
        "other_qualified": "Кол-во квал лидов прочее",
    }

    success_fields = {
        "all_success": "Кол-во успехов общее",
        "target_success": "Кол-во успехов таргет",
        "zvonobot_success": "Кол-во успехов звонобот",
        "other_success": "Кол-во успехов прочее",
    }

    extra_fields = {
        "other_city_total": "Кол-во лидов другой город",
        "other_city_processed": "Кол-во обработано лидов другой город",
        "other_city_qualified": "Кол-во квал лидов другой город",
        "other_city_success": "Кол-во успехов другой город",
    }

    sw = SheetWorker()
    ws = sw.table.add_worksheet(
        f"{MONTHS[datetime.datetime.now().month]} + {datetime.datetime.now().year}",
        90,
        33
    )
    


