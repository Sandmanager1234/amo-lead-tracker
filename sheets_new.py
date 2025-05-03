import datetime
from typing import Any, Iterable


import gspread
from loguru import logger
from time_new import KZ_TIMEZONE


def get_range_by_pipeline(pipeline: str) -> list[int]:
    pipelines_ranges = {
        "astana": [46, 61],
        "almaty": [26, 41],
        "online": [3, 22],
    }
    return pipelines_ranges[pipeline] if pipeline in pipelines_ranges else None


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
            if not (key == 'other_city' and not include_other_city)
        ]

        worksheet.update(data, range)
