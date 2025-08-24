import os
import time
import gspread
import datetime
from dotenv import load_dotenv
from gspread_formatting import set_frozen, set_column_width, CellFormat, TextFormat, Color
from gspread_formatting.conditionals import (    
    get_conditional_format_rules, 
    ConditionalFormatRule, 
    GridRange,
    BooleanCondition,
    BooleanRule,
                                             )
from kztime import get_local_datetime
from loguru import logger

from google_sheets.template_generator import TemplateGenerator

load_dotenv()

class GoogleSheets:
    def __init__(self):
        try:
            logger.info("Инициализация GoogleSheets")
            gc = gspread.service_account(filename="credentials.json")
            self.table = gc.open_by_key(os.getenv("table_id"))
            self.tg = TemplateGenerator()
            logger.info("Успешное подключение к таблице")
        except Exception as e:
            logger.error(f"Ошибка при инициализации GoogleSheets: {e}")
            raise

    def get_sheet(self, month, year, today):
        sheet_name = f'{self.tg.MONTH[month]} {year}'
        try:
            ws = self.table.worksheet(sheet_name)
            time.sleep(0.3)
        except gspread.WorksheetNotFound as ex:
            logger.warning(f'Лист {sheet_name} не найден. Ошибка: {ex}')
            ws = self.create_worksheet(today)
        except Exception as ex:
            logger.warning(f'Ошибка получения листа: {ex}')
            time.sleep(30)
            ws = self.get_sheet(month, year, today)
        return ws

    def create_worksheet(self, today):
        try:
            shablon, month = self.tg.create_shablon(today)
            ws = self.table.add_worksheet(f'{self.tg.MONTH[month]} {today.year}', 128, 128)
            ws.insert_cols(shablon, value_input_option="USER_ENTERED")
            self.beutify_sheet(ws)
        except Exception as ex:
            logger.error(f'Ошибка создания таблицы Маркетинг: {ex}')    
        return ws

    def create_new_sheet(self, sheet_name):
        try:
            ws = self.table.add_worksheet(sheet_name, 123, 33)
            dates = Constructor.get_date_row()
            for i in [2, 40, 78]:
                ws.insert_row(dates, i, value_input_option="USER_ENTERED")

            ws.insert_cols(Constructor.get_first_col(), 1, value_input_option="USER_ENTERED")
            ws.format(['10:10', '12:12', '14:15', '19:19', '21:21', '23:24', '28:29', '31:32', '34:37',
                        '48:48', '59:59', '61:62', '57:57', '59:59', '61:62', '66:67', '69:70', '72:75',
                        '87:87', '89:89', '91:91', '93:94', '98:98', '100:100', '102:102', '104:105',
                        '109:110', '112:113', '115:116', '118:121'
                   ], {
                'numberFormat': {'type': 'PERCENT'}
            })
            # BLUE
            ws.format(['3:6', '41:44', '79:83'], {
                "backgroundColor": {
                    
                        "red": 0.7882,
                        "green": 0.8549,
                        "blue": 0.9725
                    
                },
            })
            # PURPLE
            ws.format(['8:15', '46:53', '85:94'], {
                "backgroundColor": {
                "red": 0.851,
                "green": 0.8235,
                "blue": 0.9137
                },
            })
            # YELLOW
            ws.format(['17:24', '55:62', '96:105', 'B1', 'B39', 'B77'], {
                "backgroundColor": {
                "red": 1.0,
                "green": 0.9490,
                "blue": 0.8
                },
            })
            # GREEN
            ws.format(['26:37', '64:75', '107:121'], {
                "backgroundColor": {
                "red": 0.851,
                "green": 0.9176,
                "blue": 0.8275
                },
            })
            # RED
            ws.format(['A2', 'A40', 'A78'], {
                "backgroundColor": {
                "red": 0.9569,
                "green": 0.8,
                "blue": 0.8
                },
                'textFormat': {
                    "fontSize": 14,
                    "bold": True
                }
            })
            ws.format(['B1', 'B39', 'B77'], {
                'textFormat': {
                    "fontSize": 20,
                    "bold": True
                }
            })
            ws.format(['A1', 'A39', 'A77'], {
                'textFormat': {
                    "fontSize": 14,
                    "bold": True
                }
            })
            self.set_rules(ws)
            time.sleep(2)
            set_frozen(ws, cols=1)
            set_column_width(ws, 'A', 300)
            return ws
        except Exception as e:
            logger.error(f'Ошибка создания таблицы: {e}')
            raise

    def insert_leads_data(self, leads_data: dict, start_day: datetime):
        insert_data = {}
        for _ in range(30):
            week_num, month, year = self.tg.get_weeknum(start_day)
            weekday = start_day.date().isoweekday()
            if year not in insert_data:
                insert_data[year] = {}
            if month not in insert_data[year]:
                insert_data[year][month] = {}
                insert_data[year][month]['last_day'] = start_day
            for pipe in leads_data:
                if pipe not in insert_data[year][month]:
                    insert_data[year][month][pipe] = {}
                if week_num not in insert_data[year][month][pipe]:
                    insert_data[pipe][year][month][pipe][week_num] = [[] for _ in range(7)]
                tmp_data = leads_data[pipe][start_day.year][start_day.month][start_day.day]
                if pipe != 'online':
                    insert_data[year][month][pipe][week_num][weekday - 1] = [ 
                        tmp_data[0],
                        tmp_data[1],
                        tmp_data[2],
                        tmp_data[3],
                        '',
                        tmp_data[4],
                        tmp_data[5],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 11, 6),
                        tmp_data[6],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 13, 7),
                        tmp_data[7],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 15, 8),
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 10, 5),
                        '',
                        tmp_data[8],
                        tmp_data[9],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 20, 11),
                        tmp_data[10],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 22, 13),
                        tmp_data[11],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 24, 15),
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 19, 10),
                        '',
                        tmp_data[12],
                        tmp_data[13],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 29, 11),
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 29, 20),
                        tmp_data[14],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 32, 13),
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 32, 22),
                        tmp_data[15],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 35, 15),
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 35, 24),
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 28, 10),
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 28, 19)
                    ]
                else:
                    insert_data[year][month][pipe][week_num][weekday - 1] = [
                        tmp_data[0],
                        tmp_data[1],
                        tmp_data[2],
                        tmp_data[3],
                        tmp_data[4],
                        '',
                        tmp_data[5],
                        tmp_data[6],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 12, 6),
                        tmp_data[6],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 14, 7),
                        tmp_data[7],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 16, 8),
                        tmp_data[8],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 18, 9),
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 11, 5),
                        '',
                        tmp_data[9],
                        tmp_data[10],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 23, 12),
                        tmp_data[11],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 25, 14),
                        tmp_data[12],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 27, 16),
                        tmp_data[13],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 29, 18),
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 22, 11),
                        '',
                        tmp_data[14],
                        tmp_data[15],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 34, 12),
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 34, 23),
                        tmp_data[16],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 37, 14),
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 37, 25),
                        tmp_data[17],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 40, 16),
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 40, 27),
                        tmp_data[18],
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 43, 18),
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 43, 29),
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 33, 11),
                        self.tg.get_div_col_formula(week_num, pipe, weekday, 33, 22)
                    ]
            start_day -= datetime.timedelta(days=1)
        
        for year in insert_data:
            for month in insert_data[year]:
                day = insert_data[year][month]['last_day']
                ws = self.get_sheet(month, year, day)
                for pipe in insert_data[year][month]:
                    len_data = 35 if pipe != 'online' else 43
                    for weeknum in insert_data[year][month][pipe]:
                        week_data = insert_data[year][month][pipe][weeknum]
                        ws.update(
                            week_data,
                            self.tg.get_week_range(pipe, weeknum, len_data)
                        )
                        time.sleep(0.3)
                        logger.info(f'Добавлена статистика за Неделю: {weeknum} в воронке {pipe}. Диапазон: {self.tg.get_week_range(pipe, weeknum, len_data)}')
        
    def insert_leads_data_vertical(self, leads_data: dict):
        insert_data = {}
        for pipe in leads_data:
            insert_data[pipe] = {}
            for year in leads_data[pipe]:
                insert_data[pipe][year] = {}
                for month in leads_data[pipe][year]:
                    for day in leads_data[pipe][year][month]:
                        tmp_data = leads_data[pipe][year][month][day]
                        
                        if pipe != "online":
                            insert_data[pipe][year][month] = [
                                
                            ]
                        else:
                            insert_data[pipe][year][month] = [

                            ]

        for pipe in insert_data:
            for year in insert_data[pipe]:
                for month in insert_data[pipe][year]:
                    ws = self.get_vertical_sheet(month, year)
                    ws.insert_rows(
                        insert_data[pipe][year][month],
                        3 + 
                    )
                    logger.info(f'Добавлена статистика за месяц: {self.tg.MONTH[month]} {year} в воронке {pipe}. Диапазон: {self.tg.get_week_range(pipe, weeknum, len_data)}')
        


        


    def insert_records(self, record_statistic: dict, day_count, start_day):
        # i need few arrays by month which i can insert in table
        month_data = {}
        curr_week_num = -1
        prev_month = 0
        for _ in range(30):
            week_num, month = self.tg.get_weeknum(start_day)
            value = record_statistic.get(
                start_day.year, {}
            ).get(
                start_day.month, {}
            ).get(
                start_day.day, 0
            )
            if value != 0:
                day_count -= 1
            if month not in month_data:
                prev_month = month
                month_data[month] = {}
                month_data[month]['month'] = []
                month_data[month]['start_day'] = start_day
                month_data[month]['start_weeknum'] = week_num
            if curr_week_num == -1:
                curr_week_num = week_num
            if curr_week_num != week_num and prev_month == month:
                week_id = 7 + 9 * (week_num - 1)
                month_data[month]['month'].extend(['', f'=СУММ({self.tg.convert_num_to_letters(week_id + 1)}10:{self.tg.convert_num_to_letters(week_id + 7)}10)',])
                curr_week_num = week_num
            month_data[month]['month'].append(value)
            start_day += datetime.timedelta(days=1)
        for month in month_data:
            start_day = month_data[month]['start_day']
            week_num = month_data[month]['start_weeknum']
            ws = self.get_sheet(start_day, month)
            col_id = 5 + 2 * week_num + 7 * (week_num - 1) + start_day.isoweekday()
            ws.update(
                [month_data[month]['month']],
                f'{sg.convert_num_to_letters(col_id)}{10}:{sg.convert_num_to_letters(col_id + len(month_data[month]["month"]))}{10}',
                raw=False
            )
            logger.info(f'Добавлена статистика за месяц: {sg.MONTH[month]} в диапазон {sg.convert_num_to_letters(col_id)}{9}:{sg.convert_num_to_letters(col_id + len(month_data[month]["month"]))}{9}')

    
    
    
if __name__ == '__main__':
    gs = GoogleSheets()
    # ws = gs.get_sheet(9999999999)
    ws = gs.create_new_sheet('test sheet')
    # print(Constructor.get_date_row())