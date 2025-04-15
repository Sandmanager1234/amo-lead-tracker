import time
import gspread
from gspread_formatting import set_frozen, set_column_width
import os
from kztime import date_from_timestamp, get_local_time
from loguru import logger

MONTH = {
    1: 'Январь',
    2: 'Февраль',
    3: 'Март',
    4: 'Апрель',
    5: 'Май',
    6: 'Июнь',
    7: 'Июль',
    8: 'Август',
    9: 'Сентябрь',
    10: 'Октябрь',
    11: 'Ноябрь',
    12: 'Декабрь',
}

LETTERS = {
    1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F', 7: 'G', 8: 'H', 
    9: 'I', 10: 'J', 11: 'K', 12: 'L', 13: 'M', 14: 'N', 15: 'O', 16: 'P', 
    17: 'Q', 18: 'R', 19: 'S', 20: 'T', 21: 'U', 22: 'V', 23: 'W', 24: 'X', 
    25: 'Y', 26: 'Z',27: 'AA', 28: 'AB', 29: 'AC', 30: 'AD', 31: 'AE', 32: 'AF'
}


class Constructor:
    def get_first_col():
        first_col = []
        
        rows_title = [
            'Кол-во лидов общее',
            'Кол-во лидов таргет',
            'Кол-во лидов звонобот',
            'Кол-во лидов прочее',
            '',
            'Кол-во обработанных лидов',
            'Кол-во квал лидов',
            '% Квалификации',
            '',
            'Кол-во успешек',
            'Конверсия из лида в продажу',
            'Конверсия из квал-лида в продажу',
            ''
        ]
        for city in ['АЛМАТА', 'АСТАНА', 'ОНЛАЙН']:
            first_col.append(city)
            first_col.extend(rows_title)
        return [first_col]
    
    def get_date_row():
        month = str(get_local_time().month).zfill(2)
        return [f'{str(i).zfill(2)}.{month}' for i in range(1, 32)]
    
    def get_formules_rows():
        formulas = {}
        for row_index in [1, 15, 29]:
            formulas[row_index] = Constructor.get_date_row()
        for row_id in [2, 16, 30]:
            formulas[row_id] = []
            for i in range(1, 32):
                formulas[row_id].append(f'=СУММ({LETTERS[i]}{row_id+1}:{LETTERS[i]}{row_id+3})')
        for row_id in [9, 23, 37]:
            formulas[row_id] = []
            for i in range(1, 32):
                formulas[row_id].append(f'={LETTERS[i]}{row_id-1}/{LETTERS[i]}{row_id-2}')
        for row_id in [9, 23, 37]:
            formulas[row_id] = []
            for i in range(1, 32):
                formulas[row_id].append(f'={LETTERS[i]}{row_id-1}/{LETTERS[i]}{row_id-2}')
        for row_id in [12, 26, 40]:
            formulas[row_id] = []
            for i in range(1, 32):
                formulas[row_id].append(f'={LETTERS[i]}{row_id-1}/{LETTERS[i]}{row_id-10}')
        for row_id in [13, 27, 41]:
            formulas[row_id] = []
            for i in range(1, 32):
                formulas[row_id].append(f'={LETTERS[i]}{row_id-5}/{LETTERS[i]}{row_id-11}')
        return formulas


class GoogleSheets:
    def __init__(self):
        try:
            logger.info("Инициализация GoogleSheets")
            gc = gspread.service_account(filename="credentials.json")
            self.table = gc.open_by_key(os.getenv("table_id"))
            # self.ws = self.create_new_sheet()
            logger.info("Успешное подключение к таблице")
        except Exception as e:
            logger.error(f"Ошибка при инициализации GoogleSheets: {e}")
            raise

    def get_sheet(self, timestamp: int = None):
        try:
            if not timestamp:
                current_date = get_local_time()
            else:
                current_date = date_from_timestamp(timestamp)
            sheet_name = f'{MONTH[current_date.month]} {current_date.year}'
            ws = self.table.worksheet(sheet_name)
            time.sleep(0.3)
        except gspread.WorksheetNotFound as ex:
            logger.warning(f'Лист {sheet_name} не найден. Ошибка: {ex}')
            ws = self.create_new_sheet(sheet_name)
        except Exception as ex:
            logger.warning(f'Ошибка получения листа: {ex}')
            time.sleep(1)
            ws = self.get_sheet(timestamp)
        return ws

    def create_new_sheet(self, sheet_name):
        try:
            ws = self.table.add_worksheet(sheet_name, 42, 42)
            formulas = Constructor.get_formules_rows()
            rows = []
            for index in range(1, 43):
                if index not in formulas:
                    rows.append([''])
                else:
                    rows.append(formulas[index])
            ws.insert_rows(rows, 1, value_input_option="USER_ENTERED")
            ws.insert_cols(Constructor.get_first_col(), 1, value_input_option="USER_ENTERED")
            ws.format(['9:9', '12:12', '13:13', '23:23', '26:26', '27:27', '37:37', '40:40', '41:41'], {
                'numberFormat': {'type': 'PERCENT'}
            })
            ws.format(['2:5', '16:19', '30:33'], {
                "backgroundColor": {
                    
                        "red": 0.7882,
                        "green": 0.8549,
                        "blue": 0.9725
                    
                },
            })
            ws.format(['7:9', '21:23', '35:37'], {
                "backgroundColor": {
                "red": 0.851,
                "green": 0.8235,
                "blue": 0.9137
                },
            })
            ws.format(['11:13', '25:27', '39:41'], {
                "backgroundColor": {
                "red": 0.851,
                "green": 0.9176,
                "blue": 0.8275
                },
            })
            ws.format(['A1', 'A15', 'A29'], {
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
            time.sleep(2)
            set_frozen(ws, cols=1)
            set_column_width(ws, 'A', 200)
            return ws
        except Exception as e:
            logger.error(f'Ошибка создания таблицы: {e}')
            raise

    def insert_value(self, row, col, timestamp: int = None):
        """Вставляет строку данных на указанный индекс."""
        try:
            logger.info(f"Вставка +1 на позицию {row}:{col}")
            ws = self.get_sheet(timestamp)
            value = ws.cell(row, col).value
            time.sleep(0.22)
            value = int(value) if value else 0
            ws.update_cell(row, col, value + 1)
            time.sleep(0.22)
            logger.info("Значение успешно вставлена")
        except Exception as e:
            logger.error(f"Ошибка при вставке значения: {e}")
            raise

    def minus_value(self, row, col):
        try:
            logger.info(f"Вставка -1 на позицию {row}:{col}")
            ws = self.get_sheet()
            value = ws.cell(row, col).value
            time.sleep(0.22)
            value = int(value) if value else 0
            ws.update_cell(row, col, value - 1)
            time.sleep(0.22)
            logger.info("Значение успешно вставлена")
        except Exception as e:
            logger.error(f"Ошибка при вставке значения: {e}")
            raise