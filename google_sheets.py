import os
import time
import gspread
from dotenv import load_dotenv
from gspread_formatting import set_frozen, set_column_width, CellFormat, TextFormat, Color
from gspread_formatting.conditionals import (    
    get_conditional_format_rules, 
    ConditionalFormatRule, 
    GridRange,
    BooleanCondition,
    BooleanRule,
                                             )
from kztime import date_from_timestamp, get_local_time
from loguru import logger


load_dotenv()

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
            'Кол-во обработанных лидов таргет',
            '% обработки таргет',
            'Кол-во обработанных лидов звонобот',
            '% обработки звонобот',
            'Кол-во обработанных лидов прочее',
            '% обработки прочее',
            '% обработки',
            '',
            'Кол-во квал лидов',
            'Кол-во квал лидов таргет',
            'Квалификация таргет',
            'Кол-во квал лидов звонобот',
            'Квалификация звонобот',
            'Кол-во квал лидов прочее',
            'Квалификация прочее',
            '% Квалификации',
            '',
            'Кол-во успешек',
            'Кол-во успешек таргет',
            '% продаж с таргета',
            '% продаж с таргета с квала',
            'Кол-во успешек звонобот',
            '% продаж с звонобот',
            '% продаж с звонобот с квала',
            'Кол-во успешек прочее',
            '% продаж с прочее',
            '% продаж с прочее с квала',
            'Конверсия из лида в продажу',
            'Конверсия из квал-лида в продажу',
            ''
        ]
        rows_title_online = [
            'Кол-во лидов общее',
            'Кол-во лидов таргет',
            'Кол-во лидов звонобот',
            'Кол-во лидов Другой город',
            'Кол-во лидов прочее',
            '',
            'Кол-во обработанных лидов',
            'Кол-во обработанных лидов таргет',
            '% обработки таргет',
            'Кол-во обработанных лидов звонобот',
            '% обработки звонобот',
            'Кол-во обработанных лидов Другой город',
            '% обработки Другой город',
            'Кол-во обработанных лидов прочее',
            '% обработки прочее',
            '% обработки',
            '',
            'Кол-во квал лидов',
            'Кол-во квал лидов таргет',
            'Квалификация таргет',
            'Кол-во квал лидов звонобот',
            'Квалификация звонобот',
            'Кол-во квал лидов Другой город',
            'Квалификация Другой город',
            'Кол-во квал лидов прочее',
            'Квалификация прочее',
            '% Квалификации',
            '',
            'Кол-во успешек',
            'Кол-во успешек таргет',
            '% продаж с таргета',
            '% продаж с таргета с квала',
            'Кол-во успешек звонобот',
            '% продаж с звонобот',
            '% продаж с звонобот с квала',
            'Кол-во успешек Другой город',
            '% продаж с Другой город',
            '% продаж с Другой город с квала',
            'Кол-во успешек прочее',
            '% продаж с прочее',
            '% продаж с прочее с квала',
            'Конверсия из лида в продажу',
            'Конверсия из квал-лида в продажу',
            ''
        ]

        for city in ['АЛМАТА', 'АСТАНА']:
            first_col.append('План по лидам')
            first_col.append(city)
            first_col.extend(rows_title)
        first_col.extend(['План по лидам', 'ОНЛАЙН'])
        first_col.extend(rows_title_online)
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
            set_column_width(ws, 'A', 200)
            return ws
        except Exception as e:
            logger.error(f'Ошибка создания таблицы: {e}')
            raise
    
    def set_rules(self, ws):
        rules = [
            {
                'formula': '= B3 > $B1 * 1,2',
                'color': {
                    'r': 0.2352,
                    'g': 0.4705,
                    'b': 0.8470
                }
            },
            {
                'formula': '= B3 > $B1 * 0,9',
                'color': {
                    'r': 0.0039,
                    'g': 1.0,
                    'b': 0.0
                }
            },
            {
                'formula': '= B3 > $B1 * 0,8',
                'color': {
                    'r': 1.0,
                    'g': 1.0,
                    'b': 0.0
                }
            },
            {
                'formula': '= B3 > $B1 * 0,6',
                'color': {
                    'r': 0.8784,
                    'g': 0.4,
                    'b': 0.4
                }
            },
            {
                'formula': '= B3 <= $B1 * 0,6',
                'color': {
                    'r': 1.0,
                    'g': 0.0,
                    'b': 0.0
                }
            }
        ]
        cond_rules = get_conditional_format_rules(ws)
        for rule in rules:
            cond_rule = ConditionalFormatRule(
                ranges=[GridRange.from_a1_range('B3:AF3', ws), GridRange.from_a1_range('B3:AF3', ws), GridRange.from_a1_range('B41:AF41', ws), GridRange.from_a1_range('B79:AF79', ws)],
                booleanRule=BooleanRule(
                    condition=BooleanCondition('CUSTOM_FORMULA', [rule['formula']]),
                    format=CellFormat(textFormat=TextFormat(bold=True), backgroundColor=Color(rule['color']['r'],rule['color']['g'],rule['color']['b']))
                )
            )
            cond_rules.rules.append(cond_rule)
        cond_rules.save()

    def insert_value(self, row, col, timestamp: int = None):
        """Вставляет строку данных на указанный индекс."""
        try:
            logger.info(f"Вставка +1 на позицию {row}:{col}")
            ws = self.get_sheet(timestamp)
            value = ws.cell(row, col).value
            time.sleep(0.3)
            value = int(value) if value else 0
            ws.update_cell(row, col, value + 1)
            time.sleep(0.3)
            logger.info("Значение успешно вставлена")
        except Exception as e:
            logger.error(f"Ошибка при вставке значения: {e}")
            raise

    def insert_col(self, row, col, values, today_ts):
        ws = self.get_sheet(today_ts)
        ws.update(values, f'{LETTERS[col]}{row}:{LETTERS[col]}{row + len(values)}', raw=False)

    def minus_value(self, row, col):
        try:
            logger.info(f"Вставка -1 на позицию {row}:{col}")
            ws = self.get_sheet()
            value = ws.cell(row, col).value
            time.sleep(0.3)
            value = int(value) if value else 0
            ws.update_cell(row, col, value - 1)
            time.sleep(0.3)
            logger.info("Значение успешно вставлена")
        except Exception as e:
            logger.error(f"Ошибка при вставке значения: {e}")
            raise

    def insert_val(self, row, col, values, today_ts):
        ws = self.get_sheet(today_ts)
        for value in values:
            ws.update_cell(row, col, value)
            logger.info(f'Запись обновлена на позиции {row}:{col}')
            time.sleep(0.3)
            row += 1
        
if __name__ == '__main__':
    gs = GoogleSheets()
    # ws = gs.get_sheet(9999999999)
    ws = gs.create_new_sheet('test sheet')
    # print(Constructor.get_date_row())