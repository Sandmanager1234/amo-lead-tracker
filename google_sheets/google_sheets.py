import os
import time
import gspread
import datetime
from dotenv import load_dotenv
from gspread_formatting import set_frozen, set_column_width
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

    def get_sheet(self, month: int, year: int, today):
        sheet_name = f'{self.tg.MONTH[month]} (ОП) {year}'
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
            ws = self.table.add_worksheet(f'{self.tg.MONTH[month]} (ОП) {today.year}', 256, 256)
            ws.insert_cols(shablon, value_input_option="USER_ENTERED")
            self.beautify_sheet(ws)
            time.sleep(1)
            return ws
        except Exception as ex:
            logger.error(f'Ошибка создания таблицы Маркетинг: {ex}')
            raise
            
    
    def get_vertical_sheet(self, month, year, pipe) -> gspread.Worksheet:
        sheet_name = f'{self.tg.pipe_names_rus.get(pipe, pipe)} {self.tg.MONTH[month]} {year}'
        try:
            ws = self.table.worksheet(sheet_name)
            time.sleep(0.3)
        except gspread.WorksheetNotFound as ex:
            logger.warning(f'Лист {sheet_name} не найден. Ошибка: {ex}')
            ws = self.create_vertical_worksheet(sheet_name, pipe)
        except Exception as ex:
            logger.warning(f'Ошибка получения листа: {ex}')
            time.sleep(30)
            ws = self.get_vertical_sheet(month, year, pipe)
        return ws

    def create_vertical_worksheet(self, sheet_name: str, pipe):
        try:
            shablon = self.tg.create_vertical_shablon(pipe)
            ws = self.table.add_worksheet(sheet_name, 128, 128)
            ws.insert_rows(shablon, value_input_option="USER_ENTERED")
            self.beautify_vertical_sheet(ws, pipe=='online')
            return ws
        except Exception as ex:
            logger.error(f'Ошибка создания таблицы Маркетинг: {ex}')   
            raise
    
    def beautify_vertical_sheet(self, ws: gspread.Worksheet, is_online=False):
        try:
            # COLOR CELLS to RED
            ws.format(
                [
                    'A1:A2'
                ],
                {
                    "backgroundColor": {
                        "red": 0.9569,
                        "green": 0.8,
                        "blue": 0.8
                    },
                }
            )
            # color to blue
            ws.format(
                [
                    'B2:F33' if is_online else 'B2:E33'
                ],
                {
                    "backgroundColor": {
                        "red": 0.7882,
                        "green": 0.8549,
                        "blue": 0.9725
                    },
                }
            )
            # PURPLE
            ws.format(
                [
                    'H2:Q33' if is_online else 'G2:N33'
                ],
                {
                    "backgroundColor": {
                        "red": 0.851,
                        "green": 0.8235,
                        "blue": 0.9137
                    },
                }
            )
            time.sleep(0.9)
            # YELLOW
            ws.format(
                [
                    'S2:AB33' if is_online else 'P2:W33'
                ],
                {
                    "backgroundColor": {
                        "red": 1.0,
                        "green": 0.9490,
                        "blue": 0.8
                    },
                }
            )
            # GREEN
            ws.format(
                [
                    'AD2:AR33' if is_online else 'Y2:AJ33'
                ],
                {
                    "backgroundColor": {
                        "red": 0.851,
                        "green": 0.9176,
                        "blue": 0.8275
                    },
                }
            )
            # color cells to rgb(102, 102, 102) GREY
            ws.format([
                    'G2:G33' if is_online else 'F2:F33',
                    'R2:R33' if is_online else 'O2:O33',
                    'AC2:AC33' if is_online else 'X2:X33'
                ], {
                    "backgroundColor": {
                            "red": 0.4,
                            "green": 0.4,
                            "blue": 0.4
                    },
                }
            )
            time.sleep(0.7)
            ws.format( #ALL
                [
                    'A1:AR33' if is_online else 'A1:AJ33',
                ],
                {
                'borders': {
                        "top": {
                            'style': 'SOLID'
                        },
                        "right": {
                            'style': 'SOLID'
                        },
                        "left": {
                            'style': 'SOLID'
                        },
                        "bottom": {
                            'style': 'SOLID'
                        }
                    } 
                }
            )
            ws.format( #BOTTOM
                [
                    'A2:AR2' if is_online else 'A2:AJ2',
                ],
                {
                'borders': {
                        "top": {
                            'style': 'SOLID'
                        },
                        "right": {
                            'style': 'SOLID'
                        },
                        "left": {
                            'style': 'SOLID'
                        },
                        "bottom": {
                            'style': 'SOLID_THICK'
                        }
                    } 
                }
            )
            time.sleep(0.6)
            ws.format(['A1:AR33'], {     
                'wrapStrategy': 'WRAP',
                'horizontalAlignment': 'CENTER',
                "verticalAlignment": 'MIDDLE'
            })
            ws.format(['A1'], {     
                'textFormat': {
                    "fontSize": 20,
                    "bold": True
                }
            })
            set_column_width(ws, 'A', 200)
        except Exception as ex:
            logger.error(f'Ошибка формтирования таблицы: {ex}')
            time.sleep(10)
            self.beautify_vertical_sheet(ws)        

    def beautify_sheet(self, ws: gspread.Worksheet):
         # MERGE CELLS
        try:
            merges = []
            for i in range(3):
                merges.extend(
                    [
                        {"range": f"A{1 + 40 * i}:B{2 + 40 * i}"},
                        {"range": f"A{3 + 40 * i}:B{4 + 40 * i}"},
                        {"range": f"C{3 + 40 * i}:C{4 + 40 * i}"},
                        {"range": f"D{3 + 40 * i}:D{4 + 40 * i}"},
                        {"range": f"E{3 + 40 * i}:E{4 + 40 * i}"},
                        *[{"range": f"{self.tg.convert_num_to_letters(6 + 9 * j)}{3 + 40 * i}:{self.tg.convert_num_to_letters(7 + 9 * j)}{3 + 40 * i}"} for j in range(5)]
                    ]
                )
            for i in range(2):
                merges.extend(
                    [
                        {"range": f"A{5 + 40 * i}:A{8 + 40 * i}"},
                        {"range": f"A{10 + 40 * i}:A{17 + 40 * i}"},
                        {"range": f"A{19 + 40 * i}:A{26 + 40 * i}"},
                        {"range": f"A{28 + 40 * i}:A{39 + 40 * i}"}
                    ]
                )
            merges.extend(
                [
                    {"range": "A85:A89"},
                    {"range": "A91:A100"},
                    {"range": "A102:A111"},
                    {"range": "A113:A127"}
                ]
            )
            ws.batch_merge(
                merges
            )
            # COLOR CELLS to RED
            ws.format(
                [
                    'A1:B2',
                    'A3:AX4',
                    'A41:B42',
                    'A43:AX44',
                    'A81:B82',
                    'A83:AX84'
                ],
                {
                    "backgroundColor": {
                        "red": 0.9569,
                        "green": 0.8,
                        "blue": 0.8
                    },
                }
            )
            # color to blue
            ws.format(
                [
                    'A5:AX8',
                    'A45:AX48',
                    'A85:AX89'
                ],
                {
                    "backgroundColor": {
                        "red": 0.7882,
                        "green": 0.8549,
                        "blue": 0.9725
                    },
                }
            )
            # PURPLE
            ws.format(
                [
                    'A10:AX17',
                    'A50:AX57',
                    'A91:AX100'
                ],
                {
                    "backgroundColor": {
                        "red": 0.851,
                        "green": 0.8235,
                        "blue": 0.9137
                    },
                }
            )
            # YELLOW
            ws.format(
                [
                    'A19:AX26',
                    'A59:AX66',
                    'A102:AX111'
                ],
                {
                    "backgroundColor": {
                        "red": 1.0,
                        "green": 0.9490,
                        "blue": 0.8
                    },
                }
            )
            # GREEN
            ws.format(
                [
                    'A28:AX39',
                    'A68:AX79',
                    'A113:AX127'
                ],
                {
                    "backgroundColor": {
                        "red": 0.851,
                        "green": 0.9176,
                        "blue": 0.8275
                    },
                }
            )
            # color cells to rgb(102, 102, 102) GREY
            ws.format([
                    'A9:AX9', 'A18:AX18', 'A27:AX27', 'A40:AX40',
                    'A49:AX49', 'A58:AX58', 'A67:AX67', 'A80:AX80',
                    'A90:AX90', 'A101:AX101', 'A112:AX112', 'A128:AX128',
                ], {
                    "backgroundColor": {
                            "red": 0.4,
                            "green": 0.4,
                            "blue": 0.4
                    },
                }
            )
            time.sleep(1.2)
            # TEXT EDIT
            ws.format(
                [
                    '3:4',
                    '43:44',
                    '83:84',
                ], {
                'textFormat': {
                    "fontSize": 10,
                    "bold": True
                }
            })
            ws.format(['A1', 'A41', 'A81'], {
                'textFormat': {
                    "fontSize": 20,
                    "bold": True
                }
            })
            time.sleep(0.5)
            ws.format(['A5:A40', 'A45:A80', 'A85:A128'], {
                'textFormat': {
                    "fontSize": 15,
                    "bold": True
                }
            })
            ws.format(['A1:AX128'], {     
                'wrapStrategy': 'WRAP',
                'horizontalAlignment': 'CENTER',
                "verticalAlignment": 'MIDDLE'
            })
            time.sleep(0.8)
            # BORDERS 
            ws.format( #ALL
                [
                    'A3:AX40',
                    'A43:AX80',
                    'A83:AX128',
                ],
                {
                'borders': {
                        "top": {
                            'style': 'SOLID'
                        },
                        "right": {
                            'style': 'SOLID'
                        },
                        "left": {
                            'style': 'SOLID'
                        },
                        "bottom": {
                            'style': 'SOLID'
                        }
                    } 
                }
            )
            ws.format(  # TOP
                [
                    'A2:AX2',
                    'A42:AX42',
                    'A82:AX82'
                ],
                {  
                    'borders': {
                        "bottom": {
                            'style': 'SOLID_THICK'
                        }
                    }
                }
            )
            time.sleep(0.9)
            ws.format(  # BOTTOM
                [
                    'A41:AX41',
                    'A81:AX81',
                    'A129:AX129'
                ],
                {  
                    'borders': {
                        "top": {
                            'style': 'SOLID_THICK'
                        }
                    }
                }
            )
            ws.format( # RIGHT SIDE
                [
                    'A6:B40',
                    'E6:E40', 'G6:G40', 
                    'N6:N40', 'P6:P40',
                    'W6:W40', 'Y6:Y40',
                    'AF6:AF40', 'AH6:AH40',
                    'AO6:AO40', 'AQ6:AQ40',
                    'AX6:AX40',

                    'A46:B80',
                    'E46:E80', 'G46:G80', 
                    'N46:N80', 'P46:P80',
                    'W46:W80', 'Y46:Y80',
                    'AF46:AF80', 'AH46:AH80',
                    'AO46:AO80', 'AQ46:AQ80',
                    'AX46:AX80',

                    'A86:B128',
                    'E86:E128', 'G86:G128', 
                    'N86:N128', 'P86:P128',
                    'W86:W128', 'Y86:Y128',
                    'AF86:AF128', 'AH86:AH128',
                    'AO86:AO128', 'AQ86:AQ128',
                    'AX86:AX128',
                ],
                {
                    'borders': {
                        "top": {
                            'style': 'SOLID'
                        },
                        "right": {
                            'style': 'SOLID_THICK'
                        },
                        "left": {
                            'style': 'SOLID'
                        },
                        "bottom": {
                            'style': 'SOLID'
                        },
                    }
                }
            )
            time.sleep(0.4)
            ws.format( # RIGHT UP ANGLE SIDE
                [
                    'A5', 
                    'B5',
                    'E5', 'G5', 'F3:G3',
                    'N3', 'N5', 'P5', 'O3:P3',
                    'W3', 'W5', 'Y5', 'X3:Y3',
                    'AF3', 'AF5', 'AH5', 'AG3:AH3',
                    'AO3', 'AO5', 'AQ5', 'AP3:AQ3',
                    'AX5', 'AX3',

                    'A45', 
                    'B45',
                    'E45', 'G45', 'F43:G43',
                    'N43', 'N45', 'P45', 'O43:P43',
                    'W43', 'W45', 'Y45', 'X43:Y43',
                    'AF43', 'AF45', 'AH45', 'AG43:AH43',
                    'AO43', 'AO45', 'AQ45', 'AP43:AQ43',
                    'AX45', 'AX43',

                    'A85', 
                    'B85',
                    'E85', 'G85', 'F83:G83',
                    'N83', 'N85', 'P85', 'O83:P83',
                    'W83', 'W85', 'Y85', 'X83:Y83',
                    'AF83', 'AF85', 'AH85', 'AG83:AH83',
                    'AO83', 'AO85', 'AQ85', 'AP83:AQ83',
                    'AX85', 'AX83'
                ],
                {
                    'borders': {
                        "top": {
                            'style': 'SOLID_THICK'
                        },
                        "right": {
                            'style': 'SOLID_THICK'
                        },
                        "left": {
                            'style': 'SOLID'
                        },
                        "bottom": {
                            'style': 'SOLID'
                        },
                    }
                }
            )
            ws.format( # RIGHT HEAD SOLID SIDE
                [
                    'B1', 'B3', 'E3',
                    'B41', 'B43', 'E43',
                    'B81', 'B83', 'E83'
                ],
                {
                    'borders': {
                        "top": {
                            'style': 'SOLID_THICK'
                        },
                        "right": {
                            'style': 'SOLID_THICK'
                        },
                        "left": {
                            'style': 'SOLID'
                        },
                        "bottom": {
                            'style': 'SOLID_THICK'
                        },
                    }
                }
            )
            time.sleep(0.3)
            ws.format( #  HEAD BOT SIDE
                [
                    'F4', 'H4:M4',
                    'O4', 'Q4:V4',
                    'X4', 'Z4:AE4',
                    'AG4', 'AI4:AN4',
                    'AP4', 'AR4:AW4',

                    'F44', 'H44:M44',
                    'O44', 'Q44:V44',
                    'X44', 'Z44:AE44',
                    'AG44', 'AI44:AN44',
                    'AP44', 'AR44:AW44',

                    'F84', 'H84:M84',
                    'O84', 'Q84:V84',
                    'X84', 'Z84:AE84',
                    'AG84', 'AI84:AN84',
                    'AP84', 'AR84:AW84'
                ],
                {
                    'borders': {
                        "top": {
                            'style': 'SOLID'
                        },
                        "right": {
                            'style': 'SOLID'
                        },
                        "left": {
                            'style': 'SOLID'
                        },
                        "bottom": {
                            'style': 'SOLID_THICK'
                        },
                    }
                }
            )
            ws.format( # RIGHT HEAD SOLID SIDE
                [
                    'C3', 'D3',
                    'C43', 'D43',
                    'C83', 'D83'
                ],
                {
                    'borders': {
                        "top": {
                            'style': 'SOLID_THICK'
                        },
                        "right": {
                            'style': 'SOLID'
                        },
                        "left": {
                            'style': 'SOLID'
                        },
                        "bottom": {
                            'style': 'SOLID_THICK'
                        },
                    }
                }
            )
            time.sleep(0.8)
            ws.format( # RIGHT BOT HEAD SOLID SIDE
                [
                    'G4', 'N4',
                    'P4', 'W4',
                    'Y4', 'AF4',
                    'AH4', 'AO4',
                    'AQ4', 'AX4',

                    'G44', 'N44',
                    'P44', 'W44',
                    'Y44', 'AF44',
                    'AH44', 'AO44',
                    'AQ44', 'AX44',

                    'G84', 'N84',
                    'P84', 'W84',
                    'Y84', 'AF84',
                    'AH84', 'AO84',
                    'AQ84', 'AX84'
                ],
                {
                    'borders': {
                        "top": {
                            'style': 'SOLID'
                        },
                        "right": {
                            'style': 'SOLID_THICK'
                        },
                        "left": {
                            'style': 'SOLID'
                        },
                        "bottom": {
                            'style': 'SOLID_THICK'
                        },
                    }
                }
            )
            # CELLS FORMAT
            ws.format(
                [
                    '12:12', '14:14', '16:17',
                    '21:21', '23:23', '25:26',
                    '30:31', '33:34', '36:39',

                    '52:52', '54:54', '56:57',
                    '61:61', '63:63', '65:66',
                    '70:71', '73:74', '76:79',

                    '93:93', '95:95', '97:97', '99:100',
                    '104:104', '106:106', '108:108', '110:111',
                    '115:116', '118:119', '121:122', '124:128'
                ], {
                'numberFormat': {'type': 'PERCENT'}
            })
            time.sleep(0.6)
            # CELL SIZE
            set_column_width(ws, 'A', 190)
            set_column_width(ws, 'B', 290)
            # FREEZE
            set_frozen(ws, cols=2)
        except Exception as ex:
            logger.error(f'Ошибка форматирования таблицы: {ex}')
            time.sleep(10)
            self.beautify_sheet(ws)


    def insert_leads_data(self, leads_data: dict, start_day: datetime):
        try:
            insert_data = {}
            for _ in range(31):
                week_num, month, year = self.tg.get_weeknum(start_day)
                week_num -= 1
                weekday = start_day.date().isoweekday()
                if year not in insert_data:
                    insert_data[year] = {}
                if month not in insert_data[year]:
                    insert_data[year][month] = {}
                    insert_data[year][month]['last_day'] = start_day
                    insert_data[year][month]['pipes'] = {}
                for pipe in leads_data:
                    if pipe not in insert_data[year][month]['pipes']:
                        insert_data[year][month]['pipes'][pipe] = {}
                    if week_num not in insert_data[year][month]['pipes'][pipe]:
                        insert_data[year][month]['pipes'][pipe][week_num] = [[] for _ in range(7)]
                    tmp_data = leads_data[pipe][start_day.year][start_day.month][start_day.day]
                    if pipe != 'online':
                        insert_data[year][month]['pipes'][pipe][week_num][weekday - 1] = [ 
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
                        insert_data[year][month]['pipes'][pipe][week_num][weekday - 1] = [
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
                start_day += datetime.timedelta(days=1)

            for year in insert_data:
                for month in insert_data[year]:
                    day = insert_data[year][month]['last_day']
                    ws = self.get_sheet(month, year, day)
                    for pipe in insert_data[year][month]['pipes']:
                        len_data = 35 if pipe != 'online' else 43
                        for weeknum in insert_data[year][month]['pipes'][pipe]:
                            week_data = insert_data[year][month]['pipes'][pipe][weeknum]
                            ws.update(
                                week_data,
                                self.tg.get_week_range(pipe, weeknum, len_data),
                                value_input_option="USER_ENTERED",
                                major_dimension='COLUMNS'
                            )
                            time.sleep(0.5)
                            logger.info(f'Добавлена статистика за Неделю: {weeknum} в воронке {pipe}. Диапазон: {self.tg.get_week_range(pipe, weeknum, len_data)}')
        except Exception as ex:
            from traceback import print_exc
            print(print_exc())
            logger.error(f'Ошибка записи: {ex}')
        
    def insert_leads_data_vertical(self, leads_data: dict):
        try:
            insert_data = {}
            for pipe in leads_data:
                insert_data[pipe] = {}
                for year in leads_data[pipe]:
                    insert_data[pipe][year] = {}
                    for month in leads_data[pipe][year]:
                        insert_data[pipe][year][month] = {}
                        insert_data[pipe][year][month]['data'] = []
                        min_day = 31
                        for day in leads_data[pipe][year][month]:
                            tmp_data = leads_data[pipe][year][month][day]
                            min_day = min(day, min_day)
                            if pipe != "online":
                                insert_data[pipe][year][month]['data'].append(
                                    [
                                    f'{year}.{month}.{day}',
                                    tmp_data[0],
                                    tmp_data[1],
                                    tmp_data[2],
                                    tmp_data[3],
                                    '',
                                    tmp_data[4],
                                    tmp_data[5],
                                    f'=H{2 + day}/C{2 + day}',
                                    tmp_data[6],
                                    f'=J{2 + day}/D{2 + day}',
                                    tmp_data[7],
                                    f'=L{2 + day}/E{2 + day}',
                                    f'=G{2 + day}/B{2 + day}',
                                    '',
                                    tmp_data[8],
                                    tmp_data[9],
                                    f'=Q{2 + day}/H{2 + day}',
                                    tmp_data[10],
                                    f'=S{2 + day}/J{2 + day}',
                                    tmp_data[11],
                                    f'=U{2 + day}/M{2 + day}',
                                    f'=P{2 + day}/G{2 + day}',
                                    '',
                                    tmp_data[12],
                                    tmp_data[13],
                                    f'=Z{2 + day}/H{2 + day}',
                                    f'=Z{2 + day}/Q{2 + day}',
                                    tmp_data[14],
                                    f'=AC{2 + day}/J{2 + day}',
                                    f'=AC{2 + day}/S{2 + day}',
                                    tmp_data[15],
                                    f'=AF{2 + day}/L{2 + day}',
                                    f'=AF{2 + day}/U{2 + day}',
                                    f'=Y{2 + day}/G{2 + day}',
                                    f'=Y{2 + day}/P{2 + day}'
                                ]
                                )
                            else:
                                insert_data[pipe][year][month]['data'].append( [
                                    f'{year}.{month}.{day}',
                                    tmp_data[0],
                                    tmp_data[1],
                                    tmp_data[2],
                                    tmp_data[3],
                                    tmp_data[4],
                                    '',
                                    tmp_data[5],
                                    tmp_data[6],
                                    f'=I{2 + day}/C{2 + day}',
                                    tmp_data[7],
                                    f'=K{2 + day}/D{2 + day}',
                                    tmp_data[8],
                                    f'=M{2 + day}/E{2 + day}',
                                    tmp_data[9],
                                    f'=O{2 + day}/F{2 + day}',
                                    f'=H{2 + day}/B{2 + day}',
                                    '',
                                    tmp_data[10],
                                    tmp_data[11],
                                    f'=T{2 + day}/I{2 + day}',
                                    tmp_data[12],
                                    f'=V{2 + day}/K{2 + day}',
                                    tmp_data[13],
                                    f'=X{2 + day}/M{2 + day}',
                                    tmp_data[14],
                                    f'=Z{2 + day}/O{2 + day}',
                                    f'=S{2 + day}/H{2 + day}',
                                    '',
                                    tmp_data[15],
                                    tmp_data[16],
                                    f'=AE{2 + day}/I{2 + day}',
                                    f'=AE{2 + day}/T{2 + day}',
                                    tmp_data[17],
                                    f'=AH{2 + day}/K{2 + day}',
                                    f'=AH{2 + day}/V{2 + day}',
                                    tmp_data[18],
                                    f'=AK{2 + day}/M{2 + day}',
                                    f'=AK{2 + day}/X{2 + day}',
                                    tmp_data[19],
                                    f'=AN{2 + day}/O{2 + day}',
                                    f'=AN{2 + day}/Z{2 + day}',
                                    f'=AD{2 + day}/H{2 + day}',
                                    f'=AD{2 + day}/S{2 + day}'
                                ]
                                )
                        insert_data[pipe][year][month]['first_day'] = min_day 
            for pipe in insert_data:
                for year in insert_data[pipe]:
                    for month in insert_data[pipe][year]:
                        min_day = insert_data[pipe][year][month]['first_day'] 
                        ws = self.get_vertical_sheet(month, year, pipe)
                        ws.update(
                            insert_data[pipe][year][month]['data'][::-1],
                            f'A{2 + min_day}:AR{33}',
                            value_input_option="USER_ENTERED"
                        )
                        time.sleep(0.3)
                        logger.info(f'Добавлена статистика за месяц: {self.tg.MONTH[month]} {year} в воронке {pipe}.')
        except Exception as ex:
            import traceback
            print(traceback.print_exc())
            logger.error(f'Ошибка записи: {ex}')

    
if __name__ == '__main__':
    gs = GoogleSheets()
    # ws = gs.get_sheet(9999999999)
    ws = gs.create_new_sheet('test sheet')
    # print(Constructor.get_date_row())