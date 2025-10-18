import os
import time
import gspread
import datetime
from dotenv import load_dotenv
from gspread_formatting import set_frozen, set_column_width
from loguru import logger

from google_sheets.template_generator import TemplateGenerator
from google_sheets.shablon import categories, categories_online, groups

# from template_generator import TemplateGenerator
# from shablon import categories, categories_online, groups


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
            ws = self.table.add_worksheet(f'{self.tg.MONTH[month]} (ОП) {today.year}', (5 + len(categories_online) * 8 + 11) * 3, 52)
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
            ws = self.table.add_worksheet(sheet_name, 45, 45)
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
            if is_online:
                categories_count = len(categories_online)
            else:
                categories_count = len(categories)
            ws.format(
                [
                    f'{self.tg.convert_num_to_letters(2)}2:{self.tg.convert_num_to_letters(2 + categories_count)}33'
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
                    f'{self.tg.convert_num_to_letters(4 + categories_count)}2:{self.tg.convert_num_to_letters(5 + categories_count * 3)}33'
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
                    f'{self.tg.convert_num_to_letters(7 + categories_count * 3)}2:{self.tg.convert_num_to_letters(8 + categories_count * 5)}33'
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
                    f'{self.tg.convert_num_to_letters(10 + categories_count * 5)}2:{self.tg.convert_num_to_letters(12 + categories_count * 8)}33'
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
                    f'{self.tg.convert_num_to_letters(3 + categories_count)}2:{self.tg.convert_num_to_letters(3 + categories_count)}33',
                    f'{self.tg.convert_num_to_letters(6 + categories_count * 3)}2:{self.tg.convert_num_to_letters(6 + categories_count * 3)}33',
                    f'{self.tg.convert_num_to_letters(9 + categories_count * 5)}2:{self.tg.convert_num_to_letters(9 + categories_count * 5)}33',
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
                    f'A1:{self.tg.convert_num_to_letters(12 + categories_count * 8)}33',
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
                    f'A2:{self.tg.convert_num_to_letters(12 + categories_count * 8)}2',
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
            ws.format([f'A1:{self.tg.convert_num_to_letters(12 + categories_count * 8)}33'], {     
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

            # TEXT FORMAT 
            percent_rows = []
            for j in range(3):
                for k in range(1, categories_count + 1):
                    if j == 2:
                        index_1 = 2 + (categories_count + 2) + (categories_count * 2 + 3) * j  + (3 * k) - 1
                        index_2 = 2 + (categories_count + 2) + (categories_count * 2 + 3) * j  + 3 * k
                        letter_1 = self.tg.convert_num_to_letters(index_1)
                        letter_2 = self.tg.convert_num_to_letters(index_2)
                        percent_rows.append(
                            f'{letter_1}:{letter_2}'
                        )
                    else:
                        index = 2 + (categories_count + 2) + (categories_count * 2 + 3) * j  + 2 * k
                        letter = self.tg.convert_num_to_letters(index)
                        percent_rows.append(
                            f'{letter}:{letter}'
                        )
                l1 = self.tg.convert_num_to_letters(2 + categories_count * 3 + 3)
                percent_rows.append(
                    f'{l1}:{l1}'
                )
                l2 = self.tg.convert_num_to_letters(
                    2 + categories_count * 5 + 6
                )
                percent_rows.append(f'{l2}:{l2}')
                l3 = self.tg.convert_num_to_letters(
                    2 + categories_count * 8 + 9
                )
                l4 = self.tg.convert_num_to_letters(
                    2 + categories_count * 8 + 10
                )
                percent_rows.append(f'{l3}:{l4}')
            ws.format(
                percent_rows, 
                {
                    'numberFormat': {'type': 'PERCENT'}
                }
            )

        except Exception as ex:
            logger.error(f'Ошибка формтирования таблицы: {ex}')
            time.sleep(10)
            self.beautify_vertical_sheet(ws)        

    def beautify_sheet(self, ws: gspread.Worksheet):
         # MERGE CELLS
        try:
            diff = 120
            merges = []
            for i in range(3):
                merges.extend(
                    [
                        {"range": f"A{1 + diff * i}:B{2 + diff * i}"},
                        {"range": f"A{3 + diff * i}:B{4 + diff * i}"},
                        {"range": f"C{3 + diff * i}:C{4 + diff * i}"},
                        {"range": f"D{3 + diff  * i}:D{4 + diff * i}"},
                        {"range": f"E{3 + diff * i}:E{4 + diff * i}"},
                        *[{"range": f"{self.tg.convert_num_to_letters(6 + 9 * j)}{3 + diff * i}:{self.tg.convert_num_to_letters(7 + 9 * j)}{3 + diff * i}"} for j in range(5)]
                    ]
                )
            for i in range(3):
                min_size = len(categories) if i != 2 else len(categories_online)
                merges.extend(
                    [
                        {"range": f"A{5 + diff * i}:A{5 + min_size + diff * i}"},
                        {"range": f"A{5 + min_size + 2 + diff * i}:A{5 + min_size * 3 + 3 + diff * i}"},
                        {"range": f"A{5 + min_size * 3 + 5 + diff * i}:A{5 + min_size * 5 + 6 + diff * i}"},
                        {"range": f"A{5 + min_size * 5 + 8 + diff * i}:A{5 + min_size * 8 + 10 + diff * i}"}
                    ]
                )

            ws.batch_merge(
                merges
            )
            # COLOR CELLS to RED
            red_cells = []
            for i in range(3):
                red_cells.append(f'A{1 + diff * i}:B{2 + diff * i}')
                red_cells.append(f'A{3 + diff * i}:AX{4 + diff * i}')
            ws.format(
                red_cells,
                {
                    "backgroundColor": {
                        "red": 0.9569,
                        "green": 0.8,
                        "blue": 0.8
                    },
                }
            )
            # color to blue
            blue_cells = []
            for i in range(3):
                min_size = len(categories) if i != 2 else len(categories_online)
                blue_cells.append(f'A{5 + diff * i}:AX{5 + min_size + diff * i}')
            ws.format(
                blue_cells,
                {
                    "backgroundColor": {
                        "red": 0.7882,
                        "green": 0.8549,
                        "blue": 0.9725
                    },
                }
            )
            # PURPLE
            purple_cells = []
            for i in range(3):
                min_size = len(categories) if i != 2 else len(categories_online)
                purple_cells.append(f'A{5 + min_size + 2 + diff * i}:AX{5 + min_size * 3 + 3 + diff * i}')
            ws.format(
                purple_cells,
                {
                    "backgroundColor": {
                        "red": 0.851,
                        "green": 0.8235,
                        "blue": 0.9137
                    },
                }
            )
            # YELLOW
            yellow_cells = []
            for i in range(3):
                min_size = len(categories) if i != 2 else len(categories_online)
                yellow_cells.append(f'A{5 + min_size * 3 + 5 + diff * i}:AX{5 + min_size * 5 + 6 + diff * i}')
            ws.format(
                yellow_cells,
                {
                    "backgroundColor": {
                        "red": 1.0,
                        "green": 0.9490,
                        "blue": 0.8
                    },
                }
            )
            # GREEN
            green_cells = []
            for i in range(3):
                min_size = len(categories) if i != 2 else len(categories_online)
                green_cells.append(f'A{5 + min_size * 5 + 8 + diff * i}:AX{5 + min_size * 8 + 10 + diff * i}')
            ws.format(
                green_cells,
                {
                    "backgroundColor": {
                        "red": 0.851,
                        "green": 0.9176,
                        "blue": 0.8275
                    },
                }
            )
            # color cells to rgb(102, 102, 102) GREY
            grey_cells = []
            for i in range(3):
                min_size = len(categories) if i != 2 else len(categories_online)
                grey_cells.append(f'A{5 + min_size + 1 + diff * i}:AX{5 + min_size + 1 + diff * i}')
                grey_cells.append(f'A{5 + min_size * 3 + 4 + diff * i}:AX{5 + min_size * 3 + 4 + diff * i}')
                grey_cells.append(f'A{5 + min_size * 5 + 7 + diff * i}:AX{5 + min_size * 5 + 7 + diff * i}')
                grey_cells.append(f'A{5 + min_size * 8 + 11 + diff * i}:AX{5 + min_size * 8 + 11 + diff * i}')
            ws.format(
                grey_cells, 
                {
                    "backgroundColor": {
                            "red": 0.4,
                            "green": 0.4,
                            "blue": 0.4
                    },
                }
            )
            time.sleep(1.2)
            # TEXT EDIT
            header = []
            for i in range(3):
                header.append(f'{3 + diff * i}:{4 + diff * i}')
            ws.format(
                header, 
                {
                'textFormat': {
                    "fontSize": 10,
                    "bold": True
                }
            })
            titles = []
            for i in range(3):
                titles.append(f'A{1 + diff * i}')
            ws.format(
                titles, 
                {
                'textFormat': {
                    "fontSize": 20,
                    "bold": True
                }
            })
            time.sleep(0.5)
            header_row = []
            for i in range(3):
                min_size = len(categories) if i != 2 else len(categories_online)
                header_row.append(f'A{5 + diff * i}:A{5 + min_size * 8 + 11 + diff * i}')
            ws.format(
                header_row, 
                {
                'textFormat': {
                    "fontSize": 15,
                    "bold": True
                }
            })
            ws.format([f'A:B'], {     
                'wrapStrategy': 'WRAP',
                'horizontalAlignment': 'CENTER',
                "verticalAlignment": 'MIDDLE'
            })
            time.sleep(0.8)
            # BORDERS 
            all_borders = []
            for i in range(3):
                min_size = len(categories) if i != 2 else len(categories_online)
                all_borders.append(f'A{3 + diff * i}:AX{5 + min_size * 8 + 11 + diff * i}')
            ws.format( #ALL
                all_borders,
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
            top_borders = []
            for i in range(3):
                top_borders.append(f'A{2 + diff * i}:AX{2 + diff * i}')
            ws.format(  # TOP
                top_borders,
                {  
                    'borders': {
                        "bottom": {
                            'style': 'SOLID_THICK'
                        }
                    }
                }
            )
            time.sleep(0.9)
            bottom_borders = []
            for i in range(3):
                min_size = len(categories) if i != 2 else len(categories_online)
                bottom_borders.append(f'A{5 + min_size * 8 + 12 + diff * i}:AX{5 + min_size * 8 + 12 + diff * i}')
            ws.format(  # BOTTOM
                bottom_borders,
                {  
                    'borders': {
                        "top": {
                            'style': 'SOLID_THICK'
                        }
                    }
                }
            )
            right_borders = []
            for i in range(3):
                min_size = len(categories) if i != 2 else len(categories_online)
                right_borders.append(f'A{6 + diff * i}:B{5 + min_size * 8 + 11 + diff * i}')
                right_borders.append(f'E{6 + diff * i}:E{5 + min_size * 8 + 11 + diff * i}')
                right_borders.append(f'G{6 + diff * i}:G{5 + min_size * 8 + 11 + diff * i}')
                right_borders.append(f'N{6 + diff * i}:N{5 + min_size * 8 + 11 + diff * i}')
                right_borders.append(f'P{6 + diff * i}:P{5 + min_size * 8 + 11 + diff * i}')
                right_borders.append(f'W{6 + diff * i}:W{5 + min_size * 8 + 11 + diff * i}')
                right_borders.append(f'Y{6 + diff * i}:Y{5 + min_size * 8 + 11 + diff * i}')
                right_borders.append(f'AF{6 + diff * i}:AF{5 + min_size * 8 + 11 + diff * i}')
                right_borders.append(f'AH{6 + diff * i}:AH{5 + min_size * 8 + 11 + diff * i}')
                right_borders.append(f'AO{6 + diff * i}:AO{5 + min_size * 8 + 11 + diff * i}')
                right_borders.append(f'AQ{6 + diff * i}:AQ{5 + min_size * 8 + 11 + diff * i}')
                right_borders.append(f'AX{6 + diff * i}:AX{5 + min_size * 8 + 11 + diff * i}')

            ws.format( # RIGHT SIDE
                right_borders,
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
            right_up_angle_borders = []
            for i in range(3):
                right_up_angle_borders.append(f'A{5 + diff * i}')
                right_up_angle_borders.append(f'B{5 + diff * i}')
                right_up_angle_borders.append(f'E{5 + diff * i}')
                right_up_angle_borders.append(f'G{5 + diff * i}')
                right_up_angle_borders.append(f'F{3 + diff * i}:G{3 + diff * i}')
                right_up_angle_borders.append(f'N{3 + diff * i}')
                right_up_angle_borders.append(f'N{5 + diff * i}')
                right_up_angle_borders.append(f'P{5 + diff * i}')
                right_up_angle_borders.append(f'O{3 + diff * i}:P{3 + diff * i}')
                right_up_angle_borders.append(f'W{3 + diff * i}')
                right_up_angle_borders.append(f'W{5 + diff * i}')
                right_up_angle_borders.append(f'Y{5 + diff * i}')
                right_up_angle_borders.append(f'X{3 + diff * i}:Y{3 + diff * i}')
                right_up_angle_borders.append(f'AF{3 + diff * i}')
                right_up_angle_borders.append(f'AF{5 + diff * i}')
                right_up_angle_borders.append(f'AH{5 + diff * i}')
                right_up_angle_borders.append(f'AG{3 + diff * i}:AH{3 + diff * i}')
                right_up_angle_borders.append(f'AO{3 + diff * i}')
                right_up_angle_borders.append(f'AO{5 + diff * i}')
                right_up_angle_borders.append(f'AQ{5 + diff * i}')
                right_up_angle_borders.append(f'AP{3 + diff * i}:AQ{3 + diff * i}')
                right_up_angle_borders.append(f'AX{3 + diff * i}')
                right_up_angle_borders.append(f'AX{5 + diff * i}')
                
            ws.format( # RIGHT UP ANGLE SIDE
                right_up_angle_borders,
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
            right_head_borders = []
            for i in range(3):
                right_head_borders.append(f'B{1 + diff * i}')
                right_head_borders.append(f'B{3 + diff * i}')
                right_head_borders.append(f'E{3 + diff * i}')
            ws.format( # RIGHT HEAD SOLID SIDE
                right_head_borders,
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
            head_bot_side = []
            for i in range(3):
                head_bot_side.append(f'F{4 + diff * i}')
                head_bot_side.append(f'O{4 + diff * i}')
                head_bot_side.append(f'X{4 + diff * i}')
                head_bot_side.append(f'AG{4 + diff * i}')
                head_bot_side.append(f'AP{4 + diff * i}')
                head_bot_side.append(F'H{4 + diff * i}:M{4 + diff * i}')
                head_bot_side.append(F'Q{4 + diff * i}:V{4 + diff * i}')
                head_bot_side.append(F'Z{4 + diff * i}:AE{4 + diff * i}')
                head_bot_side.append(F'AI{4 + diff * i}:AN{4 + diff * i}')
                head_bot_side.append(F'AR{4 + diff * i}:AW{4 + diff * i}')
            ws.format( #  HEAD BOT SIDE
                head_bot_side,
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
            right_head_side = []
            for i in range(3):
                right_head_side.append(f'C{3 + diff * i}')
                right_head_side.append(f'D{3 + diff * i}')
            ws.format( # RIGHT HEAD SOLID SIDE
                right_head_side,
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
            right_bot_head = []
            for i in range(3):
                right_bot_head.append(f'G{4 + diff * i}')
                right_bot_head.append(f'N{4 + diff * i}')
                right_bot_head.append(f'P{4 + diff * i}')
                right_bot_head.append(f'W{4 + diff * i}')
                right_bot_head.append(f'Y{4 + diff * i}')
                right_bot_head.append(f'AF{4 + diff * i}')
                right_bot_head.append(f'AH{4 + diff * i}')
                right_bot_head.append(f'AO{4 + diff * i}')
                right_bot_head.append(f'AQ{4 + diff * i}')
                right_bot_head.append(f'AX{4 + diff * i}')
            ws.format( # RIGHT BOT HEAD SOLID SIDE
                right_bot_head,
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
            percent_rows = []
            for i in range(3):
                curr_cat = categories if i != 2 else categories_online
                min_size = len(curr_cat)
                for j in range(3):
                    for k in range(1, min_size + 1):
                        if j == 2:
                            index_1 = diff * i + 5 + (min_size + 2) + (min_size * 2 + 3) * j  + (3 * k) - 1
                            index_2 = diff * i + 5 + (min_size + 2) + (min_size * 2 + 3) * j  + 3 * k
                            percent_rows.append(
                                f'{index_1}:{index_2}'
                            )
                        else:
                            index = diff * i + 5 + (min_size + 2) + (min_size * 2 + 3) * j  + 2 * k
                            percent_rows.append(
                                f'{index}:{index}'
                            )
                percent_rows.append(f'{5 + min_size * 3 + 3 + diff * i}:{5 + min_size * 3 + 3 + diff * i}')
                percent_rows.append(f'{5 + min_size * 5 + 6 + diff * i}:{5 + min_size * 5 + 6 + diff * i}')
                percent_rows.append(f'{5 + min_size * 8 + 9 + diff * i}:{5 + min_size * 8 + 10 + diff * i}')
            ws.format(
                percent_rows, 
                {
                    'numberFormat': {'type': 'PERCENT'}
                }
            )
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
                    insert_data[year][month]['pipes'][pipe][week_num][weekday - 1] = self._get_insert_col(tmp_data, week_num, pipe, weekday)
                start_day += datetime.timedelta(days=1)
            for year in insert_data:
                for month in insert_data[year]:
                    day = insert_data[year][month]['last_day']
                    ws = self.get_sheet(month, year, day)
                    for pipe in insert_data[year][month]['pipes']:
                        for weeknum in insert_data[year][month]['pipes'][pipe]:
                            week_data = insert_data[year][month]['pipes'][pipe][weeknum]
                            len_cat = len(categories) if pipe != 'online' else len(categories_online)
                            len_data = len_cat * 8 + 11 
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


    def _get_insert_col(self, tmp_data, week_num, pipe_name, weekday):
        rows = []
        pipe_name = self.tg.pipe_names_rus.get(pipe_name, pipe_name)
        if pipe_name != 'Онлайн':
            cats = categories
        else:
            cats = categories_online
        field_num = 0
        for _, group in groups.items():
            for field_name, field_info in group['base'].items():
                if field_name == 'categories':
                    for cat in cats:
                        fields = field_info['fields']
                        for field in fields:
                            if field['type'] == 'ratio':
                                a = self.tg.temp[pipe_name][field['a']][cat]
                                b = self.tg.temp[pipe_name][field['b']][cat]
                                rows.append(self.tg.get_div_col_formula(week_num, weekday, a, b))
                            else:
                                rows.append(tmp_data[field_num])
                                field_num += 1
                else:
                    if field_info['type'] == 'ratio':
                        a = self.tg.temp[pipe_name][field_info['a']][cat]
                        b = self.tg.temp[pipe_name][field_info['b']][cat]
                        rows.append(self.tg.get_div_col_formula(week_num, weekday, a, b))
                    else:
                        rows.append(tmp_data[field_num])
                        field_num += 1
            rows.append('')
        return rows
        
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
                            insert_data[pipe][year][month]['data'].append(
                                [
                                f'{year}.{month}.{day}',
                                *self._get_insert_vertical_rows(tmp_data, day, pipe)
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
                            f'A{2 + min_day}:DU{33}',
                            value_input_option="USER_ENTERED"
                        )
                        time.sleep(0.3)
                        logger.info(f'Добавлена статистика за месяц: {self.tg.MONTH[month]} {year} в воронке {pipe}.')
        except Exception as ex:
            import traceback
            print(traceback.print_exc())
            logger.error(f'Ошибка записи: {ex}')

    def _get_insert_vertical_rows(self, tmp_data, day, pipe_name):
        rows = []
        diff = 2
        ubrat = 5 + self.tg.PIPELINES_DIFF[pipe_name]
        pipe_name = self.tg.pipe_names_rus.get(pipe_name, pipe_name)
        if pipe_name != 'Онлайн':
            cats = categories
        else:
            cats = categories_online
        field_num = 0
        for _, group in groups.items():
            for field_name, field_info in group['base'].items():
                if field_name == 'categories':
                    for cat in cats:
                        fields = field_info['fields']
                        for field in fields:
                            if field['type'] == 'ratio':
                                a = self.tg.temp[pipe_name][field['a']][cat] - ubrat + diff
                                b = self.tg.temp[pipe_name][field['b']][cat] - ubrat + diff
                                rows.append(self.tg.get_vertical_div_col_formula(day, a, b))
                            else:
                                rows.append(tmp_data[field_num])
                                field_num += 1
                else:
                    if field_info['type'] == 'ratio':
                        a = self.tg.temp[pipe_name][field_info['a']][cat] - ubrat + diff
                        b = self.tg.temp[pipe_name][field_info['b']][cat] - ubrat + diff
                        rows.append(self.tg.get_vertical_div_col_formula(day, a, b))
                    else:
                        rows.append(tmp_data[field_num])
                        field_num += 1
            rows.append('')
        return rows

    
if __name__ == '__main__':
    gs = GoogleSheets()
    # ws = gs.get_sheet(9999999999)
    td = datetime.date.today()
    # ws = gs.create_worksheet(td)
    ws = gs.create_vertical_worksheet('тест онлайн', 'online')
    ws = gs.create_vertical_worksheet('тест астана', 'astana')
    # print(Constructor.get_date_row())
