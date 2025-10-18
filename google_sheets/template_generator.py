import calendar
import datetime
from google_sheets.shablon import groups, categories, categories_online

class TemplateGenerator:

    pipe_names_rus = {
        'astana': 'Астана',
        'almaty': 'Алмата',
        'online': 'Онлайн'
    }
    
    WEEK_NAMES = {
        1: 'Понедельник',
        2: 'Вторник',
        3: 'Среда',
        4: 'Четверг',
        5: 'Пятница',
        6: 'Суббота',
        7: 'Воскресенье'
    }

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
        12: 'Декабрь'
    }

    PIPELINES_DIFF = {
        'Алмата': 0,
        'Астана': 120,
        'Онлайн': 240,
        'almaty': 0,
        'astana': 120,
        'online': 240
    }
    pipes_diff = {
        'Алмата': 0,
        'Астана': 120,
        'Онлайн': 240,
    }

    def __init__(self):
        self.temp = self._get_temp()


    def get_week_month(self, today: datetime.datetime):
        weekday = today.isoweekday()
        close_monday = today - datetime.timedelta(days=weekday-1)
        return close_monday.month

    def generate_month_weeks(self, today: datetime.datetime):
        month = self.get_week_month(today)
        weeks = calendar.monthcalendar(year=today.year, month=month)
        if weeks[0][0] == 0:
            del weeks[0]
        return weeks, month
    
    def get_year(self, month, today):
        if month == 12 and today.month == 1:
            return today.year - 1
        return today.year
    
    def get_weeknum(self, today: datetime.datetime):
        weeks, month = self.generate_month_weeks(today)
        if month < today.month:
            weeknum = len(weeks)
        else:
            weekday = today.isoweekday()
            day_num = today.day - weekday + 7
            weeknum = day_num // 7
        year = self.get_year(month, today)
        return weeknum, month, year

    def get_formula_row(self, weeks_ids: list, row_num: int, is_avg: bool = False):
        symbs = []
        for week_id in weeks_ids:
            symbs.append(f'{self.convert_num_to_letters(week_id)}{row_num}')
        sum_vars = '+'.join(symbs)
        if is_avg:
            return f'=({sum_vars})/{len(weeks_ids)}'
        return f'={sum_vars}'


    def get_week_range(self, pipe: str, week_num: int, len_data: int):
        letter = self.convert_num_to_letters(8 + week_num * 9)
        letter_end = self.convert_num_to_letters(14 + week_num * 9)
        return f'{letter}{5 + self.PIPELINES_DIFF.get(pipe, 0)}:{letter_end}{len_data + 5 + self.PIPELINES_DIFF.get(pipe, 0)}'


    def create_shablon(self, today: datetime.datetime):
        weeks, month = self.generate_month_weeks(today)
        weeks_num = len(weeks)
        weeks_ids = [7 + 9 * week_num for week_num in range(weeks_num)]
        col_count = 5 + 9 * weeks_num
        cols = [[] for _ in range(col_count)]
        
        for pipe, diff in self.pipes_diff.items(): 
            cols[0].extend(
                [
                    f'{pipe} {self.MONTH[month]}',
                    '', 
                    'Метрики', 
                    '', 
                    'Общая статистика',
                    *['' for _ in range(len(categories) + 1 if pipe != 'Онлайн' else len(categories_online) + 1)],
                    'Обработанные лиды',
                    *['' for _ in range(len(categories) * 2 + 2 if pipe != 'Онлайн' else len(categories_online) * 2 + 2)],
                    'Квал лиды',
                    *['' for _ in range(len(categories) * 2 + 2 if pipe != 'Онлайн' else len(categories_online) * 2 + 2)],
                    'Успешные лиды',
                    *['' for _ in range(len(categories) * 3 + 3 if pipe != 'Онлайн' else len(categories_online) * 3 + 3)]
                ]
            )
            cols[1].extend(
                [
                    *['' for _ in range(4)],
                    *self._get_title_row(pipe)
                ]
            )
            cols[2].extend(
                [
                    '', 
                    '', 
                    'План', 
                    '',
                    *['' for _ in range(len(categories) * 8 + 12 if pipe != 'Онлайн' else len(categories_online) * 8 + 12)]
                ]
            )
            cols[3].extend(
                [
                    '', 
                    '', 
                    'Факт',
                    '', 
                    *self._get_formulas_rows(pipe, diff, weeks_ids)
                ]
            )
            cols[4].extend(
                [
                    '', 
                    '', 
                    'Проекция', 
                    '',
                    *['' for _ in range(len(categories) * 8 + 12 if pipe != 'Онлайн' else len(categories_online) * 8 + 12)]
                ]
            )
            for i, week in enumerate(weeks):
                cols[5 + 9 * i].extend(
                    [
                        '',
                        '', 
                        f'Неделя - {i + 1}', 
                        'План',
                        *['' for _ in range(len(categories) * 8 + 12 if pipe != 'Онлайн' else len(categories_online) * 8 + 12)]
                    ]
                )
                week_id = weeks_ids[i]
                cols[6 + 9 * i].extend(
                    [
                        '', 
                        '', 
                        '', 
                        'Факт',
                        *self._get_week_formulas_rows(pipe, diff, week_id)
                    ]
                )
                counter = 1
                for j, day in enumerate(week):
                    _diff = 0
                    if day == 0:
                        day = counter
                        counter += 1
                        _diff = 1
                    cols[7 + j + 9 * i].extend(
                        [
                            '',
                            '',
                            f'{day}.{month + _diff}',
                            f'{self.WEEK_NAMES[j + 1]}',
                            *['' for _ in range(len(categories) * 8 + 12 if pipe != 'Онлайн' else len(categories_online) * 8 + 12)]
                        ]
                    )
        return cols, month
    
    def _get_temp(self):
        temp = {}
        for pipe_name, pipe_diff in self.pipes_diff.items():
            pipe_index = 5 + pipe_diff
            if pipe_name not in temp:
                temp[pipe_name] = {}
            if pipe_name != 'Онлайн':
                cats = categories
            else:
                cats = categories_online
            field_num = 0
            for group_name, group in groups.items():
                if group_name not in temp[pipe_name]:
                    temp[pipe_name][group_name] = {}
                for field_name, field_info in group['base'].items():
                    if field_name == 'categories':
                        for cat in cats:
                            fields = field_info['fields']
                            for field in fields:
                                if group_name not in temp[pipe_name][group_name] and field['type'] == 'count':
                                    temp[pipe_name][group_name][cat] = field_num + pipe_index
                                field_num += 1
                    else:
                        if group_name not in temp[pipe_name][group_name] and field_info['type'] == 'count':
                            temp[pipe_name][group_name][field_name] = field_num + pipe_index
                        field_num += 1
                field_num += 1
        return temp
    
    def _get_title_row(self, pipe_name):
        rows = []
        pipe_name = self.pipe_names_rus.get(pipe_name, pipe_name)
        if pipe_name != 'Онлайн':
            cats = categories
        else:
            cats = categories_online
        for _, group in groups.items():
            for field_name, field_info in group['base'].items():
                if field_name == 'categories':
                    for cat in cats:
                        fields = field_info['fields']
                        for field in fields:
                            rows.append(' '.join([field.get('prefix', ''), cat, field.get('postfix', '')]))
                else:
                    rows.append(field_info['title'])
            rows.append('')
        return rows
    
    def _get_formulas_rows(self, pipe_name, pipe_diff, week_ids):
        rows = []
        pipe_index = 5 + pipe_diff
        pipe_name = self.pipe_names_rus.get(pipe_name, pipe_name)
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
                                a = self.temp[pipe_name][field['a']][cat]
                                b = self.temp[pipe_name][field['b']][cat]
                                rows.append(f"=D{a}/D{b}")
                            else:
                                rows.append(self.get_formula_row(week_ids, pipe_index + field_num))
                            field_num += 1
                else:
                    if field_info['type'] == 'ratio':
                        a = self.temp[pipe_name][field_info['a']][cat]
                        b = self.temp[pipe_name][field_info['b']][cat]
                        rows.append(f"=D{a}/D{b}")
                    else:
                        rows.append(self.get_formula_row(week_ids, pipe_index + field_num))
                    field_num += 1
            rows.append('')
            field_num += 1
        return rows
    
    def _get_week_formulas_rows(self, pipe_name, pipe_diff, week_id):
        rows = []
        pipe_index = 5 + pipe_diff
        pipe_name = self.pipe_names_rus.get(pipe_name, pipe_name)
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
                                a = self.temp[pipe_name][field['a']][cat]
                                b = self.temp[pipe_name][field['b']][cat]
                                rows.append(self.get_div_formula_for_week(week_id, a, b))
                            else:
                                rows.append(self.get_formula_for_week(week_id, pipe_index + field_num))
                            field_num += 1
                else:
                    if field_info['type'] == 'ratio':
                        a = self.temp[pipe_name][field_info['a']][cat]
                        b = self.temp[pipe_name][field_info['b']][cat]
                        rows.append(self.get_div_formula_for_week(week_id, a, b))
                    else:
                        rows.append(self.get_formula_for_week(week_id, pipe_index + field_num))
                    field_num += 1
            rows.append('')
            field_num += 1
        return rows
    
    
    def create_vertical_shablon(self, pipe):
        rows = []
        rows.append([self.pipe_names_rus[pipe], *self._get_vertical_formulas(pipe)])
        row_titles = ['День']
        row_titles.extend(self._get_title_row(pipe))
        rows.append(row_titles)
        return rows
    
    def _get_vertical_formulas(self, pipe_name):
        rows = []
        diff = 2
        pipe_name = self.pipe_names_rus.get(pipe_name, pipe_name)
        ubrat = 5 + self.PIPELINES_DIFF[pipe_name]
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
                                a = self.temp[pipe_name][field['a']][cat] - ubrat + diff
                                b = self.temp[pipe_name][field['b']][cat] - ubrat + diff
                                rows.append(self.get_vertical_ratio(a, b))
                            else:
                                rows.append(self.get_vertical_sum(diff + field_num))
                            field_num += 1
                else:
                    if field_info['type'] == 'ratio':
                        a = self.temp[pipe_name][field_info['a']][cat] - ubrat + diff
                        b = self.temp[pipe_name][field_info['b']][cat] - ubrat + diff
                        rows.append(self.get_vertical_ratio(a, b))
                    else:
                        rows.append(self.get_vertical_sum(diff + field_num))
                    field_num += 1
            rows.append('')
            field_num += 1
        return rows

    def get_vertical_sum(self, col):
        return f'=СУММ({self.convert_num_to_letters(col)}3:{self.convert_num_to_letters(col)}40)'
    
    def get_vertical_ratio(self, a, b):
        return f'={self.convert_num_to_letters(a)}1/{self.convert_num_to_letters(b)}1'
    
    def get_formula_for_week(self, week_id, row_num):
        return f'=СУММ({self.convert_num_to_letters(week_id + 1)}{row_num}:{self.convert_num_to_letters(week_id + 7)}{row_num})'

    def get_div_formula_for_week(self, week_id, dividend, divisor):
        return f'={self.convert_num_to_letters(week_id)}{dividend}/{self.convert_num_to_letters(week_id)}{divisor}'

    def get_div_col_formula(self, week_num, day, dividend, divisor):
        week_id = 7 + 9 * week_num
        return f'={self.convert_num_to_letters(week_id + day)}{dividend}/{self.convert_num_to_letters(week_id + day)}{divisor}'
    
    def get_vertical_div_col_formula(self, day, dividend, divisor):
        return f'={self.convert_num_to_letters(dividend)}{2 + day}/{self.convert_num_to_letters(divisor)}{2 + day}'

    def convert_num_to_letters(self, last_index):
        if last_index:
            index = last_index % 26
            count = (last_index // 26)
            if not index:
                index = 26
            if last_index % 26 == 0:
                count -= 1
            if count:
                range_id = f"{self.index_to_range(count)}{self.index_to_range(index)}"
            else:
                range_id = self.index_to_range(index)
            return range_id
        else:
            return 'A'

    def index_to_range(self, index):
        return chr(64 + index)


if __name__ == '__main__':
    import datetime


    t = TemplateGenerator()
    today = datetime.datetime.today()
    print(t.create_shablon(today))