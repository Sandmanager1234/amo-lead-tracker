import calendar
import datetime


class TemplateGenerator:

    __rows_title = [
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
    
    __rows_title_online = [
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
    
    pipe_names_rus = {
        'astana': 'Астана',
        'almaty': 'Алматы',
        'online': 'Онлайн'
    }
    
    pipes_titles = {
        'Алмата': __rows_title,
        'Астана': __rows_title,
        'Онлайн': __rows_title_online
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
        'Астана': 40,
        'Онлайн': 80,
        'almaty': 0,
        'astana': 40,
        'online': 80
    }


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
        
        for pipe in self.pipes_titles: 
            cols[0].extend(
                [
                    f'{pipe} {self.MONTH[month]}',
                    '', 
                    'Метрики', 
                    '', 
                    'Общая статистика',
                    *['' for _ in range(4 if pipe != 'Онлайн' else 5)],
                    'Обработанные лиды',
                    *['' for _ in range(8 if pipe != 'Онлайн' else 10)],
                    'Квал лиды',
                    *['' for _ in range(8 if pipe != 'Онлайн' else 10)],
                    'Успешные лиды',
                    *['' for _ in range(12 if pipe != 'Онлайн' else 14)]
                ]
            )
            cols[1].extend(
                [
                    *['' for _ in range(4)],
                    *self.pipes_titles.get(pipe, [])
                ]
            )
            cols[2].extend(
                [
                    '', 
                    '', 
                    'План', 
                    *['' for _ in range(37)]
                ]
            )
            if pipe != 'Онлайн':
                cols[3].extend(
                    [
                        '', 
                        '', 
                        'Факт',
                        '',
                        self.get_formula_row(weeks_ids, 5 + self.PIPELINES_DIFF.get(pipe, 0)),
                        self.get_formula_row(weeks_ids, 6 + self.PIPELINES_DIFF.get(pipe, 0)),
                        self.get_formula_row(weeks_ids, 7 + self.PIPELINES_DIFF.get(pipe, 0)),
                        self.get_formula_row(weeks_ids, 8 + self.PIPELINES_DIFF.get(pipe, 0)),
                        '',
                        self.get_formula_row(weeks_ids, 10 + self.PIPELINES_DIFF.get(pipe, 0)),
                        self.get_formula_row(weeks_ids, 11 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{11 + self.PIPELINES_DIFF.get(pipe, 0)}/D{6 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        self.get_formula_row(weeks_ids, 13 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{13 + self.PIPELINES_DIFF.get(pipe, 0)}/D{7 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        self.get_formula_row(weeks_ids, 15 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{15 + self.PIPELINES_DIFF.get(pipe, 0)}/D{8 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        f'=D{10 + self.PIPELINES_DIFF.get(pipe, 0)}/D{5 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        '',
                        self.get_formula_row(weeks_ids, 19 + self.PIPELINES_DIFF.get(pipe, 0)),
                        self.get_formula_row(weeks_ids, 20 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{20 + self.PIPELINES_DIFF.get(pipe, 0)}/D{11 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        self.get_formula_row(weeks_ids, 22 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{22 + self.PIPELINES_DIFF.get(pipe, 0)}/D{13 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        self.get_formula_row(weeks_ids, 24 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{24 + self.PIPELINES_DIFF.get(pipe, 0)}/D{15 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        f'=D{19 + self.PIPELINES_DIFF.get(pipe, 0)}/D{10 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        '',
                        self.get_formula_row(weeks_ids, 28 + self.PIPELINES_DIFF.get(pipe, 0)),
                        self.get_formula_row(weeks_ids, 29 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{29 + self.PIPELINES_DIFF.get(pipe, 0)}/D{11 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        f'=D{29 + self.PIPELINES_DIFF.get(pipe, 0)}/D{20 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        self.get_formula_row(weeks_ids, 32 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{32 + self.PIPELINES_DIFF.get(pipe, 0)}/D{13 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        f'=D{32 + self.PIPELINES_DIFF.get(pipe, 0)}/D{22 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        self.get_formula_row(weeks_ids, 35 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{35 + self.PIPELINES_DIFF.get(pipe, 0)}/D{15 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        f'=D{35 + self.PIPELINES_DIFF.get(pipe, 0)}/D{24 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        f'=D{28 + self.PIPELINES_DIFF.get(pipe, 0)}/D{10 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        f'=D{28 + self.PIPELINES_DIFF.get(pipe, 0)}/D{19 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        ''
                    ]
                )
            else:
                cols[3].extend(
                    [
                        '', 
                        '', 
                        'Факт',
                        '',
                        self.get_formula_row(weeks_ids, 5 + self.PIPELINES_DIFF.get(pipe, 0)),
                        self.get_formula_row(weeks_ids, 6 + self.PIPELINES_DIFF.get(pipe, 0)),
                        self.get_formula_row(weeks_ids, 7 + self.PIPELINES_DIFF.get(pipe, 0)),
                        self.get_formula_row(weeks_ids, 8 + self.PIPELINES_DIFF.get(pipe, 0)),
                        self.get_formula_row(weeks_ids, 9 + self.PIPELINES_DIFF.get(pipe, 0)),
                        '',
                        self.get_formula_row(weeks_ids, 11 + self.PIPELINES_DIFF.get(pipe, 0)),
                        self.get_formula_row(weeks_ids, 12 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{12 + self.PIPELINES_DIFF.get(pipe, 0)}/D{6 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        self.get_formula_row(weeks_ids, 14 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{14 + self.PIPELINES_DIFF.get(pipe, 0)}/D{7 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        self.get_formula_row(weeks_ids, 16 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{16 + self.PIPELINES_DIFF.get(pipe, 0)}/D{8 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        self.get_formula_row(weeks_ids, 18 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{18 + self.PIPELINES_DIFF.get(pipe, 0)}/D{9 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        f'=D{11 + self.PIPELINES_DIFF.get(pipe, 0)}/D{5 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        '',
                        self.get_formula_row(weeks_ids, 22 + self.PIPELINES_DIFF.get(pipe, 0)),
                        self.get_formula_row(weeks_ids, 23 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{23 + self.PIPELINES_DIFF.get(pipe, 0)}/D{12 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        self.get_formula_row(weeks_ids, 25 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{25 + self.PIPELINES_DIFF.get(pipe, 0)}/D{14 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        self.get_formula_row(weeks_ids, 27 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{27 + self.PIPELINES_DIFF.get(pipe, 0)}/D{16 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        self.get_formula_row(weeks_ids, 29 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{29 + self.PIPELINES_DIFF.get(pipe, 0)}/D{18 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        f'=D{22 + self.PIPELINES_DIFF.get(pipe, 0)}/D{11 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        '',
                        self.get_formula_row(weeks_ids, 33 + self.PIPELINES_DIFF.get(pipe, 0)),
                        self.get_formula_row(weeks_ids, 34 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{34 + self.PIPELINES_DIFF.get(pipe, 0)}/D{12 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        f'=D{34 + self.PIPELINES_DIFF.get(pipe, 0)}/D{23 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        self.get_formula_row(weeks_ids, 37 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{37 + self.PIPELINES_DIFF.get(pipe, 0)}/D{14 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        f'=D{37 + self.PIPELINES_DIFF.get(pipe, 0)}/D{25 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        self.get_formula_row(weeks_ids, 40 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{40 + self.PIPELINES_DIFF.get(pipe, 0)}/D{16 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        f'=D{40 + self.PIPELINES_DIFF.get(pipe, 0)}/D{27 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        self.get_formula_row(weeks_ids, 43 + self.PIPELINES_DIFF.get(pipe, 0)),
                        f'=D{43 + self.PIPELINES_DIFF.get(pipe, 0)}/D{18 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        f'=D{43 + self.PIPELINES_DIFF.get(pipe, 0)}/D{29 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        f'=D{33 + self.PIPELINES_DIFF.get(pipe, 0)}/D{11 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        f'=D{33 + self.PIPELINES_DIFF.get(pipe, 0)}/D{22 + self.PIPELINES_DIFF.get(pipe, 0)}',
                        ''
                    ]
                )
            cols[4].extend(
                [
                    '', 
                    '', 
                    'Проекция', 
                    *['' for _ in range(37)]
                ]
            )
            for i, week in enumerate(weeks):
                cols[5 + 9 * i].extend(
                    [
                        '',
                        '', 
                        f'Неделя - {i + 1}', 
                        'План',
                        *['' for _ in range(36)]
                    ]
                )
                week_id = weeks_ids[i]
                if pipe == 'Онлайн':
                    cols[6 + 9 * i].extend(
                        [
                            '', 
                            '', 
                            '', 
                            'Факт',
                            self.get_formula_for_week(week_id, pipe, 5),
                            self.get_formula_for_week(week_id, pipe, 6),
                            self.get_formula_for_week(week_id, pipe, 7),
                            self.get_formula_for_week(week_id, pipe, 8),
                            self.get_formula_for_week(week_id, pipe, 9),
                            '',
                            self.get_formula_for_week(week_id, pipe, 11),
                            self.get_formula_for_week(week_id, pipe, 12),
                            self.get_div_formula_for_week(week_id, pipe, 12, 6),
                            self.get_formula_for_week(week_id, pipe, 14),
                            self.get_div_formula_for_week(week_id, pipe, 14, 7),
                            self.get_formula_for_week(week_id, pipe, 16),
                            self.get_div_formula_for_week(week_id, pipe, 16, 8),
                            self.get_formula_for_week(week_id, pipe, 18),
                            self.get_div_formula_for_week(week_id, pipe, 18, 9),
                            self.get_div_formula_for_week(week_id, pipe, 11, 5),
                            '',
                            self.get_formula_for_week(week_id, pipe, 22),
                            self.get_formula_for_week(week_id, pipe, 23),
                            self.get_div_formula_for_week(week_id, pipe, 23, 12),
                            self.get_formula_for_week(week_id, pipe, 25),
                            self.get_div_formula_for_week(week_id, pipe, 25, 14),
                            self.get_formula_for_week(week_id, pipe, 27),
                            self.get_div_formula_for_week(week_id, pipe, 27, 16),
                            self.get_formula_for_week(week_id, pipe, 29),
                            self.get_div_formula_for_week(week_id, pipe, 29, 18),
                            self.get_div_formula_for_week(week_id, pipe, 22, 11),
                            '',
                            self.get_formula_for_week(week_id, pipe, 33),
                            self.get_formula_for_week(week_id, pipe, 34),
                            self.get_div_formula_for_week(week_id, pipe, 34, 12),
                            self.get_div_formula_for_week(week_id, pipe, 34, 23),
                            self.get_formula_for_week(week_id, pipe, 37),
                            self.get_div_formula_for_week(week_id, pipe, 37, 14),
                            self.get_div_formula_for_week(week_id, pipe, 37, 25),
                            self.get_formula_for_week(week_id, pipe, 40),
                            self.get_div_formula_for_week(week_id, pipe, 40, 27),
                            self.get_formula_for_week(week_id, pipe, 43),
                            self.get_div_formula_for_week(week_id, pipe, 43, 18),
                            self.get_div_formula_for_week(week_id, pipe, 43, 29),
                            self.get_div_formula_for_week(week_id, pipe, 33, 11),
                            self.get_div_formula_for_week(week_id, pipe, 33, 22),
                            ''
                        ]
                    )
                else:
                    cols[6 + 9 * i].extend(
                        [
                            '', 
                            '', 
                            '', 
                            'Факт',
                            self.get_formula_for_week(week_id, pipe, 5),
                            self.get_formula_for_week(week_id, pipe, 6),
                            self.get_formula_for_week(week_id, pipe, 7),
                            self.get_formula_for_week(week_id, pipe, 8),
                            '',
                            self.get_formula_for_week(week_id, pipe, 10),
                            self.get_formula_for_week(week_id, pipe, 11),
                            self.get_div_formula_for_week(week_id, pipe, 11, 6),
                            self.get_formula_for_week(week_id, pipe, 13),
                            self.get_div_formula_for_week(week_id, pipe, 13, 7),
                            self.get_formula_for_week(week_id, pipe, 15),
                            self.get_div_formula_for_week(week_id, pipe, 15, 8),
                            self.get_div_formula_for_week(week_id, pipe, 10, 5),
                            '',
                            self.get_formula_for_week(week_id, pipe, 19),
                            self.get_formula_for_week(week_id, pipe, 20),
                            self.get_div_formula_for_week(week_id, pipe, 20, 11),
                            self.get_formula_for_week(week_id, pipe, 22),
                            self.get_div_formula_for_week(week_id, pipe, 22, 13),
                            self.get_formula_for_week(week_id, pipe, 24),
                            self.get_div_formula_for_week(week_id, pipe, 24, 15),
                            self.get_div_formula_for_week(week_id, pipe, 19, 10),
                            '',
                            self.get_formula_for_week(week_id, pipe, 28),
                            self.get_formula_for_week(week_id, pipe, 29),
                            self.get_div_formula_for_week(week_id, pipe, 29, 11),
                            self.get_div_formula_for_week(week_id, pipe, 29, 20),
                            self.get_formula_for_week(week_id, pipe, 32),
                            self.get_div_formula_for_week(week_id, pipe, 32, 13),
                            self.get_div_formula_for_week(week_id, pipe, 32, 22),
                            self.get_formula_for_week(week_id, pipe, 35),
                            self.get_div_formula_for_week(week_id, pipe, 35, 15),
                            self.get_div_formula_for_week(week_id, pipe, 35, 24),
                            self.get_div_formula_for_week(week_id, pipe, 28, 10),
                            self.get_div_formula_for_week(week_id, pipe, 28, 19),
                            ''
                        ]
                    )
                counter = 1
                for j, day in enumerate(week):
                    diff = 0
                    if day == 0:
                        day = counter
                        counter += 1
                        diff = 1
                    cols[7 + j + 9 * i].extend(
                        [
                            '',
                            '',
                            f'{day}.{month + diff}',
                            f'{self.WEEK_NAMES[j + 1]}',
                            *['' for _ in range(36)]
                        ]
                    )
        return cols, month
    
    def create_vertical_shablon(self, pipe):
        rows = []
        if pipe == 'online':
            rows.append(
                [
                    pipe,
                    self.get_vertical_sum(2),
                    self.get_vertical_sum(3),
                    self.get_vertical_sum(4),
                    self.get_vertical_sum(5),
                    self.get_vertical_sum(6),
                    '',
                    self.get_vertical_sum(8),
                    self.get_vertical_sum(9),
                    '=I1/C1',
                    self.get_vertical_sum(11),
                    '=K1/D1',
                    self.get_vertical_sum(13),
                    '=M1/E1',
                    self.get_vertical_sum(15),
                    '=O1/F1',
                    '=H1/B1',
                    '',
                    self.get_vertical_sum(19),
                    self.get_vertical_sum(20),
                    '=T1/I1',
                    self.get_vertical_sum(22),
                    '=V1/K1',
                    self.get_vertical_sum(24),
                    '=X1/M1',
                    self.get_vertical_sum(26),
                    '=Z1/O1',
                    '=S1/H1',
                    '',
                    self.get_vertical_sum(30),
                    self.get_vertical_sum(31),
                    '=AE1/I1',
                    '=AE1/T1',
                    self.get_vertical_sum(34),
                    '=AH1/K1',
                    '=AH1/V1',
                    self.get_vertical_sum(37),
                    '=AK1/M1',
                    '=AK1/X1',
                    self.get_vertical_sum(40),
                    '=AN1/O1',
                    '=AN1/Z1',
                    '=AD1/H1',
                    '=AD1/S1',
                    ''
                ]
            )
        else:
            rows.append(
                [
                    self.pipe_names_rus[pipe],
                    self.get_vertical_sum(2),
                    self.get_vertical_sum(3),
                    self.get_vertical_sum(4),
                    self.get_vertical_sum(5),
                    '',
                    self.get_vertical_sum(7),
                    self.get_vertical_sum(8),
                    '=H1/C1',
                    self.get_vertical_sum(10),
                    '=J1/D1',
                    self.get_vertical_sum(12),
                    '=L1/E1',
                    '=G1/B1',
                    '',
                    self.get_vertical_sum(16),
                    self.get_vertical_sum(17),
                    '=Q1/H1',
                    self.get_vertical_sum(19),
                    '=S1/J1',
                    self.get_vertical_sum(21),
                    '=U1/M1',
                    '=P1/G1',
                    '',
                    self.get_vertical_sum(25),
                    self.get_vertical_sum(26),
                    '=Z1/H1',
                    '=Z1/Q1',
                    self.get_vertical_sum(29),
                    '=AC1/J1',
                    '=AC1/S1',
                    self.get_vertical_sum(32),
                    '=AF1/L1',
                    '=AF1/U1',
                    '=Y1/G1',
                    '=Y1/P1',
                    ''
                ]
            )
        row_titles = ['День']
        if pipe != 'online':
            row_titles.extend(self.__rows_title)
        else:
            row_titles.extend(self.__rows_title_online)
        rows.append(
            row_titles
        )
        return rows

    def get_vertical_sum(self, col):
        return f'=СУММ({self.convert_num_to_letters(col)}3:{self.convert_num_to_letters(col)}40)'
    
    def get_formula_for_week(self, week_id, pipe, row_num):
        return f'=СУММ({self.convert_num_to_letters(week_id + 1)}{row_num + self.PIPELINES_DIFF.get(pipe, 0)}:{self.convert_num_to_letters(week_id + 7)}{row_num + self.PIPELINES_DIFF.get(pipe, 0)})'

    def get_div_formula_for_week(self, week_id, pipe, dividend, divisor):
        return f'={self.convert_num_to_letters(week_id)}{dividend + self.PIPELINES_DIFF.get(pipe, 0)}/{self.convert_num_to_letters(week_id)}{divisor + self.PIPELINES_DIFF.get(pipe, 0)}'

    def get_div_col_formula(self, week_num, pipe, day, dividend, divisor):
        week_id = 7 + 9 * week_num
        return f'={self.convert_num_to_letters(week_id + day)}{dividend + self.PIPELINES_DIFF.get(pipe, 0)}/{self.convert_num_to_letters(week_id + day)}{divisor + self.PIPELINES_DIFF.get(pipe, 0)}'

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