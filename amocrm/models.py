import re
import os
from datetime import datetime, timedelta
from typing import Generator
from loguru import logger
from dotenv import load_dotenv

from kztime import get_local_datetime, get_today_info

load_dotenv()


class Event:
    def __init__(self, id: str) -> "Event":
        self.id: str = id
        self.entity_id : str = ''
        self.after_value : str = ''
        self.after_status : str = ''
        self.before_status : str = ''
        self.before_value : str = ''
        self.event_type : str = ''
        self.created_at : int = 0

    @classmethod
    def from_json(cls, data: dict):
        try:
            self : Event = cls(data.get('id'))  
            self.entity_id = data.get('entity_id')
            self.event_type = data.get('type')
            self.created_at = data.get('created_at')
            match self.event_type:
                case 'lead_status_changed':
                    self.after_value = data.get(
                        'value_after', [{}]
                    )[0].get(
                        'lead_status'
                    ).get(
                        'pipeline_id'
                    )
                    self.before_value = data.get(
                        'value_before', [{}]
                    )[0].get(
                        'lead_status'
                    ).get(
                        'pipeline_id'
                    )
                    self.after_status = data.get(
                        'value_after', [{}]
                    )[0].get(
                        'lead_status'
                    ).get(
                        'status_id'
                    )
                    self.before_status = data.get(
                        'value_before', [{}]
                    )[0].get(
                        'lead_status'
                    ).get(
                        'status_id'
                    )
                case 'entity_tag_added':
                    self.after_value = data.get(
                        'value_after', [{}]
                    )[0].get(
                        'tag'
                    ).get(
                        'name'
                    )
                case 'lead_added':
                    pass
                case _:
                    raise Exception(f'Неизвестный тип события: {self.event_type}')

            return self
        except Exception as e:
            logger.error(f'Ошибка обработки события: {e}')


class Events: 
    def __init__(self):
        self.events = []

    def __iter__(self) -> Generator[Event, None, None]:
        for event in self.events:
            yield event

    @classmethod
    def from_json(cls, data: dict):
        try:
            self : Events = cls()  
            event_objs = data.get('_embedded', {}).get('events', [])
            for event_obj in event_objs:
                self.add_event(Event.from_json(event_obj))
            return self
        except Exception as e:
            logger.error(f'Ошибка обработки списка событий: {e}')

    def add_event(self, event: Event):
        self.events.append(event)

    def get_timestamp_by_index(self, timestamp: int, id: int = 0):
        if not timestamp:
            timestamp = self.events[id].created_at
        return timestamp


class Tag:
    id : int
    name : str
    target_type : int
    target_tags = [
        'ss',
        'ent', 
        'kids',
        'online', 
        'ast',
        'ala',
        'two', 
        'video',  
        'WA',
        'FB',
        'FBONLINE',
        'WEB', 
        'OLIMP',
        'PROG'
    ]
    zvonobot_tags = [
        'звонобот',
        'zvonobot'
    ]

    def __init__(self, name: str):
        self.name = name
        self.target_type = self.__get_target_type()

    def __get_target_type(self):
        if self.is_other_city:
            return -1
        elif self.is_target:
            return 0
        elif self.is_zvonobot:
            return 1
        else:
            return 2

    def __get_regex(self, tags):
        regex_list = []
        for tag in tags:
            lower_tag = tag.lower()
            regex_letters = []
            for letter in lower_tag:
                regex_letters.append(f'{letter}{letter.upper()}')
            regex_word = f'^[{"][".join(regex_letters)}]'
            regex_list.append(regex_word)
        return '|'.join(regex_list)
    
    def _get_target_regex_tags(self):
        return self.__get_regex(self.target_tags)
    
    def _get_zvonobot_regex_tags(self):
        return self.__get_regex(self.zvonobot_tags)
    
    def is_target(self):
        return bool(re.search(self._get_target_regex_tags(), self.name))
    
    def is_zvonobot(self):
        return bool(re.search(self._get_zvonobot_regex_tags(), self.name))
    
    def is_other_city(self):
        return self.name == 'Другой_город'

    @classmethod
    def from_json(cls, data: dict):
        try:
            self : Tag = cls(data.get('name', ''))
            return self
        except Exception as e:
            logger.error(f"Общая ошибка обработки данных: {e}")
            raise


class Tags: 
    def __init__(self):
        self.tags : list[Tag] = []

    def __iter__(self) -> Generator[Tag, None, None]:
        for tag in self.tags:
            yield tag

    def add_tag(self, tag: Tag):
        self.tags.append(tag)
    
    def get_type(self):
        target_type = 2
        for tag in self.tags:
            target_type = min(target_type, tag.target_type)
        return target_type

    @classmethod
    def from_json(cls, data: dict):
        try:
            self : Tags = cls()  
            tags_json = data.get('_embedded', {}).get('tags', [])
            for tag_json in tags_json:
                self.add_tag(Tag.from_json(tag_json))
            return self
        except Exception as e:
            logger.error(f'Ошибка обработки списка событий: {e}')


class Lead:
    id : int
    pipeline_id : str
    pipe_name : str
    status_id : str
    poll_type : str = 'tags'
    tags_type : int
    reject_reason : str
    is_qual : bool = False
    is_after_processing : bool = False
    is_success : bool = False
    created_at : int
    dt_created_at: datetime 
    updated_at : int

    after_processing_statuses = {
        os.getenv('astana_pipeline'): ['62970105', '62970109', '62970113'],
        os.getenv('almaty_pipeline'): ['63909053', '63909057', '63909061'],
        os.getenv('pipeline_online'): ['68601325', '68601329', '68601333']
    }
    success_pipes = [os.getenv('pipeline_astana_success'), os.getenv('pipeline_almaty_success')]

    pipe_names = {
        os.getenv('astana_pipeline'): 'astana',
        os.getenv('pipeline_astana_success'): 'astana',
        os.getenv('almaty_pipeline'): 'almaty',
        os.getenv('pipeline_almaty_success'): 'almaty',
        os.getenv('pipeline_online'): 'online'
    }

    def __init__(self, id: str):
        self.id = id
        self.poll_type : str = ''
        self.reject_reason : str = 'не заполнено'

    def __get_value_from_json(self, field: dict, _all: bool = False) -> str:
        """Приватный метод для получения значения из JSON"""
        try:
            if not _all:
                value = (
                    field["values"][0]["value"]
                    if field["values"][0]["value"] is not None
                    else ""
                )
            else:
                value = ", ".join(
                    [
                        value["value"]
                        for value in field["values"]
                        if value["value"] is not None
                    ]
                )
            logger.debug(f"{field.get('field_name','Неизвестное')}: {value}")
            return value
        except (KeyError, IndexError, TypeError) as e:
            logger.warning(
                f"Ошибка при обработке поля `{field.get('field_name','Неизвестное')}`: {e}"
            )
            return "не заполнено"
        
    # добавить created_at
    @classmethod
    def from_json(cls, data: dict) -> "Lead":
        """Обрабатывает вкладку `Основное` в сделке."""
        try:
            self: Lead = cls(data.get("id", ""))
            self.pipeline_id = str(data.get('pipeline_id', ''))
            self.pipe_name = self.pipe_names.get(self.pipeline_id, '')
            self.status_id = str(data.get('status_id', ''))
            self.tags_type = Tags.from_json(data).get_type()
            self.created_at = data.get('created_at', 0)
            self.dt_created_at = get_local_datetime(self.created_at)
            self.updated_at = data.get('updated_at', 0)
            
            fields = data.get("custom_fields_values", []) if data.get("custom_fields_values", []) else []
            for field in fields:
                match field.get("field_name", None):
                    case 'ЗНР причина':
                        self.reject_reason = self.__get_value_from_json(field)
                    case _:
                        continue
            if self.pipeline_id in self.success_pipes or self.status_id == os.getenv('status_online_success'):
                self.is_success = True

            if (self.status_id not in self.after_processing_statuses.get(self.pipeline_id, []) or self.is_success):
                self.is_after_processing = True
            
            if (self.status_id not in self.after_processing_statuses.get(self.pipeline_id, [])
                and 
                self.reject_reason not in ['Не прошли квал', 'НД']):
                self.is_qual = True

            return self
        except KeyError as e:
            logger.error(f"Ключ не найден в данных: {e}")
            raise
        except Exception as e:
            logger.error(f"Общая ошибка обработки данных: {e}")
            raise

class Leads:

    offset = {
        'almaty': 0,
        'astana': 40,
        'online': 80
    }

    def __init__(self):
        self.leads : list[Lead] = []
        self.count = len(self.leads)

    def __iter__(self) -> Generator[Lead, None, None]:
        for lead in self.leads:
            yield lead

    def add_leads(self, leads: 'Leads'):
        self.leads.extend(leads.leads)
        self.count += leads.count

    def add_lead(self, lead: Lead):
        self.leads.append(lead)
        self.count += 1

    def get_total_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x: (x.dt_created_at.day == day.day and x.dt_created_at.month == day.month) and x.pipe_name == pipe, 
                    self.leads
                )
            )
        )

    def get_target_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x:
                        x.tags_type == 0 and (
                            x.dt_created_at.day == day.day and x.dt_created_at.month == day.month
                        ) and x.pipe_name == pipe, 
                    self.leads
                )
            )
        )
        
    def get_zvonobot_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x: 
                        x.tags_type == 1 and (
                            x.dt_created_at.day == day.day and x.dt_created_at.month == day.month
                            ) and x.pipe_name == pipe, self.leads
                        )
                    )
                )
    
    def get_other_city_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x: x.tags_type == -1 and (
                        x.dt_created_at.day == day.day and x.dt_created_at.month == day.month
                        ) and x.pipe_name == pipe, self.leads
                    )
                )
            )
    
    def get_other_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x: x.tags_type == 2 and (
                        x.dt_created_at.day == day.day and x.dt_created_at.month == day.month
                        ) and x.pipe_name == pipe, self.leads
                    )
                )
            )
    
    def get_after_processing_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x: x.is_after_processing == True and (
                        x.dt_created_at.day == day.day and x.dt_created_at.month == day.month
                    ) and x.pipe_name == pipe, self.leads
                )
            )
        )
    
    def get_qual_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x: x.is_qual == True and (
                        x.dt_created_at.day == day.day and x.dt_created_at.month == day.month
                    ) and x.pipe_name == pipe, self.leads
                )
            )
        )
    
    def get_success_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x: x.is_success == True and (
                        x.dt_created_at.day == day.day and x.dt_created_at.month == day.month
                    ) and x.pipe_name == pipe, self.leads
                )
            )
        )
    
    def get_qual_target_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x: x.tags_type == 0 and x.is_qual == True and (
                        x.dt_created_at.day == day.day and x.dt_created_at.month == day.month
                    ) and x.pipe_name == pipe, self.leads
                )
            )
        )
    
    def get_qual_zvonobot_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x: x.tags_type == 1 and x.is_qual == True and (
                        x.dt_created_at.day == day.day and x.dt_created_at.month == day.month
                    ) and x.pipe_name == pipe, self.leads
                )
            )
        )
    
    def get_qual_other_city_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x: x.tags_type == -1 and x.is_qual == True and (
                    x.dt_created_at.day == day.day and x.dt_created_at.month == day.month
                ) and x.pipe_name == pipe, self.leads
            )
        )
    )
    
    def get_qual_other_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x: x.tags_type == 2 and x.is_qual == True and (
                        x.dt_created_at.day == day.day and x.dt_created_at.month == day.month
                    ) and x.pipe_name == pipe, self.leads
                )
            )
        )
    
    def get_processing_target_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x: x.tags_type == 0 and x.is_after_processing == True and (
                        x.dt_created_at.day == day.day and x.dt_created_at.month == day.month
                    ) and x.pipe_name == pipe, self.leads
                )
            )
        )
    
    def get_processing_zvonobot_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x: x.tags_type == 1 and x.is_after_processing == True and (
                        x.dt_created_at.day == day.day and x.dt_created_at.month == day.month
                    ) and x.pipe_name == pipe, self.leads
                )
            )
        )
    
    def get_processing_other_city_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x: x.tags_type == -1 and x.is_after_processing == True and (
                        x.dt_created_at.day == day.day and x.dt_created_at.month == day.month
                    ) and x.pipe_name == pipe, self.leads
                )
            )
        )
    
    def get_processing_other_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x: x.tags_type == 2 and x.is_after_processing == True and (
                        x.dt_created_at.day == day.day and x.dt_created_at.month == day.month
                    ) and x.pipe_name == pipe, self.leads
                )
            )
        )
    
    def get_success_target_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x: x.tags_type == 0 and x.is_success == True and (
                        x.dt_created_at.day == day.day and x.dt_created_at.month == day.month
                    ) and x.pipe_name == pipe, self.leads
                )
            )
        )
    
    def get_success_zvonobot_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x: x.tags_type == 1 and x.is_success == True and (
                        x.dt_created_at.day == day.day and x.dt_created_at.month == day.month
                    ) and x.pipe_name == pipe, self.leads
                )
            )
        )
    
    def get_success_other_city_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x: 
                        x.tags_type == -1 and x.is_success == True and (
                            x.dt_created_at.day == day.day and x.dt_created_at.month == day.month
                        ) and x.pipe_name == pipe, 
                    self.leads
                )
            )
        )
    
    def get_success_other_count(self, pipe: str, day: datetime):
        return len(
            list(
                filter(
                    lambda x: x.tags_type == 2 and x.is_success == True and (
                        x.dt_created_at.day == day.day and x.dt_created_at.month == day.month
                    ) and x.pipe_name == pipe, self.leads
                )
            )
        )
    
    def get_day_data(self, pipe, dt):
        if pipe == 'online':
            return [
                self.get_total_count(pipe, dt),
                self.get_target_count(pipe, dt),
                self.get_zvonobot_count(pipe, dt),
                self.get_other_city_count(pipe, dt),
                self.get_other_count(pipe, dt),
                self.get_after_processing_count(pipe, dt),
                self.get_processing_target_count(pipe, dt),
                self.get_processing_zvonobot_count(pipe, dt),
                self.get_processing_other_city_count(pipe, dt),
                self.get_processing_other_count(pipe, dt),
                self.get_qual_count(pipe, dt),
                self.get_qual_target_count(pipe, dt),
                self.get_qual_zvonobot_count(pipe, dt),
                self.get_qual_other_city_count(pipe, dt),
                self.get_qual_other_count(pipe, dt),
                self.get_success_count(pipe, dt),
                self.get_success_target_count(pipe, dt),
                self.get_success_zvonobot_count(pipe, dt),
                self.get_success_other_city_count(pipe, dt),
                self.get_success_other_count(pipe, dt)
            ]
        else:
            return [
                self.get_total_count(pipe, dt),
                self.get_target_count(pipe, dt),
                self.get_zvonobot_count(pipe, dt),
                self.get_other_count(pipe, dt),
                self.get_after_processing_count(pipe, dt),
                self.get_processing_target_count(pipe, dt),
                self.get_processing_zvonobot_count(pipe, dt),
                self.get_processing_other_count(pipe, dt),
                self.get_qual_count(pipe, dt),
                self.get_qual_target_count(pipe, dt),
                self.get_qual_zvonobot_count(pipe, dt),
                self.get_qual_other_count(pipe, dt),
                self.get_success_count(pipe, dt),
                self.get_success_target_count(pipe, dt),
                self.get_success_zvonobot_count(pipe, dt),
                self.get_success_other_count(pipe, dt)
            ]
        
    def get_column_data(self, pipelines: list):
        last_30_days_data = {}
        for pipe in pipelines:
            last_30_days_data[pipe] = {}
            _, _, now = get_today_info()
            for _ in range(31):
                if now.year not in last_30_days_data[pipe]:
                    last_30_days_data[pipe][now.year] = {}
                if now.month not in last_30_days_data[pipe][now.year]:
                    last_30_days_data[pipe][now.year][now.month] = {}
                if now.day not in last_30_days_data[pipe][now.year][now.month]:
                    last_30_days_data[pipe][now.year][now.month][now.day] = self.get_day_data(pipe, now)
                now -= timedelta(days=1)
        return last_30_days_data

    @classmethod
    def from_json(cls, data: dict):
        try:
            self : Leads = cls()  
            leads_json = data.get('_embedded', {}).get('leads', [])
            for lead_json in leads_json:
                self.add_lead(Lead.from_json(lead_json))
            return self
        except Exception as e:
            logger.error(f'Ошибка обработки списка сделок: {e}')


if __name__ == '__main__':
    l = Leads()

