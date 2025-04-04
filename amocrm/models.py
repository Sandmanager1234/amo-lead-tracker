import re
import os
import datetime
import time
from typing import Generator
from loguru import logger


class Event:
    def __init__(self, id: str) -> "Event":
        self.id: str = id
        self.entity_id : str = ''
        self.after_value : str = ''
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
                case 'entity_tag_added':
                    self.after_value = data.get(
                        'value_after', [{}]
                    )[0].get(
                        'tag'
                    ).get(
                        'name'
                    )
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

    def get_timestamp_by_index(self, id: int = 0):
        try:
            timestamp = self.events[id].created_at
        except:
            timestamp = int(time.time())
        return timestamp


class Tag:
    id : int
    name : str
    target_type : str
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
        if self.is_target():
            return 0
        elif self.is_zvonobot():
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
            regex_word = f'[{"][".join(regex_letters)}]'
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
    status_id : str
    poll_type : str
    tags_list : list[Tag]
    reject_reason : str

    def __init__(self, id: str):
        self.id = id
        self.poll_type : str = ''
        self.reject_reason : str = 'не заполнено'

    def get_row_col(self, timestamp: int):
        
        try:
            row = datetime.datetime.fromtimestamp(timestamp).day + 1
            pipeline_offsets = {
                os.getenv('astana_pipeline'): 14,
                os.getenv('almaty_pipeline'): 0,
                os.getenv('pipeline_astana_success'): 14,
                os.getenv('pipeline_almaty_success'): 0,
                os.getenv('pipeline_online'): 28
            }

            poll_type_defaults = {
                'tags': lambda self: 3 + self.tags_list.get_type(),
                'success': lambda self: 11,
                'qualified': lambda self: 8,
                'proccessing': lambda self: 7
            }

            znr = [
                os.getenv('astana_pipeline'),
                os.getenv('almaty_pipeline'),
                os.getenv('pipeline_online')
            ]

            if self.poll_type not in poll_type_defaults:
                raise Exception('Невалидное значение poll_type')

            col = poll_type_defaults[self.poll_type](self)

            if self.poll_type == 'qualified' and self.pipeline_id in znr:
                if self.status_id == os.getenv('status_znr') and self.reject_reason not in ['Передумали', 'Не одобрили рассрочку', 'не заполнено']:
                    raise Exception('Не квалифицированный лид')

            if self.pipeline_id in pipeline_offsets:
                col += pipeline_offsets[self.pipeline_id]
            else:
                raise Exception(f'Неверный pipeline_id: {self.pipeline_id}')

            return col, row
        except Exception as e:
            logger.error(f"Ошибка обработки данных: {e}")
            raise

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
        
    @classmethod
    def from_json(cls, data: dict, poll_type: str) -> "Lead":
        """Обрабатывает вкладку `Основное` в сделке."""
        try:
            self: Lead = cls(data.get("id", ""))
            self.pipeline_id = str(data.get('pipeline_id', ''))
            self.status_id = str(data.get('status_id', ''))
            self.tags_list = Tags.from_json(data)
            
            self.poll_type = poll_type
            fields = data.get("custom_fields_values", []) if data.get("custom_fields_values", []) else []
            for field in fields:
                match field.get("field_name", None):
                    case 'Причина отказа':
                        self.reject_reason = self.__get_value_from_json(field)
                    case _:
                        continue
            return self
        except KeyError as e:
            logger.error(f"Ключ не найден в данных: {e}")
            raise
        except Exception as e:
            logger.error(f"Общая ошибка обработки данных: {e}")
            raise


if __name__ == '__main__':
    tag = Tag(1234, 'sdfkg')
    print(tag.get_regex())
