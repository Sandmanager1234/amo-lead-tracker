import os
import asyncio
from datetime import timedelta
from dotenv import load_dotenv
from loguru import logger

from scheduler import Scheduler
from kztime import get_local_time, get_timestamp_last_week
from google_sheets import GoogleSheets
from amocrm.amocrm import AmoCRMClient
from amocrm.models import Lead, Events, Tag
from database.db_manager import DBManager

# Загрузка переменных из .env файла
load_dotenv()

SECONDS = int(os.getenv('seconds'))
# Получение имени текущей директории
current_directory_name = os.path.basename(os.getcwd())

os.makedirs(".", exist_ok=True)  # Создание папки logs, если её нет

log_file_path = os.path.join(
    "logs", f"{current_directory_name}_{{time:YYYY-MM-DD}}.log"
)


# Настройка ротации логов
logger.add(
    log_file_path,  # Файл лога будет называться по дате и сохраняться в поддиректории с названием текущей директории
    rotation="00:00",  # Ротация каждый день в полночь
    retention="7 days",  # Хранение логов за последние 7 дней
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",  # Формат сообщений в файле
    level="INFO",  # Минимальный уровень логирования
    compression="zip",  # Архивирование старых логов
)

google = GoogleSheets()
amo_client = AmoCRMClient(
    base_url="https://teslakz.amocrm.ru",
    access_token=os.getenv("access_token"),
    client_id=os.getenv("client_id"),
    client_secret=os.getenv("client_secret"),
    permanent_access_token=True
)
dbmanager = DBManager(
    db_url=os.getenv("db_url")
)

async def processing_leads(events: Events, poll_type: str):
    for event in events:
        try:
            if event.event_type != 'entity_tag_added':
                lead_from_db = await dbmanager.get_lead(event.entity_id)
                if not lead_from_db or (event.after_value != lead_from_db.pipeline_id and poll_type == 'tags') or poll_type != 'tags':
                    lead_json = await amo_client.get_lead(event.entity_id)
                # провекра наличие сделки в бд
                # если есть ничего не делать (добавить смену статуса в бд) по типу обработки
                # если нет +1 гугл табл (всё, что снизу) + добавить в бд
                    if lead_json:                    
                        lead = Lead.from_json(lead_json, poll_type=poll_type)
                        if not lead_from_db:
                            await dbmanager.add_lead(lead)
                        else: 
                            await dbmanager.update_lead(lead)
                        timestamp = get_local_time(lead.created_at)
                    else:
                        lead = lead_from_db
                        timestamp = get_local_time(lead.created_at)
                    if timestamp > get_timestamp_last_week() and poll_type != 'news':
                        google.insert_value(*lead.get_row_col(timestamp), timestamp=timestamp)
                else:
                    if poll_type == 'tags':
                        # если сделка есть в бд, то просто обновить статус
                        lead_from_db.updated_at = event.created_at
                        await dbmanager.update_lead(lead_from_db)
            else:
                lead_from_db = await dbmanager.get_lead(event.entity_id)
                lead = Lead.from_dbmodel(lead_from_db)
                lead.tags_type = Tag(name=event.after_value).target_type
                lead.updated_at = event.created_at
                await dbmanager.update_lead(lead)
                if lead.created_at > get_timestamp_last_week():
                    google.insert_value(*lead.get_row_col(lead.created_at), timestamp=lead.created_at)
                
        except Exception as ex:
            logger.error(f'Ошибка обработки сделки: {ex}')


async def polling_leads(timestamp):
    amo_client.start_session()
    try:
        logger.info(f'Запрос событий по новым сделкам. TIMESTAMP: {timestamp}. TAG: news')
        tags_events = Events.from_json(await amo_client.get_events_new_leads(timestamp)) # события, которые попали в первичку (таргет, какие звонобот, и прочее) / сделки записываем в таблицу по created_at
        await processing_leads(tags_events, 'news') # тут нихуя не обрабатывать
        logger.info(f'Запрос событий по сделкам, к которым добавили тэг. TIMESTAMP: {timestamp}. TAG: add_tag')
        tags_events = Events.from_json(await amo_client.get_events_added_tag(timestamp)) # события, которые попали в первичку (таргет, какие звонобот, и прочее) / сделки записываем в таблицу по created_at
        await processing_leads(tags_events, 'add_tag') # тут нихуя не обрабатывать
        logger.info(f'Запрос событий по сделкам, которые пришли в первичный контакт. TIMESTAMP: {timestamp}. TAG: tags')
        tags_events = Events.from_json(await amo_client.get_events_tags(timestamp)) # события, которые попали в первичку (таргет, какие звонобот, и прочее) / сделки записываем в таблицу по created_at
        await processing_leads(tags_events, 'tags') # тут нихуя не обрабатывать
        logger.info(f'Запрос событий по сделкам, которые успешно завершились. TIMESTAMP: {timestamp}. TAG: success')
        success_events = Events.from_json(await amo_client.get_events_success(timestamp)) # завершились
        await processing_leads(success_events, 'success') # 
        logger.info(f'Запрос событий по сделкам, которые вышли из обработки. TIMESTAMP: {timestamp}. TAG: proccessing')
        from_processing_events = Events.from_json(await amo_client.get_events_from_processing(timestamp)) # вышли из статуса в обработке
        await processing_leads(from_processing_events, 'proccessing')
        logger.info(f'Запрос событий по сделкам, которые квалифицировались. TIMESTAMP: {timestamp}. TAG: qualified')
        qualified_events = Events.from_json(await amo_client.get_events_qualified(timestamp)) # сделки, которые попали в статус квал и знр 
        await processing_leads(qualified_events, 'qualified')


    except Exception as e:
        logger.error(f'Ошибка составления запроса: {e}')
    finally:
        await amo_client.close_session()


async def main():
    scheduler = Scheduler(polling_leads, SECONDS)
    await dbmanager.create_tables()  # Создание таблиц в БД
    while True:
        try:
            await scheduler.start()
            await asyncio.sleep(600)
        except Exception as ex:
            logger.error(f'Ошибка: {ex}')
        finally:
            await scheduler.stop()


# Запуск приложения
if __name__ == "__main__":
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())