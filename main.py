import os
import asyncio
from kztime import get_current_time
from datetime import timedelta
from google_sheets import GoogleSheets
from amocrm.amocrm import AmoCRMClient
from amocrm.models import Lead, Events
from dotenv import load_dotenv
from loguru import logger

from scheduler import Scheduler

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
    permanent_access_token=True,
)

async def processing_leads(events: Events, poll_type: str):
    for i, event in enumerate(events):
        if i < 1:
            try:
                lead_json = await amo_client.get_lead(event.entity_id)
                if lead_json:
                    lead = Lead.from_json(lead_json, poll_type=poll_type)
                    timestamp = event.created_at
                    if poll_type == 'proccessing':
                        from_timestamp = int((get_current_time() - timedelta(weeks=1)).replace(hour=0, minute=0, microsecond=0, second=0).timestamp()) 
                        events = Events.from_json(await amo_client.get_events_processing_before(lead.id, from_timestamp))
                        timestamp = events.get_timestamp_by_index()
                    google.insert_value(*lead.get_row_col(timestamp))
            except Exception as ex:
                logger.error(f'Ошибка обработки сделки: {ex}')
        else:
            exit()


async def polling_leads(timestamp):
    amo_client.start_session()
    try:

        tags_events = Events.from_json(await amo_client.get_events_tags(timestamp))
        await processing_leads(tags_events, 'tags')
 
        success_events = Events.from_json(await amo_client.get_events_success(timestamp))
        await processing_leads(success_events, 'success')
        
        qualified_events = Events.from_json(await amo_client.get_events_qualified(timestamp))
        await processing_leads(qualified_events, 'qualified')

        from_processing_events = Events.from_json(await amo_client.get_events_from_processing(timestamp))
        await processing_leads(from_processing_events, 'proccessing')

    except Exception as e:
        logger.error(f'Ошибка составления запроса: {e}')
    finally:
        await amo_client.close_session()


async def main():
    scheduler = Scheduler(polling_leads, SECONDS)
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