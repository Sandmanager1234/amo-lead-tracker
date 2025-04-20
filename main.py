import os
import asyncio
from datetime import timedelta
from dotenv import load_dotenv
from loguru import logger

from scheduler import Scheduler
from kztime import get_local_time, get_today
from google_sheets import GoogleSheets
from amocrm.amocrm import AmoCRMClient
from amocrm.models import Leads


# Загрузка переменных из .env файла
load_dotenv()

PIPES = {
    'astana': [os.getenv('astana_pipeline'), os.getenv('pipeline_astana_success')], 
    'almaty': [os.getenv('almaty_pipeline'), os.getenv('pipeline_almaty_success')], 
    'online': [os.getenv('pipeline_online')]
}
PROCESSING_STATUSES = [os.getenv('status_astana_processing'), os.getenv('status_almaty_processing'), os.getenv('status_online_processing')]
FIRST_STATUSES = [os.getenv('status_online_first'), os.getenv('status_astana_first'), os.getenv('status_almaty_first')]
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


async def polling_pipelines(last_update: int):
    today = get_today(last_update)
    for pipeline in PIPES:
        try:
            page = 1
            response = await amo_client.get_leads(today, PIPES[pipeline])
            leads = Leads.from_json(response)
            next = response.get('_links', {}).get('next')
            while next:
                response = await amo_client.get_leads(today, PIPES[pipeline])
                leads.add_leads(Leads.from_json(await amo_client.get_leads(today, PIPES[pipeline], page)))
                next = response.get('_links', {}).get('next')
            
            local_today = get_local_time(local_today)
            google.insert_val(*leads.get_all(pipeline, today), local_today)
            google.insert_val(leads.get_ap_qual(pipeline, today), local_today)
            google.insert_val(leads.get_success(pipeline, today), local_today)
            
        except Exception as ex:
            logger.error(f'Ошибка обработки воронки: {ex}')


async def main():
    scheduler = Scheduler(polling_pipelines, SECONDS)
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