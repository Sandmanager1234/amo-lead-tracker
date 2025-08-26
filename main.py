import os
import asyncio
from dotenv import load_dotenv
from loguru import logger
# from schedule import repeat, run_pending, every

from kztime import get_local_datetime, get_last_week_list, get_today_info, get_last_month
from google_sheets.google_sheets import GoogleSheets
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


async def polling_pipelines():
    amo_client.start_session()
    start_ts, _, day = get_last_month()
    try:
        page = 1
        response = await amo_client.get_last_month_leads(PIPES, start_ts)
        await asyncio.sleep(0.3)
        if response:
            leads = Leads.from_json(response)
            next = response.get('_links', {}).get('next', None)
            while next:
                page += 1
                response = await amo_client.get_last_month_leads(PIPES, start_ts, page)
                if response:
                    leads.add_leads(Leads.from_json(response))
                next = response.get('_links', {}).get('next', None)
                await asyncio.sleep(0.2)
        leads_data = leads.get_column_data(PIPES)
        google.insert_leads_data(leads_data, day)
        google.insert_leads_data_vertical(leads_data)
        # записать в гугол
    except Exception as ex: 
        logger.error(f'Ошибка обработки воронки: {ex}')
    finally:
        await amo_client.close_session()


def test():
    _, _, day = get_today_info()
    import datetime
    start_day = day.replace(month=7) - datetime.timedelta()
    # import json
    # with open('lead_data_json.json', 'r',encoding='utf8') as file:
    #     leads_data = json.load(file)
    # google.insert_leads_data(leads_data, start_day)    
    # google.insert_leads_data_vertical(leads_data)
    print(start_day)
    ws = google.get_vertical_sheet(8, 2025, 'online')
    # print(google.tg.create_vertical_shablon('online'))
    # print(ws)
    # shab, m = google.tg.create_shablon(day)
    # print(len(shab[0]))


# @repeat(every(6).minutes)
def main():
    asyncio.run(polling_pipelines())


# Запуск приложения
if __name__ == "__main__":
    # while True:
        # run_pending()
    main()