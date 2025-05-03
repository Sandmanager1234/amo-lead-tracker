import datetime as dt
import os

from loguru import logger

from amocrm.amocrm import AmoCRMClient

from leads_counting import get_processed, get_qualified, get_success, get_total
from models_new import LeadsResponse
from sheets_new import SheetWorker, get_letter_by_timestamp, get_range_by_pipeline
from time_new import (
    get_today,
    get_tomorrow,
    get_week_timestamps,
    KZ_TIMEZONE,
)

current_directory_name = os.path.basename(os.getcwd())

os.makedirs(".", exist_ok=True)  # Создание папки logs, если её нет

log_file_path = os.path.join(
    "logs", f"{current_directory_name}_{{time:YYYY-MM-DD}}.log"
)

amo_client = AmoCRMClient(
    base_url="https://teslakz.amocrm.ru",
    access_token=os.getenv("access_token"),
    client_id=os.getenv("client_id"),
    client_secret=os.getenv("client_secret"),
    permanent_access_token=True,
)

logger.add(
    log_file_path,  # Файл лога будет называться по дате и сохраняться в поддиректории с названием текущей директории
    rotation="00:00",  # Ротация каждый день в полночь
    retention="7 days",  # Хранение логов за последние 7 дней
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",  # Формат сообщений в файле
    level="DEBUG",  # Минимальный уровень логирования
    compression="zip",  # Архивирование старых логов
)


async def polling_pipeline(
    pipeline_id: list, ts_from: int = get_today(), ts_to: int = get_tomorrow()
):
    

    logger.info(
        f"Сбор данных для воронки: {pipeline_id} за {dt.datetime.fromtimestamp(ts_from, tz=KZ_TIMEZONE).date()}"
    )
    try:
        data = {}

        response = await amo_client.get_leads(ts_from, ts_to, pipeline_id)

        if response:
            parsed = LeadsResponse.model_validate(response)

            data = {
                "total": get_total(parsed),
                "processed": get_processed(parsed),
                "qualified": get_qualified(parsed),
                "success": get_success(parsed),
            }

        return data

    except Exception as ex:
        logger.error(f"Ошибка обработки воронки: {ex.with_traceback()}")
        return
    finally:
        pass


async def worker():
    sheet = SheetWorker()

    PIPES = {
        "astana": [os.getenv("astana_pipeline"), os.getenv("pipeline_astana_success")],
        "almaty": [os.getenv("almaty_pipeline"), os.getenv("pipeline_almaty_success")],
        "online": [os.getenv("pipeline_online"), os.getenv("pipeline_online_success")],
    }

    logger.info(f"Проход по воронкам за неделю: до {dt.date.today()}")

    amo_client.start_session()

    for day in get_week_timestamps():
        logger.info(
            f"Сбор данных за : {dt.datetime.fromtimestamp(day[0], tz=KZ_TIMEZONE)}"
        )

        tasks = [
            asyncio.create_task(polling_pipeline(pipes, day[0], day[1]))
            for pipes in PIPES.values()
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

    

        for (pipes_name, _), data_total in zip(PIPES.items(), results):
            if isinstance(data_total, Exception):
                logger.error(f"Ошибка обработки воронки {pipes_name}: {data_total}")
                continue

            if not data_total:
                continue

            logger.info(
                f"Обновление данных в Google Sheets для {pipes_name} за {dt.datetime.fromtimestamp(day[0], tz=KZ_TIMEZONE)}"
            )
            sheet.insert_col(
                data_total,
                get_range_by_pipeline(pipes_name),
                get_letter_by_timestamp(day[0]),
                sheet="Лист1",
                include_other_city=(pipes_name == "online"),
            )

    await amo_client.close_session()
        # for pipes_name, pipes in PIPES.items():
        #     logger.info(f"Проход по воронкам {pipes_name} ")
        #     data_total = await polling_pipeline(pipes, day[0], day[1])

        #     logger.success(data_total)

        #     if data_total:
        #         logger.info(
        #             f"Обновление данных в Google Sheets для {pipes_name} за {dt.datetime.fromtimestamp(day[0], tz=KZ_TIMEZONE).date()}"
        #         )

        #         sheet.insert_col(
        #             data_total,
        #             get_range_by_pipeline(pipes_name),
        #             get_letter_by_timestamp(day[0]),
        #             sheet="Лист1",
        #             include_other_city=True if pipes_name == "online" else False,
        #         )

async def simple_scheduler():
    while True:
        try:
            
            await worker()
            logger.info("Завершаем работу на 5 минут")
            await asyncio.sleep(60 * 5)
        except Exception as ex:
            logger.error(f"Ошибка: {ex.with_traceback()}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(simple_scheduler())
