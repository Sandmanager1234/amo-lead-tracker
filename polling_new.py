import datetime as dt
import json
import os

from loguru import logger

from amocrm.amocrm import AmoCRMClient

from leads_counting import get_processed, get_qualified, get_success, get_total
from models_new import LeadsResponse


amo_client = AmoCRMClient(
    base_url="https://teslakz.amocrm.ru",
    access_token=os.getenv("access_token"),
    client_id=os.getenv("client_id"),
    client_secret=os.getenv("client_secret"),
    permanent_access_token=True,
)


def get_yesterday():
    kz_timezone = dt.timezone(dt.timedelta(hours=5))
    yesterday = (dt.datetime.now(kz_timezone) - dt.timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return int(yesterday.timestamp())


def get_today():
    kz_timezone = dt.timezone(dt.timedelta(hours=5))
    today = dt.datetime.now(kz_timezone).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return int(today.timestamp())


def get_tomorrow():
    kz_timezone = dt.timezone(dt.timedelta(hours=5))
    tomorrow = (dt.datetime.now(kz_timezone) + dt.timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return int(tomorrow.timestamp())


async def polling_pipeline(pipeline_id: int):
    amo_client.start_session()

    logger.info(f"Сбор данных для воронки: {pipeline_id} за сегодня")
    try:
        
        # response = await amo_client.get_leads(
        #     get_today(), get_tomorrow(), [pipeline_id]
        # )

        response = await amo_client.get_leads(
            1743457200, 1743543600, [pipeline_id]
        )

        print(
            json.dumps(response, indent=4, ensure_ascii=False),
            file=open("response.txt", "w", encoding="utf-8"),
        )

        if response:
            parsed = LeadsResponse.model_validate(response)

            print(f"Общее: {get_total(parsed)}")
            print(f'Обработано: {get_processed(parsed)}')
            print(f'Квалифицировано: {get_qualified(parsed)}')
            print(f"Успешно: {get_success(parsed)}")  

            # for lead in parsed.leads:
            #     print()
            #     print(
            #         f"ID: {lead.id}, Создан: {lead.created_at}, Статус: {lead.status_id}"
            #     )
            #     print(
            #         f"Reject reason: {lead.reject_reason}, Success: {lead.is_success}, After processing: {lead.is_after_processing}, Qualified: {lead.is_qualified}"
            #     )
            #     for tag in lead.tags:
            #         print(f"  {tag.name} — тип: {tag.type}")

    except Exception as ex:
        logger.error(f"Ошибка обработки воронки: {ex.with_traceback()}")
        return
    finally:
        await amo_client.close_session()


if __name__ == "__main__":
    import asyncio

    pipeline_id = 7747517
    asyncio.run(polling_pipeline(pipeline_id))
