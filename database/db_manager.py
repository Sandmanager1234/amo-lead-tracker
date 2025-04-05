from loguru import logger
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from .models import Base, Lead
from amocrm.models import Lead as LeadObject



class DBManager:
    def __init__(self, db_url):
        logger.info(f"Подключение к БД: {db_url}")
        self.engine = create_async_engine(db_url, echo=False)
        self.async_session = async_sessionmaker(self.engine, expire_on_commit=False)
        logger.info("Подключение установлено")

    async def create_tables(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def dispose(self):
        await self.engine.dispose()

    async def get_lead(self, lead_id: int):
        async with self.async_session() as session:
            async with session.begin():
                lead = await session.get(Lead, lead_id)
                if lead:
                    logger.info(f"Найдена сделка с id {lead_id}!")
                else:
                    logger.info(f"Не найдена сделка с id {lead_id}")
                return lead

    async def add_lead(self, lead: LeadObject):
        async with self.async_session() as session:
            async with session.begin():
                session.add(Lead(
                    id=lead.id,
                    status_id=lead.status_id,
                    pipeline_id=lead.pipeline_id,
                    poll_type=lead.poll_type,
                    tags_type=lead.tags_type,
                    reject_reason=lead.reject_reason,
                    created_at=lead.created_at,
                    updated_at=lead.updated_at,
                ))
                await session.commit()
                logger.info(f"Добавлена сделка с id {lead.id}")

    async def update_lead(self, lead: LeadObject | Lead):
        async with self.async_session() as session:
            async with session.begin():
                db_lead = await session.get(Lead, lead.id)
                if db_lead:
                    db_lead.status_id = lead.status_id
                    db_lead.pipeline_id = lead.pipeline_id
                    db_lead.poll_type = lead.poll_type
                    db_lead.tags_type = lead.tags_type
                    db_lead.reject_reason = lead.reject_reason
                    db_lead.created_at = lead.created_at
                    db_lead.updated_at = lead.updated_at
                    await session.commit()
                    logger.info(f"Обновлена сделка с id {lead.id}")
                else:
                    logger.info(f"Не найдена сделка с id {lead.id} для обновления")

    

