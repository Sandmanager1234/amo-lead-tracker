
from __future__ import annotations
import asyncio
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy import Integer, String, DateTime, TIMESTAMP
from sqlalchemy.orm import mapped_column
from sqlalchemy.ext.asyncio import create_async_engine

class Base(AsyncAttrs, DeclarativeBase):
    pass

class Lead(Base):
    __tablename__ = "lead"
    id: Mapped[int] = mapped_column(primary_key=True)
    pipeline_id: Mapped[int] = mapped_column(Integer())
    status_id: Mapped[str] = mapped_column(String(50))
    poll_type: Mapped[int] = mapped_column(Integer())
    tags_type: Mapped[int] = mapped_column(Integer())
    reject_reason: Mapped[str] = mapped_column(String(50))
    created_at: Mapped[int] = mapped_column(Integer())
    updated_at: Mapped[int] = mapped_column(Integer())
    
    
async def async_main():
    engine = create_async_engine(
        "sqlite+aiosqlite:///example.db",
        echo=True,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(async_main())
    
