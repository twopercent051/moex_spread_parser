import asyncio
from typing import List

from sqlalchemy import MetaData, DateTime, Column, Integer, String, select, insert
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, as_declarative

from tgbot.config import load_config

config = load_config(".env")

DATABASE_URL = f'mysql+aiomysql://{config.db.user}:{config.db.password}@{config.db.host}:{config.db.port}/{config.db.database}'

engine = create_async_engine(DATABASE_URL)

async_session_maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


@as_declarative()
class Base:
    metadata = MetaData()


class InstrumentsDB(Base):
    __tablename__ = "cb_data590"

    id = Column(Integer, nullable=False, autoincrement=True, primary_key=True)
    f11370 = Column(String)  # Базовый актив
    f11380 = Column(String)  # Производное базового
    f11430 = Column(String)  # Исследование проведено
    f11390 = Column(DateTime)  # Начало
    f11400 = Column(DateTime)  # Конец
    f11410 = Column(String)  # Бары


class SpreadStatisticsDB(Base):
    __tablename__ = "cb_data550"

    id = Column(Integer, nullable=False, autoincrement=True, primary_key=True)
    f10760 = Column(String)  # Базовый актив
    f10770 = Column(String)  # Производное базового
    f11230 = Column(DateTime)  # Дата и время бара
    f11240 = Column(Integer)  # High базовый
    f11250 = Column(Integer)  # High производное
    f11270 = Column(Integer)  # Low базовый
    f11260 = Column(Integer)  # Low производное
    f11280 = Column(Integer)  # Отклонение High
    f11290 = Column(Integer)  # Отклонение Low
    f11300 = Column(Integer)  # Отклонение среднее


class BaseDAO:
    """Класс взаимодействия с БД"""
    model = None

    @classmethod
    async def get_many(cls, **filter_by) -> list:
        async with async_session_maker() as session:
            query = select(cls.model.__table__.columns).filter_by(**filter_by).order_by(cls.model.id.asc())
            result = await session.execute(query)
            await engine.dispose()
            return result.mappings().all()

    @classmethod
    async def create(cls, **data):
        async with async_session_maker() as session:
            stmt = insert(cls.model).values(**data)
            await session.execute(stmt)
            await engine.dispose()
            await session.commit()

    @classmethod
    async def create_many(cls, data: List[dict]):
        async with async_session_maker() as session:
            stmt = insert(cls.model).values(data)
            await session.execute(stmt)
            await engine.dispose()
            await session.commit()


class InstrumentsDAO(BaseDAO):
    model = InstrumentsDB


class SpreadStatisticsDAO(BaseDAO):
    model = SpreadStatisticsDB


async def test():
    # users = await TickersDB.get_many()
    # print(users)
    # await engine.dispose()
    print(1.005**2000)


if __name__ == "__main__":
    asyncio.run(test())