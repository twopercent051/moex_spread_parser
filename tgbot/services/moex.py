import asyncio
from typing import List

import requests

import apimoex


class MoexMain:
    market = None
    engine = None

    @classmethod
    async def get_candles_data(cls, ticker: str, start_date: str, end_date: str, interval: int) -> List[dict]:
        with requests.Session() as session:
            return apimoex.get_market_candles(session=session,
                                              security=ticker,
                                              start=start_date,
                                              end=end_date,
                                              interval=interval,
                                              market=cls.market,
                                              engine=cls.engine)


class MoexStock(MoexMain):
    market = "shares"
    engine = "stock"


class MoexFutures(MoexMain):
    market = "forts"
    engine = "futures"


async def test():
    a = await MoexStock.get_candles_data(ticker="VTBR", start_date="2023-10-11", end_date="2023-10-12", interval=60)
    b = await MoexFutures.get_candles_data(ticker="VBH4", start_date="2023-10-11", end_date="2023-10-12", interval=60)
    # print(a)
    print(b)


if __name__ == "__main__":
    asyncio.run(test())
