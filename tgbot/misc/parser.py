from datetime import datetime
from typing import List

from create_bot import logger
from tgbot.models.sql_connector import InstrumentsDAO, SpreadStatisticsDAO
from tgbot.services.moex import MoexStock, MoexFutures


class Parser:

    def __init__(self):
        self.candles = {
            "1 min": 1,
            "10 min": 10,
            "60 min": 60,
            "1 day": 24,
            "1 week": 7
        }

    async def __match_stock_and_futures(self,
                                        base_ticker: str,
                                        future_ticker: str,
                                        interval_string: str,
                                        start_date: datetime,
                                        end_date: datetime) -> List[dict]:
        interval_number = self.candles[interval_string]
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")
        logger.info(start_date)
        logger.info(end_date)
        logger.info(interval_number)
        base_data = await MoexStock.get_candles_data(ticker=base_ticker,
                                                     start_date=start_date,
                                                     end_date=end_date,
                                                     interval=interval_number)
        future_data = await MoexFutures.get_candles_data(ticker=future_ticker,
                                                         start_date=start_date,
                                                         end_date=end_date,
                                                         interval=interval_number)
        result = []
        logger.info(base_data)
        logger.info(future_data)
        for base in base_data:
            for future in future_data:
                if datetime.strptime(base["begin"], "%Y-%m-%d %H:%M:%S") == datetime.strptime(future["begin"],
                                                                                              "%Y-%m-%d %H:%M:%S"):
                    high_variation = base["high"] - future["high"]
                    low_variation = base["low"] - future["low"]
                    average_variation = (high_variation + low_variation) / 2
                    result.append(dict(f10760=base_ticker,
                                       f10770=future_ticker,
                                       f11230=datetime.strptime(base["begin"], "%Y-%m-%d %H:%M:%S"),
                                       f11240=base["high"],
                                       f11250=future["high"],
                                       f11270=base["low"],
                                       f11260=future["low"],
                                       f11280=high_variation,
                                       f11290=low_variation,
                                       f11300=average_variation))
                    break
        return result

    async def parser(self) -> List[dict]:
        instruments = await InstrumentsDAO.get_many(f11430="НЕТ")
        text = []
        for item in instruments:
            data = await self.__match_stock_and_futures(base_ticker=item["f11370"],
                                                        future_ticker=item["f11380"],
                                                        interval_string=item["f11410"],
                                                        start_date=item["f11390"],
                                                        end_date=item["f11400"])
            await SpreadStatisticsDAO.create_many(data=data)
            text.append(dict(base_ticker=item["f11370"], future_ticker=item["f11380"], quantity=len(data)))
        return text
