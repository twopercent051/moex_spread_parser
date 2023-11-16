from datetime import datetime
from typing import List, Literal

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
                                        end_date: datetime,
                                        base_multiplier: int,
                                        future_multiplier: int,
                                        base_instrument_type: Literal["Акция", "Фьючерс", ""],
                                        future_instrument_type: Literal["Акция", "Фьючерс", ""]) -> List[dict]:
        interval_number = self.candles[interval_string]
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")
        base_request_data = dict(ticker=base_ticker, start_date=start_date, end_date=end_date, interval=interval_number)
        future_request_data = dict(ticker=future_ticker,
                                   start_date=start_date,
                                   end_date=end_date,
                                   interval=interval_number)
        if base_instrument_type in ["Акция", ""]:
            base_data = MoexStock.get_candles_data(**base_request_data)
        else:
            base_data = MoexFutures.get_candles_data(**base_request_data)
        if future_instrument_type in ["Фьючерс", ""]:
            future_data = MoexFutures.get_candles_data(**future_request_data)
        else:
            future_data = MoexStock.get_candles_data(**future_request_data)
        result = []
        for base in base_data:
            for future in future_data:
                if datetime.strptime(base["begin"], "%Y-%m-%d %H:%M:%S") == datetime.strptime(future["begin"],
                                                                                              "%Y-%m-%d %H:%M:%S"):
                    base_high = base["high"] * int(base_multiplier)
                    base_low = base["low"] * int(base_multiplier)
                    future_high = future["high"] * int(future_multiplier)
                    future_low = future["low"] * int(future_multiplier)
                    high_variation = base_high - future_high
                    low_variation = base_low - future_low
                    average_variation = (high_variation + low_variation) / 2
                    result.append(dict(f10760=base_ticker,
                                       f10770=future_ticker,
                                       f11230=datetime.strptime(base["begin"], "%Y-%m-%d %H:%M:%S"),
                                       f11240=base_high,
                                       f11250=future_high,
                                       f11270=base_low,
                                       f11260=future_low,
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
                                                        end_date=item["f11400"],
                                                        base_multiplier=item["f11740"],
                                                        future_multiplier=item["f11750"],
                                                        base_instrument_type=item["f11930"],
                                                        future_instrument_type=item["f11940"])
            await SpreadStatisticsDAO.create_many(data=data)
            text.append(dict(base_ticker=item["f11370"], future_ticker=item["f11380"], quantity=len(data)))
        return text
