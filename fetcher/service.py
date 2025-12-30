import asyncio
import time
import logging

import aiohttp

from client import BybitClient
from repository import DatabaseRepository
from config import AppConfig, TIMEFRAME_MAP

class DataFetchService:
    def __init__(self, client: BybitClient, repository: DatabaseRepository, config: AppConfig, logger: logging.Logger):
        self.client = client
        self.repository = repository
        self.config = config
        self.logger = logger

    async def fetch_and_store_data(self):
        start_time = time.time()
        self.logger.info("====== 新しいデータ取得サイクルを開始 ======")

        async with aiohttp.ClientSession(timeout=self.client.timeout) as session:
            symbols = await self.client.get_all_linear_symbols(session)
            if not symbols:
                self.logger.error("銘柄が取得できず、データ取得をスキップします。")
                return

            self.logger.info(f"対象タイムフレーム: {self.config.timeframes}")

            for timeframe_str in self.config.timeframes:
                timeframe_str = timeframe_str.strip()
                if not timeframe_str: continue

                interval = TIMEFRAME_MAP.get(timeframe_str)
                if not interval:
                    self.logger.warning(f"未対応のタイムフレーム: {timeframe_str}。スキップします。")
                    continue

                self.logger.info(f"--- タイムフレーム: {timeframe_str} ({interval}) のデータ取得を開始 ---")

                # 既存データの最新タイムスタンプを取得
                latest_timestamps = self.repository.get_latest_timestamps(timeframe_str)

                sem = asyncio.Semaphore(self.config.concurrency_limit)

                async def fetch_one(symbol: str):
                    last_ts = latest_timestamps.get(symbol)
                    async with sem:
                        if last_ts:
                            # 既存データがある場合は、そのタイムスタンプから最新までを取得 (limitは最大200とする)
                            # startを指定すると、その時刻を含む足から取得されるため、重複はupsertで解決
                            return await self.client.get_kline_data(session, symbol, interval, limit=200, start=last_ts)
                        else:
                            # 新規の場合は設定された履歴数だけ取得
                            return await self.client.get_kline_data(session, symbol, interval, limit=self.config.ohlcv_history_limit)

                tasks = [fetch_one(symbol) for symbol in symbols]
                results = await asyncio.gather(*tasks)

                records_to_upsert = []
                for symbol, ohlcv_data in zip(symbols, results):
                    if ohlcv_data:
                        for row in ohlcv_data:
                            records_to_upsert.append((
                                symbol, row[0], row[1], row[2], row[3], row[4], row[5], row[6]
                            ))

                if records_to_upsert:
                    self.repository.upsert_ohlcv_data(timeframe_str, records_to_upsert)

                    upserted_symbols = {rec[0] for rec in records_to_upsert}
                    self.repository.cleanup_old_ohlcv_data(timeframe_str, upserted_symbols, self.config.ohlcv_history_limit)

                self.logger.info(f"--- タイムフレーム: {timeframe_str} のデータ取得が完了 ---")

        end_time = time.time()
        self.logger.info(f"====== データ取得サイクル完了 (所要時間: {end_time - start_time:.2f}秒) ======")
