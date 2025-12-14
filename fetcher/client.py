import aiohttp
import asyncio
import logging
from typing import List, Any, Optional

class BybitClient:
    def __init__(self, base_url: str, logger: logging.Logger):
        self.base_url = base_url
        self.logger = logger
        self.timeout = aiohttp.ClientTimeout(total=10)

    async def get_all_linear_symbols(self, session: aiohttp.ClientSession) -> List[str]:
        url = f"{self.base_url}/v5/market/instruments-info"
        symbols, cursor = [], ""
        self.logger.info("全Linear銘柄(USDT無期限)を取得中...")
        while True:
            params = {"category": "linear", "status": "Trading", "limit": 1000, "cursor": cursor}
            try:
                async with session.get(url, params={k: v for k, v in params.items() if v}) as response:
                    response.raise_for_status()
                    data = await response.json()
                    if data["retCode"] != 0:
                        self.logger.error(f"APIエラー: {data['retMsg']}")
                        break
                    result = data.get("result", {})
                    symbols.extend([item["symbol"] for item in result.get("list", []) if item.get("symbol", "").endswith("USDT")])
                    cursor = result.get("nextPageCursor", "")
                    if not cursor: break
                    await asyncio.sleep(0.1)
            except aiohttp.ClientError as e:
                self.logger.error(f"銘柄取得リクエストエラー: {e}")
                return []
        self.logger.info(f"合計 {len(symbols)} の取引可能なLinear銘柄を発見")
        return symbols

    async def get_kline_data(self, session: aiohttp.ClientSession, symbol: str, interval: str, limit: int = 5) -> Optional[List[List[Any]]]:
        params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
        try:
            async with session.get(f"{self.base_url}/v5/market/kline", params=params) as response:
                response.raise_for_status()
                data = await response.json()
                if data.get("retCode") == 0:
                    result_list = [[int(i[0]), float(i[1]), float(i[2]), float(i[3]), float(i[4]), float(i[5])] for i in data.get("result", {}).get("list", [])]
                    return result_list
                else:
                    self.logger.warning(f"{symbol} ({interval}) K線取得APIエラー: {data.get('retMsg')}")
                    return None
        except (aiohttp.ClientError, ValueError, TypeError, KeyError) as e:
            self.logger.warning(f"{symbol} ({interval}) K線取得リクエスト/パースエラー: {e}")
            return None
