from pydantic import BaseModel, Field
from typing import List

class PriceInfo(BaseModel):
    """価格情報"""
    close: float = Field(..., description="現在の足の終値")
    prev_close: float = Field(..., description="前の足の終値")

class ChangeInfo(BaseModel):
    """変動情報"""
    pct: float = Field(..., description="価格変動率 (%)")
    direction: str = Field(..., description="変動方向 ('up' または 'down')")

class VolatilityData(BaseModel):
    """変動率データ本体"""
    symbol: str = Field(..., description="銘柄シンボル")
    timeframe: str = Field(..., description="タイムフレーム")
    candle_ts: int = Field(..., description="ローソク足の開始タイムスタンプ (ミリ秒)")
    price: PriceInfo
    change: ChangeInfo
    
    class Config:
        from_attributes = True

class VolatilityResponse(BaseModel):
    """APIレスポンス全体"""
    count: int = Field(..., description="返されたデータ件数")
    data: List[VolatilityData]

class ErrorDetail(BaseModel):
    code: str
    message: str

class ErrorResponse(BaseModel):
    error: ErrorDetail
