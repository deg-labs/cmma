from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from sqlalchemy.orm import Session
from typing import List
from enum import Enum

import crud
import schemas
from database import engine, get_db

app = FastAPI(
    title="CMMA API",
    description="BybitのOHLCVデータから価格変動率を計算するAPI",
    version="2.0.0",
)

# --- エラーハンドリング ---
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content=schemas.ErrorResponse(
            error=schemas.ErrorDetail(
                code=exc.headers.get("X-Error-Code", "HTTP_EXCEPTION"),
                message=exc.detail
            )
        ).model_dump(),
    )

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    # バリデーションエラーのメッセージを整形
    error_messages = []
    for error in exc.errors():
        field = "->".join(map(str, error['loc']))
        message = error['msg']
        error_messages.append(f"[{field}]: {message}")
    
    return JSONResponse(
        status_code=422,
        content=schemas.ErrorResponse(
            error=schemas.ErrorDetail(
                code="INVALID_INPUT",
                message=", ".join(error_messages)
            )
        ).model_dump(),
    )

# --- パラメータ用Enum ---
class Direction(str, Enum):
    up = "up"
    down = "down"
    both = "both"

class SortBy(str, Enum):
    volatility_desc = "volatility_desc"
    volatility_asc = "volatility_asc"
    symbol_asc = "symbol_asc"

VALID_TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1M"]

# --- エンドポイント ---
@app.get(
    "/volatility", 
    response_model=schemas.VolatilityResponse,
    summary="価格変動率の高い銘柄を取得",
    response_description="条件に一致した銘柄の変動率データ"
)
def read_volatility(
    timeframe: str = Query(..., description=f"タイムフレームを指定。有効値: {', '.join(VALID_TIMEFRAMES)}"),
    price_threshold: float = Query(..., gt=0, description="価格変動率の閾値(%)。絶対値で比較されます。例: 5.0", alias="threshold"),
    offset: int = Query(1, gt=0, description="何本前のローソク足と比較するか。デフォルトは1 (1本前)。"),
    direction: Direction = Query(Direction.both, description="変動方向をフィルタ"),
    sort: SortBy = Query(SortBy.volatility_desc, description="結果のソート順"),
    limit: int = Query(100, gt=0, le=500, description="取得する最大件数"),
    db: Session = Depends(get_db)
):
    if timeframe not in VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"無効なタイムフレームです。有効な値: {', '.join(VALID_TIMEFRAMES)}",
            headers={"X-Error-Code": "INVALID_TIMEFRAME"},
        )
    
    results = crud.get_symbols_exceeding_threshold(
        db=db, 
        timeframe=timeframe, 
        price_threshold=price_threshold,
        offset=offset,
        direction=direction.value,
        sort=sort.value,
        limit=limit
    )
    
    # crudからの結果をレスポンスモデルに変換
    volatility_data = [
        schemas.VolatilityData(
            symbol=row.symbol,
            timeframe=row.timeframe,
            candle_ts=row.candle_ts,
            price=schemas.PriceInfo(
                close=row.close,
                prev_close=row.prev_close
            ),
            change=schemas.ChangeInfo(
                pct=round(row.volatility_pct, 4),
                direction="up" if row.volatility_pct > 0 else "down"
            )
        ) for row in results
    ]

    return schemas.VolatilityResponse(count=len(volatility_data), data=volatility_data)

@app.get("/", include_in_schema=False)
def read_root():
    return {"message": "Welcome to CMMA API v2. See /docs for details."}
