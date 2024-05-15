from pydantic import BaseModel
from datetime import datetime

class MarketData(BaseModel):
    id: int
    symbol: str
    datetime: datetime
    last_price: float
    trades: int
    total_volume: int
    total_buying_volume: int
    total_selling_volume: int
    bar_delta: int
    delta_percent: float
    delta_for_price: int
    ask_volume_for_price: int
    bid_volume_for_price: int
    total_volume_for_price: int
    max_ask_volume: int
    price_with_max_ask_volume: str
    max_bid_volume: int
    price_with_max_bid_volume: str
    max_combined_volume: int
    price_with_max_combined_volume: str
    max_positive_delta: int
    max_negative_delta: int
    max_seen_delta: int
    min_seen_delta: int
    cumulative_delta: int

    class Config:
        orm_mode = True
