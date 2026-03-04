from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass
class NormalizedSwap:
    token_address: str
    tx_hash: str
    block_number: int
    timestamp: datetime
    buyer_address: str | None
    seller_address: str | None
    amount_token: Decimal
    amount_usd: Decimal | None
    side: str


class DataSourceError(Exception):
    pass


class DataSourceRateLimitError(DataSourceError):
    pass