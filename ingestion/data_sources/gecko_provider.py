from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import requests

from .types import DataSourceError, DataSourceRateLimitError, NormalizedSwap


def parse_utc_timestamp(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def to_decimal(value: Any) -> Decimal:
    return Decimal(str(value))


# EVM chains use hex addresses that are safely lowercased.
# Non-EVM chains (e.g. Solana) use case-sensitive base58 addresses.
_EVM_NETWORKS: frozenset[str] = frozenset({
    "base", "eth", "bsc", "polygon_pos", "arbitrum", "optimism", "avalanche",
})


def _norm_addr(address: str, network: str) -> str:
    """Normalise an address: lowercase for EVM, preserve case for others (Solana etc)."""
    return address.lower() if network in _EVM_NETWORKS else address


class GeckoProvider:
    def __init__(self, base_url: str, network: str) -> None:
        self.base_url = base_url
        self.network = network

    def _fetch_json(self, url: str) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(1, 6):
            try:
                response = requests.get(
                    url,
                    headers={"Accept": "application/json;version=20230302"},
                    timeout=30,
                )
                if response.status_code == 429:
                    sleep_seconds = min(2**attempt, 20)
                    time.sleep(sleep_seconds)
                    if attempt == 5:
                        raise DataSourceRateLimitError(f"GeckoTerminal rate limited: {url}")
                    continue
                response.raise_for_status()
                return response.json()
            except requests.RequestException as error:
                last_error = error
                if attempt == 5:
                    raise DataSourceError(f"GeckoTerminal request failed: {error}") from error
                time.sleep(min(2**attempt, 20))

        if last_error is not None:
            raise DataSourceError(f"GeckoTerminal request failed: {last_error}")
        raise DataSourceError("Unexpected Gecko fetch failure")

    def get_base_pool_addresses(self, max_pools: int) -> list[str]:
        data = self._fetch_json(f"{self.base_url}/networks/{self.network}/trending_pools?page=1")
        pools: list[str] = []
        for item in data.get("data", []):
            pool_id = item.get("id", "")
            if "_" not in pool_id:
                continue
            pools.append(_norm_addr(pool_id.split("_", 1)[1], self.network))
            if len(pools) >= max_pools:
                break
        return pools

    def map_trade_to_swap(self, attributes: dict[str, Any]) -> NormalizedSwap | None:
        tx_hash = attributes.get("tx_hash")
        block_number = attributes.get("block_number")
        block_timestamp = attributes.get("block_timestamp")
        kind = attributes.get("kind")

        if not tx_hash or not block_number or not block_timestamp or kind not in {"buy", "sell"}:
            return None

        from_token_address = attributes.get("from_token_address")
        to_token_address = attributes.get("to_token_address")
        tx_from_address = attributes.get("tx_from_address")

        from_token_amount = attributes.get("from_token_amount")
        to_token_amount = attributes.get("to_token_amount")
        volume_in_usd = attributes.get("volume_in_usd")

        if kind == "sell":
            token_address = from_token_address
            amount_token = from_token_amount
            buyer_address = None
            seller_address = tx_from_address
        else:
            token_address = to_token_address
            amount_token = to_token_amount
            buyer_address = tx_from_address
            seller_address = None

        if not token_address or amount_token is None:
            return None

        network = self.network
        return NormalizedSwap(
            token_address=_norm_addr(str(token_address), network),
            tx_hash=_norm_addr(str(tx_hash), network),
            block_number=int(block_number),
            timestamp=parse_utc_timestamp(str(block_timestamp)),
            buyer_address=(_norm_addr(str(buyer_address), network) if buyer_address else None),
            seller_address=(_norm_addr(str(seller_address), network) if seller_address else None),
            amount_token=to_decimal(amount_token),
            amount_usd=(to_decimal(volume_in_usd) if volume_in_usd is not None else None),
            side=kind,
        )

    def fetch_swaps(
        self,
        max_pools: int,
        max_trades_per_pool: int,
        max_pages_per_pool: int,
        lookback_hours: int,
        pool_addresses: list[str] | None = None,
    ) -> list[NormalizedSwap]:
        swaps: list[NormalizedSwap] = []
        seen_tx_hashes: set[str] = set()
        min_timestamp = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)

        if not pool_addresses:
            pool_addresses = self.get_base_pool_addresses(max_pools)
        else:
            pool_addresses = pool_addresses[:max_pools]

        total_pools = len(pool_addresses)
        for pool_idx, pool_address in enumerate(pool_addresses, 1):
            should_stop_pool = False
            for page in range(1, max_pages_per_pool + 1):
                try:
                    payload = self._fetch_json(
                        f"{self.base_url}/networks/{self.network}/pools/{pool_address}/trades?page={page}"
                    )
                except DataSourceError as err:
                    print(f"  Skipping pool {pool_address[:10]}… page {page}: {err}")
                    break
                page_rows = payload.get("data", [])
                if not page_rows:
                    break

                for trade in page_rows[:max_trades_per_pool]:
                    attributes = trade.get("attributes", {})
                    mapped = self.map_trade_to_swap(attributes)
                    if mapped is None:
                        continue
                    if mapped.timestamp < min_timestamp:
                        should_stop_pool = True
                        break
                    if mapped.tx_hash in seen_tx_hashes:
                        continue
                    seen_tx_hashes.add(mapped.tx_hash)
                    swaps.append(mapped)

                if should_stop_pool:
                    break

            if pool_idx % 10 == 0:
                print(f"  Gecko progress: {pool_idx}/{total_pools} pools processed, {len(swaps)} swaps so far")
            # Throttle between pools to avoid rate limiting
            time.sleep(0.5)

        swaps.sort(key=lambda row: (row.block_number, row.timestamp))
        return swaps