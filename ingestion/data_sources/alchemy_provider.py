from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import requests

from .types import DataSourceError, DataSourceRateLimitError, NormalizedSwap

SWAP_TOPIC_V2 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
TOKEN0_SELECTOR = "0x0dfe1681"
TOKEN1_SELECTOR = "0xd21220a7"


def hex_to_int(value: str) -> int:
    return int(value, 16)


def decode_word(data_hex: str, index: int) -> int:
    raw = data_hex[2:] if data_hex.startswith("0x") else data_hex
    start = index * 64
    end = start + 64
    return int(raw[start:end], 16)


def topic_to_address(topic: str) -> str:
    return "0x" + topic[-40:]


# Mapping of GeckoTerminal network id → Alchemy RPC subdomain
_ALCHEMY_RPC_SUBDOMAINS: dict[str, str] = {
    "base": "base-mainnet",
    "eth": "eth-mainnet",
    "polygon_pos": "polygon-mainnet",
    "arbitrum": "arb-mainnet",
    "optimism": "opt-mainnet",
}


class AlchemyProvider:
    def __init__(self, api_key: str, network: str = "base") -> None:
        subdomain = _ALCHEMY_RPC_SUBDOMAINS.get(network)
        if not subdomain:
            raise ValueError(
                f"Alchemy provider does not support network '{network}'. "
                f"Supported: {', '.join(_ALCHEMY_RPC_SUBDOMAINS)}"
            )
        self.api_key = api_key
        self.network = network
        self.rpc_url = f"https://{subdomain}.g.alchemy.com/v2/{api_key}"

    def _rpc(self, method: str, params: list[Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(1, 6):
            try:
                response = requests.post(
                    self.rpc_url,
                    json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                    timeout=30,
                )
                if response.status_code == 429:
                    time.sleep(min(2**attempt, 20))
                    if attempt == 5:
                        raise DataSourceRateLimitError(f"Alchemy rate limited for {method}")
                    continue
                response.raise_for_status()
                payload = response.json()
                if "error" in payload:
                    message = payload["error"].get("message", "Unknown Alchemy RPC error")
                    raise DataSourceError(f"Alchemy RPC {method} failed: {message}")
                return payload.get("result")
            except requests.RequestException as error:
                last_error = error
                if attempt == 5:
                    raise DataSourceError(f"Alchemy request failed: {error}") from error
                time.sleep(min(2**attempt, 20))

        if last_error is not None:
            raise DataSourceError(f"Alchemy request failed: {last_error}")
        raise DataSourceError("Unexpected Alchemy RPC failure")

    def _get_latest_block_number(self) -> int:
        result = self._rpc("eth_blockNumber", [])
        return hex_to_int(result)

    def _get_block_timestamp(self, block_number: int, cache: dict[int, datetime]) -> datetime:
        if block_number in cache:
            return cache[block_number]
        result = self._rpc("eth_getBlockByNumber", [hex(block_number), False])
        if not result:
            raise DataSourceError(f"Missing block data for block {block_number}")
        timestamp_seconds = hex_to_int(result["timestamp"])
        timestamp = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
        cache[block_number] = timestamp
        return timestamp

    def _get_token_address(self, pool_address: str, selector: str, cache: dict[str, str]) -> str:
        key = f"{pool_address}:{selector}"
        if key in cache:
            return cache[key]
        result = self._rpc(
            "eth_call",
            [{"to": pool_address, "data": selector}, "latest"],
        )
        if not result or len(result) < 66:
            raise DataSourceError(f"Unable to decode token address for pool {pool_address}")
        address = "0x" + result[-40:]
        cache[key] = address.lower()
        return cache[key]

    def fetch_swaps(
        self,
        pool_addresses: list[str],
        lookback_hours: int,
        approx_blocks_per_hour: int,
    ) -> list[NormalizedSwap]:
        if not pool_addresses:
            return []

        latest_block = self._get_latest_block_number()
        lookback_blocks = max(lookback_hours * approx_blocks_per_hour, 1)
        from_block = max(latest_block - lookback_blocks, 0)

        filter_params: dict[str, Any] = {
            "fromBlock": hex(from_block),
            "toBlock": hex(latest_block),
            "topics": [SWAP_TOPIC_V2],
            "address": pool_addresses,
        }
        logs = self._rpc("eth_getLogs", [filter_params])
        if not logs:
            return []

        token_cache: dict[str, str] = {}
        block_time_cache: dict[int, datetime] = {}
        swaps: list[NormalizedSwap] = []
        seen_tx_hashes: set[str] = set()

        for log in logs:
            tx_hash = str(log.get("transactionHash", "")).lower()
            if not tx_hash or tx_hash in seen_tx_hashes:
                continue

            topics = log.get("topics", [])
            data = str(log.get("data", "0x"))
            if len(topics) < 3 or not data.startswith("0x"):
                continue

            try:
                amount0_in = decode_word(data, 0)
                amount1_in = decode_word(data, 1)
                amount0_out = decode_word(data, 2)
                amount1_out = decode_word(data, 3)
                block_number = hex_to_int(log["blockNumber"])
            except (ValueError, KeyError, IndexError):
                continue

            if (amount0_in + amount1_in + amount0_out + amount1_out) == 0:
                continue

            pool_address = str(log.get("address", "")).lower()
            if not pool_address:
                continue

            token0 = self._get_token_address(pool_address, TOKEN0_SELECTOR, token_cache)
            _ = self._get_token_address(pool_address, TOKEN1_SELECTOR, token_cache)

            is_buy_token0 = amount0_out > amount0_in
            side = "buy" if is_buy_token0 else "sell"
            amount_token_raw = amount0_out if is_buy_token0 else amount0_in
            if amount_token_raw <= 0:
                amount_token_raw = amount1_out if amount1_out > amount1_in else amount1_in
            if amount_token_raw <= 0:
                continue

            buyer = topic_to_address(topics[2]).lower() if is_buy_token0 else None
            seller = topic_to_address(topics[1]).lower() if not is_buy_token0 else None
            timestamp = self._get_block_timestamp(block_number, block_time_cache)

            swaps.append(
                NormalizedSwap(
                    token_address=token0,
                    tx_hash=tx_hash,
                    block_number=block_number,
                    timestamp=timestamp,
                    buyer_address=buyer,
                    seller_address=seller,
                    amount_token=Decimal(amount_token_raw),
                    amount_usd=None,
                    side=side,
                )
            )
            seen_tx_hashes.add(tx_hash)

        swaps.sort(key=lambda row: (row.block_number, row.timestamp))
        return swaps