from __future__ import annotations

import os
from getpass import getpass
from typing import Any

import psycopg
from dotenv import load_dotenv

from data_sources.alchemy_provider import AlchemyProvider
from data_sources.gecko_provider import GeckoProvider
from data_sources.provider_factory import build_provider
from data_sources.types import DataSourceError, DataSourceRateLimitError, NormalizedSwap


def get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_db_password() -> str:
    password = os.getenv("PGPASSWORD")
    if password:
        return password
    return getpass("PostgreSQL password for PGUSER: ")


def fetch_recent_swaps(
    network: str,
    gecko_base_url: str,
    max_pools: int,
    max_trades_per_pool: int,
    max_pages_per_pool: int,
    lookback_hours: int,
    primary_source: str,
    fallback_source: str | None,
    pool_addresses: list[str],
    alchemy_approx_blocks_per_hour: int,
) -> tuple[list[NormalizedSwap], str]:
    primary_provider = build_provider(primary_source, network, gecko_base_url)

    try:
        if isinstance(primary_provider, GeckoProvider):
            swaps = primary_provider.fetch_swaps(
                max_pools=max_pools,
                max_trades_per_pool=max_trades_per_pool,
                max_pages_per_pool=max_pages_per_pool,
                lookback_hours=lookback_hours,
                pool_addresses=pool_addresses if pool_addresses else None,
            )
            return swaps, primary_source

        if isinstance(primary_provider, AlchemyProvider):
            swaps = primary_provider.fetch_swaps(
                pool_addresses=pool_addresses,
                lookback_hours=lookback_hours,
                approx_blocks_per_hour=alchemy_approx_blocks_per_hour,
            )
            return swaps, primary_source
    except (DataSourceRateLimitError, DataSourceError, ValueError) as error:
        print(f"Primary provider '{primary_source}' failed: {error}")
        if not fallback_source:
            raise

    fallback_provider = build_provider(fallback_source, network, gecko_base_url)
    print(f"Trying fallback provider '{fallback_source}'")

    if isinstance(fallback_provider, GeckoProvider):
        swaps = fallback_provider.fetch_swaps(
            max_pools=max_pools,
            max_trades_per_pool=max_trades_per_pool,
            max_pages_per_pool=max_pages_per_pool,
            lookback_hours=lookback_hours,
            pool_addresses=pool_addresses if pool_addresses else None,
        )
        return swaps, fallback_source

    if isinstance(fallback_provider, AlchemyProvider):
        swaps = fallback_provider.fetch_swaps(
            pool_addresses=pool_addresses,
            lookback_hours=lookback_hours,
            approx_blocks_per_hour=alchemy_approx_blocks_per_hour,
        )
        return swaps, fallback_source

    raise ValueError(f"Unsupported fallback provider: {fallback_source}")


def ensure_token(cursor: psycopg.Cursor[Any], token_address: str, chain: str) -> None:
    short = token_address[:8]
    cursor.execute(
        """
        INSERT INTO tokens (token_address, symbol, name, created_at, chain)
        VALUES (%s, %s, %s, NOW(), %s)
        ON CONFLICT (token_address) DO NOTHING
        """,
        (token_address, f"TKN_{short}", f"Token {short}", chain),
    )


def load_pool_addresses_from_db(chain: str) -> list[str]:
    """Load tracked pool addresses from the database for the given chain."""
    conn = psycopg.connect(
        host=get_env("PGHOST"),
        port=int(get_env("PGPORT", "5432")),
        dbname=get_env("PGDATABASE"),
        user=get_env("PGUSER"),
        password=get_db_password(),
        sslmode=get_env("PGSSLMODE", "disable"),
    )
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT pool_address FROM tracked_pools WHERE chain = %s ORDER BY discovered_at",
                (chain,),
            )
            rows = cursor.fetchall()
    return [row[0] for row in rows]


def insert_swaps(swaps: list[NormalizedSwap], chain: str) -> tuple[int, int]:
    if not swaps:
        return 0, 0

    conn = psycopg.connect(
        host=get_env("PGHOST"),
        port=int(get_env("PGPORT", "5432")),
        dbname=get_env("PGDATABASE"),
        user=get_env("PGUSER"),
        password=get_db_password(),
        sslmode=get_env("PGSSLMODE", "disable"),
    )

    inserted = 0
    skipped_existing = 0
    with conn:
        with conn.cursor() as cursor:
            last_block = None
            for swap in swaps:
                if last_block is not None and swap.block_number < last_block:
                    print("Warning: non-monotonic block number detected in fetched trades")
                last_block = swap.block_number

                ensure_token(cursor, swap.token_address, chain)

                cursor.execute(
                    """
                    INSERT INTO swaps_raw (
                        token_address,
                        tx_hash,
                        block_number,
                        timestamp,
                        buyer_address,
                        seller_address,
                        amount_token,
                        amount_usd,
                        side
                    )
                    SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM swaps_raw WHERE tx_hash = %s
                    )
                    """,
                    (
                        swap.token_address,
                        swap.tx_hash,
                        swap.block_number,
                        swap.timestamp,
                        swap.buyer_address,
                        swap.seller_address,
                        swap.amount_token,
                        swap.amount_usd,
                        swap.side,
                        swap.tx_hash,
                    ),
                )
                if cursor.rowcount == 1:
                    inserted += 1
                else:
                    skipped_existing += 1

    return inserted, skipped_existing


def _parse_networks() -> list[str]:
    """Return list of networks from INGEST_NETWORKS (comma-sep) or legacy INGEST_NETWORK."""
    raw = os.getenv("INGEST_NETWORKS", "").strip()
    if raw:
        return [n.strip().lower() for n in raw.split(",") if n.strip()]
    # Fallback to legacy single-network var
    return [get_env("INGEST_NETWORK", "base").lower()]


def ingest_network(network: str) -> None:
    """Run swap ingestion for a single network/chain."""
    base_url = get_env("GECKOTERMINAL_BASE_URL", "https://api.geckoterminal.com/api/v2")
    primary_source = get_env("PRIMARY_DATA_SOURCE", "gecko").lower()
    fallback_source = os.getenv("FALLBACK_DATA_SOURCE", "alchemy")
    fallback_source = fallback_source.lower() if fallback_source else None

    max_pools = int(get_env("INGEST_MAX_POOLS", "3"))
    max_trades_per_pool = int(get_env("INGEST_MAX_TRADES_PER_POOL", "25"))
    max_pages_per_pool = int(get_env("INGEST_MAX_PAGES_PER_POOL", "8"))
    lookback_hours = int(get_env("INGEST_LOOKBACK_HOURS", "72"))

    alchemy_approx_blocks_per_hour = int(get_env("ALCHEMY_APPROX_BLOCKS_PER_HOUR", "1800"))

    # Load pool addresses from DB (tracked_pools table).
    # Falls back to ALCHEMY_POOL_ADDRESSES env var or empty list.
    db_pool_addresses = load_pool_addresses_from_db(chain=network)
    if db_pool_addresses:
        pool_addresses = db_pool_addresses
        print(f"Loaded {len(pool_addresses)} tracked pools from DB for chain={network}")
    else:
        pool_addresses = [
            address.strip().lower()
            for address in os.getenv("ALCHEMY_POOL_ADDRESSES", "").split(",")
            if address.strip()
        ]
        if pool_addresses:
            print(f"No DB pools found; using {len(pool_addresses)} pool(s) from ALCHEMY_POOL_ADDRESSES env")
        else:
            print("No DB pools found and no env pool addresses; provider will discover pools dynamically")

    if primary_source == "alchemy" and not pool_addresses:
        raise ValueError("Alchemy provider requires pool addresses (populate tracked_pools or set ALCHEMY_POOL_ADDRESSES)")

    swaps, provider_used = fetch_recent_swaps(
        network=network,
        gecko_base_url=base_url,
        max_pools=max_pools,
        max_trades_per_pool=max_trades_per_pool,
        max_pages_per_pool=max_pages_per_pool,
        lookback_hours=lookback_hours,
        primary_source=primary_source,
        fallback_source=fallback_source,
        pool_addresses=pool_addresses,
        alchemy_approx_blocks_per_hour=alchemy_approx_blocks_per_hour,
    )

    inserted, skipped_existing = insert_swaps(swaps, chain=network)
    print(
        f"  [{network}] ingestion complete | "
        f"provider={provider_used} fetched={len(swaps)} inserted={inserted} skipped_existing_tx={skipped_existing}"
    )


def main() -> None:
    load_dotenv()

    networks = _parse_networks()
    print(f"Multi-chain ingestor starting — networks: {', '.join(networks)}")

    for network in networks:
        print(f"\n--- Ingesting chain: {network} ---")
        try:
            ingest_network(network)
        except Exception as err:
            print(f"  [{network}] ingestion FAILED: {err}")
            import traceback
            traceback.print_exc()

    print(f"\nAll chains done ({len(networks)} networks processed).")


if __name__ == "__main__":
    main()