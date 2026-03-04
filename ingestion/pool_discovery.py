from __future__ import annotations

import os
import time
from typing import Any

import psycopg
import requests
from dotenv import load_dotenv


def get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_db_password() -> str:
    password = os.getenv("PGPASSWORD")
    if password:
        return password
    from getpass import getpass
    return getpass("PostgreSQL password for PGUSER: ")


def fetch_json(url: str) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            response = requests.get(
                url,
                headers={"Accept": "application/json;version=20230302"},
                timeout=30,
            )
            if response.status_code == 429:
                time.sleep(min(2**attempt, 20))
                if attempt == 5:
                    raise RuntimeError(f"Rate limited: {url}")
                continue
            response.raise_for_status()
            return response.json()
        except requests.RequestException as error:
            last_error = error
            if attempt == 5:
                raise
            time.sleep(min(2**attempt, 20))
    if last_error is not None:
        raise last_error
    raise RuntimeError("Unexpected fetch failure")


def discover_pools(
    base_url: str,
    network: str,
    max_pages: int,
) -> list[dict[str, str]]:
    """Fetch trending + top pools from GeckoTerminal and return normalized pool info."""
    pools: list[dict[str, str]] = []
    seen_pool_addresses: set[str] = set()

    endpoints = [
        f"{base_url}/networks/{network}/trending_pools",
        f"{base_url}/networks/{network}/pools",
    ]

    for endpoint in endpoints:
        for page in range(1, max_pages + 1):
            try:
                data = fetch_json(f"{endpoint}?page={page}")
            except Exception as error:
                print(f"  Warning: failed to fetch {endpoint} page {page}: {error}")
                break

            items = data.get("data", [])
            if not items:
                break

            for item in items:
                pool_id = item.get("id", "")
                if "_" not in pool_id:
                    continue
                # Preserve case for non-EVM chains (e.g. Solana base58 addresses)
                _evm = network in {
                    "base", "eth", "bsc", "polygon_pos", "arbitrum", "optimism", "avalanche"
                }
                raw_pool = pool_id.split("_", 1)[1]
                pool_address = raw_pool.lower() if _evm else raw_pool
                if pool_address in seen_pool_addresses:
                    continue

                attrs = item.get("attributes", {})
                pool_name = attrs.get("name", "")

                relationships = item.get("relationships", {})
                base_token = relationships.get("base_token", {})
                base_token_data = base_token.get("data", {})
                base_token_id = base_token_data.get("id", "")

                if "_" not in base_token_id:
                    continue
                raw_token = base_token_id.split("_", 1)[1]
                token_address = raw_token.lower() if _evm else raw_token

                dex_data = relationships.get("dex", {}).get("data", {})
                dex_id = dex_data.get("id", "unknown") if dex_data else "unknown"

                seen_pool_addresses.add(pool_address)
                pools.append({
                    "pool_address": pool_address,
                    "token_address": token_address,
                    "dex": dex_id,
                    "pool_name": pool_name,
                })

            time.sleep(1.5)

    return pools


def upsert_pools(
    pools: list[dict[str, str]],
    chain: str,
    source: str,
) -> tuple[int, int]:
    if not pools:
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
    skipped = 0

    with conn:
        with conn.cursor() as cursor:
            for pool in pools:
                token_address = pool["token_address"]
                pool_address = pool["pool_address"]
                dex = pool.get("dex", "unknown")

                short = token_address[:8]
                cursor.execute(
                    """
                    INSERT INTO tokens (token_address, symbol, name, created_at, chain)
                    VALUES (%s, %s, %s, NOW(), %s)
                    ON CONFLICT (token_address) DO NOTHING
                    """,
                    (token_address, f"TKN_{short}", f"Token {short}", chain),
                )

                cursor.execute(
                    """
                    INSERT INTO tracked_pools (pool_address, token_address, dex, source, chain)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (pool_address) DO NOTHING
                    """,
                    (pool_address, token_address, dex, source, chain),
                )
                if cursor.rowcount == 1:
                    inserted += 1
                else:
                    skipped += 1

    return inserted, skipped


def _parse_networks() -> list[str]:
    """Return list of networks from INGEST_NETWORKS (comma-sep) or legacy INGEST_NETWORK."""
    raw = os.getenv("INGEST_NETWORKS", "").strip()
    if raw:
        return [n.strip().lower() for n in raw.split(",") if n.strip()]
    return [get_env("INGEST_NETWORK", "base").lower()]


def main() -> None:
    load_dotenv()

    networks = _parse_networks()
    base_url = get_env("GECKOTERMINAL_BASE_URL", "https://api.geckoterminal.com/api/v2")
    max_pages = int(get_env("DISCOVERY_MAX_PAGES", "3"))

    print(f"Multi-chain pool discovery — networks: {', '.join(networks)}")

    for network in networks:
        print(f"\nDiscovering pools on {network}...")
        try:
            pools = discover_pools(base_url=base_url, network=network, max_pages=max_pages)
            print(f"Found {len(pools)} distinct pools on {network}.")

            inserted, skipped = upsert_pools(pools, chain=network, source="gecko_discovery")
            print(
                f"  [{network}] discovery complete | new_pools={inserted} already_tracked={skipped} "
                f"total_discovered={len(pools)}"
            )
        except Exception as err:
            print(f"  [{network}] discovery FAILED: {err}")
            import traceback
            traceback.print_exc()

    print(f"\nAll chains done ({len(networks)} networks).")


if __name__ == "__main__":
    main()
