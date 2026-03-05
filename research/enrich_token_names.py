"""
One-time (and safe to re-run) backfill: fetch real symbol/name from CoinStats
for every token in the `tokens` table that still has a TKN_ placeholder.

Usage:
    python research/enrich_token_names.py [--dry-run] [--batch-size 50]
"""
from __future__ import annotations

import argparse
import json
import os
import urllib.parse
import urllib.request

import psycopg
from dotenv import load_dotenv


def get_db_conn() -> psycopg.Connection:
    return psycopg.connect(
        host=os.environ["PGHOST"],
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ.get("PGPASSWORD", ""),
        sslmode=os.getenv("PGSSLMODE", "disable"),
    )


def coinstats_lookup(addresses: list[str], api_key: str) -> dict[str, dict]:
    """Return {lower_address: {symbol, name, price}} for matched coins."""
    if not addresses:
        return {}

    lower_to_orig: dict[str, str] = {}
    for a in addresses:
        if a:
            lower_to_orig[a.lower()] = a

    blockchains = os.getenv("COINSTATS_BLOCKCHAINS", "").strip()
    params: dict[str, str] = {
        "contractAddresses": ",".join(lower_to_orig.values()),
        "limit": str(max(20, len(addresses))),
    }
    if blockchains:
        params["blockchains"] = blockchains

    url = f"https://openapiv1.coinstats.app/coins?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(
        url,
        headers={
            "X-API-KEY": api_key,
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode())
    except Exception as e:
        print(f"  CoinStats error: {e}")
        return {}

    result = payload.get("result") or []
    mapped: dict[str, dict] = {}
    requested = set(lower_to_orig.keys())
    for coin in result:
        addr_list: list[str] = []
        sa = coin.get("contractAddress")
        if isinstance(sa, str) and sa:
            addr_list.append(sa.lower())
        for entry in coin.get("contractAddresses") or []:
            if isinstance(entry, str):
                addr_list.append(entry.lower())
            elif isinstance(entry, dict):
                v = entry.get("contractAddress") or entry.get("address")
                if isinstance(v, str):
                    addr_list.append(v.lower())
        for addr in addr_list:
            if addr not in requested:
                continue
            mapped[addr] = {
                "symbol": coin.get("symbol"),
                "name": coin.get("name"),
            }
            orig = lower_to_orig.get(addr)
            if orig and orig != addr:
                mapped[orig] = mapped[addr]
    return mapped


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Show what would be updated, without writing")
    parser.add_argument("--batch-size", type=int, default=50, help="Addresses per CoinStats request")
    args = parser.parse_args()

    api_key = os.getenv("COINSTATS_API_KEY", "").strip()
    if not api_key:
        print("ERROR: COINSTATS_API_KEY not set in environment.")
        return

    conn = get_db_conn()

    # Fetch all tokens that still have dummy placeholder names
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT token_address, chain, symbol, name
            FROM tokens
            WHERE symbol LIKE 'TKN_%%' OR name LIKE 'Token %%'
            ORDER BY token_address
            """
        )
        stale = cur.fetchall()

    print(f"Found {len(stale)} token(s) with placeholder names.")
    if not stale:
        print("Nothing to do.")
        return

    total_updated = 0
    batch_size = args.batch_size

    for batch_start in range(0, len(stale), batch_size):
        batch = stale[batch_start : batch_start + batch_size]
        addresses = [r[0] for r in batch]
        print(f"  Querying CoinStats for addresses {batch_start+1}–{batch_start+len(batch)} …", end=" ", flush=True)
        market = coinstats_lookup(addresses, api_key)
        print(f"{len(market)} matched")

        if not market:
            continue

        with conn.cursor() as cur:
            for addr, data, *_ in [(*r,) for r in batch]:
                info = market.get(addr) or market.get(addr.lower())
                if not info:
                    continue
                real_symbol = info.get("symbol")
                real_name = info.get("name")
                if not real_symbol:
                    continue
                if args.dry_run:
                    print(f"    [DRY-RUN] {addr[:12]}… → symbol={real_symbol!r} name={real_name!r}")
                    total_updated += 1
                else:
                    cur.execute(
                        """
                        UPDATE tokens
                           SET symbol = %s,
                               name   = COALESCE(%s, name)
                         WHERE token_address = %s
                           AND (symbol LIKE 'TKN_%%' OR name LIKE 'Token %%')
                        """,
                        (real_symbol, real_name, addr),
                    )
                    total_updated += cur.rowcount

        if not args.dry_run:
            conn.commit()

    print(f"\n{'[DRY-RUN] Would update' if args.dry_run else 'Updated'} {total_updated} token(s).")
    conn.close()


if __name__ == "__main__":
    main()
