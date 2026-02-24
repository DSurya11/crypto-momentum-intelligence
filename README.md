# crypto-momentum-intelligence

Phase 1 foundation: PostgreSQL raw immutable data store for crypto momentum intelligence.

## Phase 1 scope (DB only)

This repo currently defines only raw ingestion tables:

- `tokens`
- `swaps_raw`
- `liquidity_events_raw`
- `price_ohlcv_raw`
- `social_raw`

No feature/scoring/model/alerts tables are included in Phase 1.

## Quick start (PostgreSQL)

1. Create a PostgreSQL database (example: `crypto_momentum`).
2. Copy `.env.example` to `.env` and fill connection values.
3. Run schema migration:

```powershell
psql -h localhost -p 5432 -U postgres -d crypto_momentum -f db/migrations/001_init_raw_schema.sql
```

## Design guarantees

- UTC-safe timestamps via `TIMESTAMPTZ`
- Required time-series indexes on `(token_address, timestamp)`
- Raw table immutability enforced by trigger (no `UPDATE` / `DELETE`)
- Numeric precision preserved via `NUMERIC(30,10)` for on-chain and price values
