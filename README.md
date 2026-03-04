# Crypto Momentum Intelligence

A live crypto trading intelligence system that ingests 5-minute OHLCV data from GeckoTerminal across Base, BSC, Solana and ETH chains, trains a stacking ensemble ML model every tick, and surfaces buy/sell/neutral recommendations through a React dashboard.

---

## What this system does

1. **Ingests** raw swap data from GeckoTerminal every 5 minutes across 4 chains
2. **Builds** 5-minute OHLCV price candles, token metrics, feature signals and forward-return labels
3. **Trains** a stacking ensemble (XGBoost + Random Forest + Extra Trees + Logistic meta-learner) on all labeled data with 3x loss-boosted sample weights for wrong past predictions
4. **Picks** the top-N tokens by model score and logs them as live picks
5. **Verifies** picks after 2 hours — records win/loss, effective return
6. **Serves** a FastAPI backend (port 8001) consumed by a React/Vite frontend dashboard
7. **Meme Radar** — scrapes Reddit/X for viral tokens and matches them against CoinStats

---

## Prerequisites

- Python 3.11+
- Node.js 18+ and npm
- PostgreSQL 14+ running locally
- Git

---

## First-time setup

### 1. Clone and create virtual environment

```powershell
git clone <repo-url>
cd crypto-momentum-intelligence
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Configure environment

```powershell
copy .env.example .env
```

Edit `.env` with your values:

```dotenv
# PostgreSQL connection
PGHOST=localhost
PGPORT=5432
PGDATABASE=crypto_momentum
PGUSER=postgres
PGPASSWORD=your_password
PGSSLMODE=disable

# Ingestion settings
INGEST_NETWORKS=base,eth,solana,bsc
INGEST_MAX_POOLS=15
INGEST_MAX_TRADES_PER_POOL=30
INGEST_MAX_PAGES_PER_POOL=2
INGEST_LOOKBACK_HOURS=24

# CoinStats API key (required for market data + meme radar)
COINSTATS_API_KEY=your_coinstats_key

# Optional - only needed if using Alchemy as fallback
ALCHEMY_API_KEY=
```

### 3. Create the database and run all migrations in order

```powershell
psql -h localhost -p 5432 -U postgres -c "CREATE DATABASE crypto_momentum;"

psql -h localhost -p 5432 -U postgres -d crypto_momentum -f db/migrations/001_init_raw_schema.sql
psql -h localhost -p 5432 -U postgres -d crypto_momentum -f db/migrations/002_create_token_metrics_5m.sql
psql -h localhost -p 5432 -U postgres -d crypto_momentum -f db/migrations/003_create_features_5m.sql
psql -h localhost -p 5432 -U postgres -d crypto_momentum -f db/migrations/004_create_labels_5m.sql
psql -h localhost -p 5432 -U postgres -d crypto_momentum -f db/migrations/005_create_tracked_pools.sql
psql -h localhost -p 5432 -U postgres -d crypto_momentum -f db/migrations/006_create_token_price_5m.sql
psql -h localhost -p 5432 -U postgres -d crypto_momentum -f db/migrations/007_create_model_picks.sql
psql -h localhost -p 5432 -U postgres -d crypto_momentum -f db/migrations/008_add_context_features_to_features_5m.sql
psql -h localhost -p 5432 -U postgres -d crypto_momentum -f db/migrations/009_add_cross_sectional_rank_features.sql
```

Run them in number order. If a migration fails, check the previous one ran first.

### 4. Install frontend dependencies

```powershell
cd frontend\alpha-whisperer-pro
npm install
cd ..\..
```

---

## Running the system

Open **three** terminal windows from the project root:

### Terminal 1 — FastAPI backend

```powershell
.\runbackend.ps1
```

Starts FastAPI on `http://127.0.0.1:8001` with hot-reload. Serves all API endpoints.

### Terminal 2 — React frontend

```powershell
.\runfrontend.ps1
```

Starts Vite dev server. Open `http://localhost:5173` in your browser.

### Terminal 3 — Live pipeline (continuous loop)

```powershell
.\runlive.ps1 -Loop -LoopIntervalMinutes 5 -TopN 50 -IngestMaxPools 15
```

Runs a full cycle every 5 minutes:

1. Ingests swap data from GeckoTerminal
2. Builds price candles, metrics, features, labels
3. Trains stacking ensemble with feedback weights
4. Saves top-50 picks to `research/live_picks_snapshot.csv` and `model_picks` DB table
5. Verifies picks that are 2h old and records win/loss

#### One-shot single tick (no loop)

```powershell
.\runlive.ps1 -TickCount 1
```

---

## Pipeline parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `-Loop` | off | Run continuously every N minutes |
| `-LoopIntervalMinutes` | 5 | Minutes between ticks |
| `-TopN` | 50 | Number of tokens to score and pick |
| `-IngestMaxPools` | 15 | Max GeckoTerminal pools to ingest per tick |
| `-IngestMaxPagesPerPool` | 2 | Pages of trades per pool |
| `-IngestMaxTradesPerPool` | 30 | Max trades fetched per pool |
| `-IngestLookbackHours` | 24 | Historical lookback window |
| `-MarketApi` | coinstats | Market data source for price enrichment |

---

## Architecture

```
GeckoTerminal API
      |
      v
swaps_raw              <- raw immutable swap events (layer 0)
      |
      v
token_price_5m         <- OHLCV candles from swap ratios (layer 1)
      |
      v
token_metrics_5m       <- aggregated volume, wallet, trade counts (layer 2)
      |
      v
features_5m            <- ML feature signals: velocity, intensity, rank (layer 3)
      |
      v
labels_5m              <- forward return targets (future_return_2h, target_up_5pct_2h) (layer 4)
      |
      v
Stacking Ensemble      <- XGBoost + RF + ET + Logistic meta, retrained each tick
      |
      v
model_picks            <- top-N picks with recommendation + entry price
      |
      v
FastAPI (port 8001)    <- REST API consumed by dashboard
      |
      v
React Dashboard        <- Live Picks, Performance, Meme Radar, Feature Importance
```

---

## ML model details

- **Architecture**: Stacking ensemble — XGBoost, Random Forest, Extra Trees as base learners; Logistic Regression as meta-learner
- **Feature sets**: `v2` (default), `cross_rank`, `base`
- **Label**: `target_up_5pct_2h` — binary, 1 if price rises >5% within 2 hours
- **Training**: Retrains on every tick using all rows with a closed 2h label window (~17K-30K rows)
- **Feedback weights**: Wrong predictions get 3-6x sample weight so the model learns harder from mistakes. Wins get 1.5x reinforcement.
- **Pump guard**: Tokens with >30% 24h price change are capped to Neutral to avoid chasing pumps

### Model recommendations

| Label | Meaning |
|-------|---------|
| `strong_buy` | High probability of >5% gain in 2h — enter position |
| `buy` | Moderate probability — enter position |
| `neutral` | No strong directional signal — no position |
| `sell` | Model predicts no gain / likely decline — avoid / exit |

Only `buy` and `strong_buy` picks contribute to portfolio avg return. Sell picks are avoidance signals — no position is opened.

---

## Dashboard pages

| Page | URL | Description |
|------|-----|-------------|
| Dashboard | `/` | Summary of latest tick |
| Live Picks | `/live` | Current model picks with prices |
| Performance | `/performance` | Win rate, avg return, chain/rec breakdown, verified history |
| Meme Radar | `/meme-radar` | Reddit/X viral tokens matched against CoinStats |
| Run Pipeline | `/run` | Trigger a manual pipeline tick from the UI |
| Settings | `/settings` | Configuration |

---

## Performance metrics explained

- **Win Rate**: % of picks where model direction was correct (sell + price fell = win; buy + price rose = win)
- **Avg Return (2h)**: Average effective 2h return across `buy`/`strong_buy` picks only, capped at +/-500% for outliers
- **Best/Worst on chain cards**: Best and worst return among `buy`/`strong_buy` picks only (sell/neutral excluded)
- **Outlier badge**: Appears on table rows where `|return| > 500%` — those picks are capped at 500% in avg calculation

---

## Key files

| File | Purpose |
|------|---------|
| `run_full_live_cycle.py` | Orchestrates full tick: ingest -> features -> labels -> pick -> verify |
| `runlive.ps1` | PowerShell wrapper to start the pipeline loop |
| `runbackend.ps1` | Starts FastAPI backend (uvicorn, port 8001) |
| `runfrontend.ps1` | Starts Vite dev server (port 5173) |
| `backend/api.py` | All FastAPI endpoints |
| `backend/meme_radar.py` | Reddit/X scraping + CoinStats coin matching |
| `research/live_top_coins.py` | Model training + scoring + pick generation (runs each tick) |
| `research/feedback_loop.py` | Win/loss outcome tracking + sample weight computation |
| `research/live_picks_snapshot.csv` | Latest picks snapshot used for 2h verification |
| `research/feature_importance.json` | Written after each tick with current feature importances |
| `ingestion/data_sources/gecko_provider.py` | GeckoTerminal API client |
| `pipeline.log` | Pipeline stdout log |
| `pipeline_err.log` | Pipeline stderr log |

---

## Checking pipeline status

```powershell
# Is the pipeline running? (returns 1 or 2 if yes)
(Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -match 'run_full_live_cycle' } | Measure-Object).Count

# Last 20 lines of pipeline log
Get-Content .\pipeline.log -Tail 20

# Last 10 lines of error log
Get-Content .\pipeline_err.log -Tail 10
```

## Stopping the pipeline

```powershell
$procs = Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -match 'run_full_live_cycle' }
$procs | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
Write-Host "Stopped $($procs.Count) process(es)"
```

---

## Database quick checks

```sql
-- Row counts per table
SELECT COUNT(*) FROM swaps_raw;
SELECT COUNT(*) FROM token_price_5m;
SELECT COUNT(*) FROM features_5m;
SELECT COUNT(*) FROM labels_5m WHERE future_return_2h IS NOT NULL;

-- Latest model picks
SELECT symbol, chain, recommendation, picked_at_utc, score
FROM model_picks ORDER BY picked_at_utc DESC LIMIT 20;

-- Latest verified picks with returns
SELECT symbol, recommendation, return_2h, is_win
FROM model_picks WHERE return_2h IS NOT NULL
ORDER BY picked_at_utc DESC LIMIT 20;
```

---

## Troubleshooting

**Pipeline hangs on pool X/Y**
GeckoTerminal rate-limiting mid-ingest. Pool gets skipped after retries. Normal behaviour — reduce with `-IngestMaxPools 10`.

**No picks generated**
Labels need a closed 2h window to exist. After first ingest, wait ~2 hours before picks appear.

**"No meme-coin matches found" in Meme Radar**
Check `COINSTATS_API_KEY` is set in `.env`. Also normal if trending posts don't mention known coins by name.

**Frontend shows stale data**
Performance data polls every 5 minutes. Hard-refresh (Ctrl+Shift+R) or wait for next poll.

**`psycopg2` not found in terminal**
The `.venv` uses `psycopg` v3, not `psycopg2`. Always use `.venv\Scripts\python.exe` for pipeline commands. For raw DB queries use pgAdmin or `psql` directly.

---

## Deployment note

This system is **not suitable for free-tier cloud** due to the ML retrain every 5 minutes on 17K+ rows.

- Frontend: Vercel or Netlify (free)
- Backend + Pipeline + PostgreSQL: $6/month Hetzner VPS (2 vCPU, 2 GB RAM) or DigitalOcean Droplet
