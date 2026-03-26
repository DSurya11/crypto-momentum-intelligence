import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

conn = psycopg.connect(
    host=os.getenv("PGHOST"),
    port=int(os.getenv("PGPORT", "5432")),
    dbname=os.getenv("PGDATABASE"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD"),
    sslmode=os.getenv("PGSSLMODE", "disable"),
)

with conn:
    with conn.cursor() as cur:
        print("Truncating all time-series data tables...")
        cur.execute("""
            TRUNCATE TABLE 
                features_5m, 
                labels_5m, 
                token_metrics_5m, 
                token_price_5m, 
                swaps_raw, 
                liquidity_events_raw, 
                price_ohlcv_raw, 
                social_raw,
                pick_outcomes,
                tracked_pools
            CASCADE;
        """)
        print("Done truncating tables.")

csv_path = "d:\\crypto-momentum-intelligence\\research\\live_picks_snapshot.csv"
if os.path.exists(csv_path):
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write("picked_at_utc,bucket_timestamp,rank,symbol,name,token_address,chain,score,entry_close_price\n")
    print("Reset live_picks_snapshot.csv")
