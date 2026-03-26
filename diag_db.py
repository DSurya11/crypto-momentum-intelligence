import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

def _conn():
    try:
        return psycopg.connect(
            host=os.getenv("PGHOST", "localhost"),
            port=int(os.getenv("PGPORT", "5432")),
            dbname=os.getenv("PGDATABASE"),
            user=os.getenv("PGUSER"),
            password=os.getenv("PGPASSWORD"),
        )
    except Exception as e:
        print(f"Error connecting: {e}")
        return None

conn = _conn()
if conn:
    with conn.cursor() as cur:
        print("--- Query 1: Zero Returns Count ---")
        cur.execute("SELECT chain, COUNT(*) AS total, COUNT(*) FILTER (WHERE return_2h = 0) AS zero_returns FROM pick_outcomes GROUP BY chain")
        for row in cur.fetchall():
            print(row)

        print("\n--- Query 2: Range of returns per chain ---")
        cur.execute("SELECT chain, MIN(return_2h), MAX(return_2h), AVG(return_2h) FROM pick_outcomes GROUP BY chain")
        for row in cur.fetchall():
            print(row)

        print("\n--- Query 3: Examples from BASE with return=0 ---")
        cur.execute("SELECT token_address, entry_price, price_2h, return_2h FROM pick_outcomes WHERE chain='base' AND return_2h = 0 LIMIT 5")
        for row in cur.fetchall():
            print(row)

        print("\n--- Query 4: Checking if prices are null ---")
        cur.execute("SELECT COUNT(*) FROM pick_outcomes WHERE price_2h IS NULL")
        print(f"Null exit prices: {cur.fetchone()[0]}")
    conn.close()
