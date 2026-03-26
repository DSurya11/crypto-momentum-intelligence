import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

def _conn():
    return psycopg.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
    )

with _conn() as conn:
    with conn.cursor() as cur:
        # Check by chain
        print("--- Stats By Chain (14d) ---")
        cur.execute("""
            SELECT chain, COUNT(*), 
            SUM(CASE WHEN is_win THEN 1 ELSE 0 END),
            AVG(effective_return), 
            MIN(effective_return), MAX(effective_return)
            FROM pick_outcomes
            WHERE picked_at_utc >= NOW() - INTERVAL '14 days'
            GROUP BY chain
        """)
        for row in cur.fetchall():
            print(f"Chain {row[0]}: Total={row[1]}, Wins={row[2]}, Avg={row[3]:.2f}, Min={row[4]:.2f}, Max={row[5]:.2f}")

        # Check by recommendation for BASE
        print("\n--- Stats By Recommendation for BASE (14d) ---")
        cur.execute("""
            SELECT recommendation, COUNT(*), 
            SUM(CASE WHEN is_win THEN 1 ELSE 0 END),
            AVG(effective_return), 
            MIN(effective_return), MAX(effective_return)
            FROM pick_outcomes
            WHERE chain='base' AND picked_at_utc >= NOW() - INTERVAL '14 days'
            GROUP BY recommendation
        """)
        for row in cur.fetchall():
            print(f"Rec {row[0]}: Total={row[1]}, Wins={row[2]}, Avg={row[3]:.2f}, Min={row[4]:.2f}, Max={row[5]:.2f}")
