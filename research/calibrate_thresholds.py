import os
import json
import psycopg
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    password = os.getenv("PGPASSWORD")
    if not password:
        raise ValueError("PGPASSWORD environment variable is required but not set")
    
    return psycopg.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "crypto_momentum"),
        user=os.getenv("PGUSER", "postgres"),
        password=password,
        sslmode=os.getenv("PGSSLMODE", "disable"),
    )

def calibrate():
    conn = get_conn()
    # Correct column is picked_at_utc
    two_weeks_ago = datetime.now(timezone.utc) - timedelta(days=14)
    
    query = """
    SELECT 
        chain,
        model_score,
        CASE WHEN effective_return > 0 THEN 1 ELSE 0 END as is_win
    FROM pick_outcomes
    WHERE picked_at_utc >= %s
    ORDER BY chain, model_score DESC
    """
    
    with conn:
        with conn.cursor() as cur:
            cur.execute(query, (two_weeks_ago,))
            rows = cur.fetchall()
            print(f"Fetched {len(rows)} rows for calibration.")
    
    data_by_chain = {}
    for chain, score, is_win in rows:
        c = str(chain).lower()
        if c not in data_by_chain:
            data_by_chain[c] = []
        
        s = float(score)
        if s > 1.1: # Likely 0-100 scale
            s = s / 100.0
        data_by_chain[c].append({'score': s, 'win': is_win})
    
    new_thresholds = {
        "global": {
            "strong_buy": 0.35,
            "buy": 0.27,
            "neutral": 0.2,
            "calibrated": False,
            "sample_size": len(rows),
            "calibrated_at": datetime.now(timezone.utc).isoformat()
        },
        "perChain": {}
    }
    
    for chain, picks in data_by_chain.items():
        total_picks = len(picks)
        best_threshold = 0.35 
        found = False
        
        running_wins = 0
        for i, pick in enumerate(picks):
            running_wins += pick['win']
            count = i + 1
            win_rate = running_wins / count
            
            if count >= 30 and win_rate >= 0.55:
                best_threshold = pick['score']
                found = True
        
        new_thresholds["perChain"][chain] = {
            "strong_buy": round(best_threshold, 4),
            "buy": round(best_threshold * 0.8, 4),
            "neutral": round(best_threshold * 0.6, 4),
            "calibrated": found,
            "sample_size": total_picks,
            "calibrated_at": datetime.now(timezone.utc).isoformat()
        }
        print(f"Chain {chain.upper()}: Threshold={best_threshold:.4f}, Samples={total_picks}, TargetReached={found}")

    # Fallback to existing
    try:
        with open('research/score_thresholds.json', 'r') as f:
            old_data = json.load(f)
            for ch, config in old_data.get('perChain', {}).items():
                if ch not in new_thresholds["perChain"]:
                    new_thresholds["perChain"][ch] = config
    except:
        pass

    with open('research/score_thresholds.json', 'w') as f:
        json.dump(new_thresholds, f, indent=2)
    print("Updated research/score_thresholds.json")

if __name__ == "__main__":
    calibrate()
