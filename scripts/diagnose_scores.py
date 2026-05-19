import csv, os
from pathlib import Path
from collections import Counter
from dotenv import load_dotenv
import psycopg

load_dotenv(dotenv_path=Path('.env'))
SNAP=Path('research/live_picks_snapshot.csv')
if SNAP.exists():
    with SNAP.open('r', encoding='utf-8', newline='') as f:
        rows=list(csv.DictReader(f))
    # Count calibrated `score` and uncalibrated `raw_score` if present
    scores_cal=[]
    scores_raw=[]
    for r in rows:
        # calibrated
        s=r.get('score')
        try:
            s_val=float(s)
            if s_val<=1.0:
                s_val=s_val*100.0
        except Exception:
            s_val=0.0
        scores_cal.append(round(s_val,4))
        # raw
        sr=r.get('raw_score') or r.get('score')
        try:
            sr_val=float(sr)
            if sr_val<=1.0:
                sr_val=sr_val*100.0
        except Exception:
            sr_val=0.0
        scores_raw.append(round(sr_val,4))
    print('snapshot_rows=',len(rows))
    c_cal=Counter(scores_cal)
    c_raw=Counter(scores_raw)
    print('top 10 duplicates in snapshot (calibrated score,value,count):')
    for v,cnt in c_cal.most_common(10):
        print(v,cnt)
    print('unique_scores_snapshot_calibrated=',len(c_cal))
    print('\n')
    print('top 10 duplicates in snapshot (raw score,value,count):')
    for v,cnt in c_raw.most_common(10):
        print(v,cnt)
    print('unique_scores_snapshot_raw=',len(c_raw))
else:
    print('snapshot missing')

# DB check
try:
    conn=psycopg.connect(host=os.getenv('PGHOST','localhost'),port=int(os.getenv('PGPORT','5432')),dbname=os.getenv('PGDATABASE'),user=os.getenv('PGUSER'),password=os.getenv('PGPASSWORD'),sslmode=os.getenv('PGSSLMODE','disable'))
    with conn.cursor() as cur:
        cur.execute("SELECT model_score, raw_model_score FROM pick_outcomes WHERE model_score IS NOT NULL")
        rows=[(r[0], r[1]) for r in cur.fetchall()]
    vals_cal=[]
    vals_raw=[]
    for v_raw in rows:
        v,vraw = v_raw
        try:
            vv=float(v)
            if vv<=1.0:
                vv=vv*100.0
        except Exception:
            vv=0.0
        vals_cal.append(round(vv,4))
        try:
            vv2=float(vraw) if vraw is not None else float(v)
            if vv2<=1.0:
                vv2=vv2*100.0
        except Exception:
            vv2=0.0
        vals_raw.append(round(vv2,4))
    c2=Counter(vals_cal)
    c3=Counter(vals_raw)
    print('pick_outcomes_rows=',len(vals_cal))
    print('top 10 duplicates in pick_outcomes (calibrated,value,count):')
    for v,cnt in c2.most_common(10):
        print(v,cnt)
    print('unique_scores_pick_outcomes_calibrated=',len(c2))
    print('\n')
    print('top 10 duplicates in pick_outcomes (raw,value,count):')
    for v,cnt in c3.most_common(10):
        print(v,cnt)
    print('unique_scores_pick_outcomes_raw=',len(c3))
except Exception as e:
    print('DB error',e)
