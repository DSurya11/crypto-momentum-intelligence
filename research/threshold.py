import os
import psycopg
import numpy as np
from dotenv import load_dotenv

load_dotenv()

# Get credentials from environment variables
host = os.getenv("PGHOST", "localhost")
port = int(os.getenv("PGPORT", "5432"))
dbname = os.getenv("PGDATABASE", "crypto_momentum")
user = os.getenv("PGUSER", "postgres")
password = os.getenv("PGPASSWORD")

if not password:
    raise ValueError("PGPASSWORD environment variable is required but not set")

conn = psycopg.connect(
    host=host,
    port=port,
    dbname=dbname,
    user=user,
    password=password
)

cur = conn.cursor()

cur.execute("""
SELECT model_score, return_2h
FROM pick_outcomes
WHERE model_score IS NOT NULL
""")

rows = cur.fetchall()

scores = np.array([r[0] for r in rows])
returns = np.array([r[1] for r in rows])

best=(0,0)

for t in np.arange(0.1,0.9,0.01):
    mask = scores>=t
    if mask.sum()<20:
        continue
    avg_return = returns[mask].mean()

    if avg_return>best[1]:
        best=(t,avg_return)

print("Best strong_buy threshold:",round(best[0],3))
print("Avg return at threshold:",round(best[1],4))