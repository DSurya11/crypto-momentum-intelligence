import psycopg
import numpy as np

conn = psycopg.connect(
    host="localhost",
    port=5432,
    dbname="crypto_momentum",
    user="postgres",
    password="hello123"
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