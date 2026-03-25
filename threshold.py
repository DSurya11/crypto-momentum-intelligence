import pandas as pd
import numpy as np
from sklearn.metrics import f1_score

df = pd.read_csv("research/live_picks_snapshot.csv")

y = df["target"].values
p = df["score"].values

def cls(x,a,b,c):
    if x>=a: return "strong_buy"
    if x>=b: return "buy"
    if x<=c: return "sell"
    return "neutral"

best=(0,0,0,0)

for a in np.arange(0.6,0.95,0.02):
    for b in np.arange(0.4,a,0.02):
        for c in np.arange(0.05,0.4,0.02):
            yp=[cls(x,a,b,c) for x in p]
            s=f1_score(y,yp,average="macro")
            if s>best[3]:
                best=(a,b,c,s)

print("strong_buy >=",round(best[0],3))
print("buy >=",round(best[1],3))
print("sell <=",round(best[2],3))
print("best F1 =",round(best[3],4))