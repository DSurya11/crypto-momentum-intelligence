"""Full pipeline test — logs to file to avoid terminal truncation."""
import sys, os, json
sys.path.insert(0, 'd:/crypto-momentum-intelligence')
os.environ.pop('APIFY_API_TOKEN', None)  # Force re-read from .env
from dotenv import load_dotenv
load_dotenv('d:/crypto-momentum-intelligence/.env', override=True)

LOG = open('d:/crypto-momentum-intelligence/pipeline_log.txt', 'w', encoding='utf-8')
def log(msg):
    print(msg)
    LOG.write(msg + '\n')
    LOG.flush()

from backend.meme_radar import run_meme_radar

token = os.getenv('APIFY_API_TOKEN', '')
log(f"Token ends: ...{token[-8:]}")

try:
    result = run_meme_radar(limit_reddit=3, limit_x=5)
    log(f"Total Scanned: {result['totalScanned']}")
    log(f"Total Viable:  {result['totalViable']}")
    log(f"Memes Searched: {result.get('memesSearched', '?')}")
    log(f"Matches: {len(result['results'])}")
    
    if result['results']:
        for i, r in enumerate(result['results'][:5]):
            log(f"MATCH #{i+1}: {r['title'][:60]}...")
            log(f"  source={r['source']}, virality={r['viralityScore']}")
            coins = [c['symbol'] for c in r['relatedCoins']]
            log(f"  coins: {coins}")
    else:
        log("NO MATCHES (CoinStats couldn't match keywords to real tokens)")
except Exception as e:
    log(f"ERROR: {e}")
    import traceback
    log(traceback.format_exc())

LOG.close()
print("Log saved to pipeline_log.txt")
