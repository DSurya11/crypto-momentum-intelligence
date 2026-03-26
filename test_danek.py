import urllib.request, json, os, urllib.error
from dotenv import load_dotenv
load_dotenv('d:/crypto-momentum-intelligence/.env')
apify = os.getenv('APIFY_API_TOKEN')

# Test the danek/twitter-scraper-ppr with the correct "search" param
url = f'https://api.apify.com/v2/acts/danek~twitter-scraper-ppr/run-sync-get-dataset-items?token={apify}'
payload = json.dumps({"search": "crypto meme coin", "max_posts": 3}).encode('utf-8')

try:
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=90) as resp:
        data = json.loads(resp.read().decode('utf-8'))
        print(f"SUCCESS: {len(data)} tweets")
        if data:
            print("KEYS:", list(data[0].keys()))
            print("SAMPLE:", json.dumps(data[0], indent=2, default=str)[:800])
except urllib.error.HTTPError as e:
    err_body = e.read().decode('utf-8', errors='ignore')
    print(f"HTTP ERROR {e.code}: {err_body}")
except Exception as e:
    print(f"ERROR: {e}")
