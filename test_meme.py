import sys, os
sys.path.append('d:/crypto-momentum-intelligence')
from dotenv import load_dotenv
load_dotenv('d:/crypto-momentum-intelligence/.env')
from backend.meme_radar import run_meme_radar

print('Starting Manual Run...')
res = run_meme_radar()
print('\nDONE!')
print(f"Total Scanned: {res.get('scanned')}")
print(f"Total Searched (Viral): {res.get('searched')}")
print(f"Total Matches: {len(res.get('matches', []))}")
