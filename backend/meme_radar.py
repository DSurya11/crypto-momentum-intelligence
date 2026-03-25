"""
Meme Radar — detect trending memes and find related crypto tokens early.

Data sources:
  - Reddit public JSON API (no auth needed): r/memes, r/dankmemes,
    r/CryptoCurrency, r/wallstreetbets, r/MemeEconomy, r/CryptoMoonShots
  - X/Twitter API v2 (requires X_BEARER_TOKEN): trending crypto meme tweets
  - CoinStats search API (requires COINSTATS_API_KEY): find tokens that
    exactly match meme names/keywords.

Logic:
  - Only memes that have an EXACT coin match (name or symbol) are shown.
  - Meme + coin are displayed together as a combined opportunity.

Scoring:
  - Virality score (0–100): based on upvote velocity, comment engagement,
    award count, cross-sub presence, and post freshness.
  - Growth prediction: rising vs. peaked indicator using momentum of
    engagement over post age.
"""

from __future__ import annotations

import json
import math
import os
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MEME_SUBREDDITS = [
    "CryptoCurrency",
    "CryptoMoonShots",
    "SatoshiStreetBets",
    "memes",
    "wallstreetbets",
]

# X/Twitter search queries — crypto-meme-focused
X_SEARCH_QUERIES = [
    "crypto meme coin -is:retweet has:media lang:en",
    "memecoin OR meme coin -is:retweet lang:en",
    "$PEPE OR $DOGE OR $SHIB OR $BONK OR $WIF meme -is:retweet lang:en",
    "new meme token launch -is:retweet lang:en",
]

# Keywords to skip — common English words that produce garbage coin matches.
# Must be comprehensive: any generic word will match random coins on CoinStats.
SKIP_KEYWORDS = frozenset({
    # Articles, pronouns, prepositions, conjunctions
    "the", "a", "an", "is", "it", "to", "and", "of", "in", "for", "on",
    "my", "me", "we", "you", "he", "she", "they", "them", "us", "our",
    "his", "her", "your", "their", "its", "i", "am",
    "this", "that", "these", "those", "with", "from", "at", "by", "as",
    "or", "if", "so", "no", "yes", "up", "down", "out", "off", "over",
    "into", "onto", "upon", "after", "before", "between", "through",
    "during", "without", "within", "along", "against", "about", "above",
    "below", "under", "around", "near", "than",
    # Common verbs
    "be", "have", "has", "had", "do", "did", "does", "done", "will",
    "would", "could", "should", "shall", "may", "might", "must",
    "can", "need", "want", "let", "say", "said", "tell", "told",
    "get", "got", "gets", "give", "gave", "go", "went", "gone", "going",
    "come", "came", "take", "took", "taken", "make", "made",
    "put", "set", "run", "see", "saw", "seen", "look", "looked",
    "find", "found", "think", "thought", "know", "knew", "known",
    "feel", "felt", "seem", "try", "tried", "use", "used",
    "keep", "kept", "start", "stop", "show", "showed", "shown",
    "turn", "move", "live", "play", "work", "read", "pay", "paid",
    "stand", "lose", "lost", "hold", "bring", "brought", "happen",
    "write", "sit", "die", "send", "fall", "fell", "stay", "leave",
    "left", "call", "called", "ask", "asked", "win", "won", "pick",
    "become", "hit", "cut", "reach", "build", "break", "spend",
    "open", "close", "watch", "wait", "follow", "carry", "walk",
    "save", "talk", "eat", "ate", "wear", "pull", "push", "catch",
    "fight", "throw", "miss", "pass", "rise", "raise", "remember",
    "love", "hate", "drop", "check", "help",
    # Adjectives / adverbs / determiners
    "not", "but", "just", "also", "very", "too", "really", "only",
    "even", "still", "already", "back", "much", "more", "most",
    "less", "least", "many", "few", "some", "any", "such",
    "each", "every", "both", "other", "another", "own", "same",
    "first", "last", "next", "new", "old", "big", "small", "great",
    "good", "bad", "best", "worst", "better", "long", "short",
    "high", "low", "right", "wrong", "real", "true", "false",
    "hard", "easy", "fast", "slow", "full", "half", "whole", "sure",
    "free", "early", "late", "young", "little", "pretty", "nice",
    "well", "here", "there", "where", "when", "how", "why", "what",
    "which", "who", "whom", "whose", "never", "always", "sometimes",
    "often", "again", "away", "far", "together", "maybe", "please",
    "though", "enough", "yet", "else", "almost", "quite",
    # Nouns — common generic words
    "are", "was", "were", "been", "being",
    "all", "like", "one", "two", "three", "four", "five", "ten",
    "now", "today", "time", "day", "week", "month", "year", "thing",
    "way", "part", "place", "case", "point", "world", "home", "hand",
    "life", "end", "head", "side", "fact", "line", "face", "eye", "body",
    "man", "men", "woman", "women", "child", "kid", "kids", "girl",
    "boy", "guy", "guys", "friend", "friends", "family", "father",
    "mother", "brother", "sister", "baby", "son", "wife", "husband",
    "car", "water", "food", "house", "door", "room", "job", "game",
    "team", "school", "company", "group", "lot", "kind", "name",
    "number", "story", "question", "answer", "idea", "word", "words",
    "problem", "reason", "state", "country", "city", "book", "war",
    "god", "law", "power", "history", "class", "student", "system",
    "level", "order", "plan", "result", "area", "form", "change",
    "force", "light", "office", "night", "morning", "top", "air",
    "issue", "land", "age", "base", "fire", "blood", "bloody",
    "death", "dead", "human", "heart", "mind", "picture", "phone",
    "post", "media", "video", "photo", "image", "news", "type",
    "stuff", "shit", "fuck", "damn", "hell", "lol", "lmao", "bruh",
    "bro", "dude", "fam", "based", "cringe", "sus", "vibe", "mood",
    "funny", "meme", "memes", "reddit", "sub", "subreddit",
    "people", "person", "everyone", "somebody", "nobody", "anybody",
    "everything", "nothing", "anything", "something",
    # Crypto-specific generic words (keep actual full coin names OUT of this list
    # so "bitcoin", "ethereum", "solana" etc. get searched by full name in CoinStats)
    "crypto", "coin", "token",
    "btc", "eth", "sol", "bnb", "xrp", "ada", "dot", "doge", "ltc",  # short tickers — skip, search by full name instead
    "buy", "sell", "price", "market", "trading", "trade", "trades",
    "money", "investment", "invest", "investor", "stock", "stocks",
    "share", "shares", "profit", "loss", "gains", "gain", "pump",
    "dump", "moon", "hodl", "bear", "bull", "bullish", "bearish",
    "wallet", "exchange", "mining", "miner", "blockchain", "defi",
    "nft", "nfts", "dao", "yield", "stake", "staking", "airdrop",
    "chart", "candle", "volume", "cap", "supply", "burn", "mint",
    "rug", "scam", "whale", "whales", "dip", "ath", "fomo", "fud",
    "diamond", "hands", "ape", "apes", "rocket", "lambo",
    "tendies", "yolo", "wsb", "gme", "squeeze", "short", "retard",
    "autist", "puts", "calls", "options", "futures", "leverage",
})

# User agent for Reddit requests
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# CoinStats API
COINSTATS_SEARCH_URL = "https://openapiv1.coinstats.app/coins"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class MemePost:
    """A single meme post from Reddit or X/Twitter."""
    title: str
    subreddit: str  # subreddit name for Reddit, query tag for X
    url: str
    permalink: str
    score: int  # upvotes (Reddit) or likes (X)
    num_comments: int  # comments (Reddit) or replies (X)
    created_utc: float
    thumbnail: str
    author: str
    upvote_ratio: float
    source: str = "reddit"  # "reddit" or "x"
    # Derived
    age_hours: float = 0.0
    upvote_velocity: float = 0.0  # upvotes per hour
    comment_ratio: float = 0.0  # comments / upvotes
    virality_score: float = 0.0  # 0-100
    growth_phase: str = "unknown"  # "rising", "peaking", "declining"
    keywords: list[str] = field(default_factory=list)


@dataclass
class RelatedCoin:
    """A crypto coin found via CoinStats that matches a meme."""
    coin_id: str
    symbol: str
    name: str
    rank: int | None = None
    icon: str = ""
    price: float | None = None
    price_change_24h: float | None = None
    market_cap: float | None = None
    volume_24h: float | None = None
    match_keyword: str = ""
    match_type: str = ""  # "exact_name", "exact_symbol", "contains"


@dataclass
class MemeRadarResult:
    """Complete result for one meme trend."""
    meme: MemePost
    related_coins: list[RelatedCoin]
    cross_sub_count: int = 0  # how many subreddits mention this


# ---------------------------------------------------------------------------
# Reddit fetching
# ---------------------------------------------------------------------------

_reddit_token: str | None = None
_reddit_token_expires_at: float = 0

def _get_reddit_bearer_token() -> str | None:
    """Fetch an OAuth bearer token using Reddit App credentials from .env."""
    global _reddit_token, _reddit_token_expires_at
    now = time.time()
    if _reddit_token and now < _reddit_token_expires_at:
        return _reddit_token

    client_id = os.getenv("REDDIT_CLIENT_ID", "").strip()
    client_secret = os.getenv("REDDIT_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        return None

    import base64
    auth_str = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")
    data = urllib.parse.urlencode({"grant_type": "client_credentials"}).encode("utf-8")
    req = urllib.request.Request(
        "https://www.reddit.com/api/v1/access_token",
        data=data,
        headers={"Authorization": f"Basic {auth_str}", "User-Agent": USER_AGENT}
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            _reddit_token = payload.get("access_token")
            # Usually expires in 86400 secs, keep it cached for 1 hour to heavily buffer
            _reddit_token_expires_at = now + 3600 
            return _reddit_token
    except Exception as e:
        print(f"[MEME RADAR] Failed to get Reddit OAuth token: {e}")
        return None


def _fetch_json(url: str, headers: dict | None = None) -> Any:
    """Fetch JSON from URL with throttle protection and OAuth support."""
    hdrs = {"User-Agent": USER_AGENT}
    
    # If using Reddit OAuth, append Bearer token
    if "reddit.com" in url:
        token = _get_reddit_bearer_token()
        if token:
            hdrs["Authorization"] = f"Bearer {token}"
            # OAuth must use oauth.reddit.com
            url = url.replace("https://www.reddit.com", "https://oauth.reddit.com")
            
    if headers:
        hdrs.update(headers)
        
    req = urllib.request.Request(url, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"[MEME RADAR] Failed to fetch {url}: {e}")
        return None


def fetch_subreddit_hot(subreddit: str, limit: int = 25) -> list[MemePost]:
    """Fetch hot posts from a subreddit using Reddit's public JSON API or OAuth."""
    url = f"https://www.reddit.com/r/{subreddit}/hot.json?limit={limit}&raw_json=1"
    data = _fetch_json(url)
    if not data or "data" not in data:
        return []

    now = time.time()
    posts: list[MemePost] = []
    for child in data["data"].get("children", []):
        d = child.get("data", {})
        if d.get("stickied") or d.get("is_self") and not d.get("selftext"):
            continue

        created = float(d.get("created_utc", now))
        age_hours = max((now - created) / 3600.0, 0.01)
        score = int(d.get("score", 0))
        comments = int(d.get("num_comments", 0))

        post = MemePost(
            title=d.get("title", ""),
            subreddit=subreddit,
            url=d.get("url", ""),
            permalink=f"https://reddit.com{d.get('permalink', '')}",
            score=score,
            num_comments=comments,
            created_utc=created,
            thumbnail=d.get("thumbnail", ""),
            author=d.get("author", ""),
            upvote_ratio=float(d.get("upvote_ratio", 0.5)),
            age_hours=age_hours,
            upvote_velocity=score / age_hours if age_hours > 0 else 0,
            comment_ratio=comments / max(score, 1),
        )
        post.keywords = extract_keywords(post.title)
        posts.append(post)

    return posts


def fetch_all_subreddits(limit_per_sub: int = 20) -> list[MemePost]:
    """Fetch live Reddit posts using Apify (trudax/reddit-scraper-lite).
    
    The trudax scraper returns fields:
      title, upVotes, numberOfComments, parsedCommunityName,
      username, createdAt, url, upVoteRatio, thumbnailUrl, body
    """
    apify = os.getenv("APIFY_API_TOKEN", "").strip()
    if not apify:
        print("[MEME RADAR] No APIFY_API_TOKEN found, skipping Reddit fetch.")
        return []

    print("[MEME RADAR] Using Apify to scan Reddit...")
    url = f"https://api.apify.com/v2/acts/trudax~reddit-scraper-lite/run-sync-get-dataset-items?token={apify}"
    
    start_urls = [{"url": f"https://www.reddit.com/r/{sub}/hot/"} for sub in MEME_SUBREDDITS]
    
    payload = json.dumps({
        "startUrls": start_urls,
        "maxItems": limit_per_sub * len(MEME_SUBREDDITS),
        "skipComments": True
    }).encode("utf-8")
    
    try:
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as err:
        print(f"[MEME RADAR] Apify Reddit search failed: {err}")
        return []

    print(f"[MEME RADAR] Reddit raw items: {len(data)}")
    now = time.time()
    posts: list[MemePost] = []
    
    for item in data:
        title = item.get("title", "")
        if not title:
            continue
        
        # trudax field names: upVotes, numberOfComments, parsedCommunityName
        score = int(item.get("upVotes", 0))
        comments = int(item.get("numberOfComments", 0))
        subreddit = item.get("parsedCommunityName", item.get("communityName", "reddit"))
        
        # Parse timestamp  (ISO format: "2026-03-25T01:00:17.000Z")
        created_str = item.get("createdAt", "")
        created_utc = now
        try:
            if created_str:
                created_dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
                created_utc = created_dt.timestamp()
        except Exception:
            pass
            
        age_hours = max((now - created_utc) / 3600.0, 0.01)
        thumb = item.get("thumbnailUrl", "")
        
        # Also extract keywords from 'body' text if available (for richer matching)
        body_text = item.get("body", "")
        combined_text = title
        if body_text and len(body_text) < 2000:
            combined_text = f"{title} {body_text}"
        
        post = MemePost(
            title=title,
            subreddit=subreddit,
            url=item.get("url", ""),
            permalink=item.get("url", ""),
            score=score,
            num_comments=comments,
            created_utc=created_utc,
            thumbnail=thumb if thumb.startswith("http") else "",
            author=item.get("username", "user"),
            upvote_ratio=float(item.get("upVoteRatio", 0.9)),
            source="reddit",
            age_hours=age_hours,
            upvote_velocity=score / age_hours if age_hours > 0 else 0,
            comment_ratio=comments / max(score, 1)
        )
        post.keywords = extract_keywords(combined_text)
        posts.append(post)
    
    print(f"[MEME RADAR] Reddit posts parsed: {len(posts)}")
    return posts

# ---------------------------------------------------------------------------
# X/Twitter fetching  (using danek/twitter-scraper-ppr — $0.30/1K results)
# ---------------------------------------------------------------------------

def _x_bearer_token() -> str:
    """Return X API bearer token from env."""
    return os.getenv("X_BEARER_TOKEN", "").strip()

def _apify_api_token() -> str:
    """Return Apify API token from env."""
    return os.getenv("APIFY_API_TOKEN", "").strip()


def fetch_all_x(max_per_query: int = 15) -> list[MemePost]:
    """Fetch live Twitter/X posts using Apify (kaitoeasyapi PPR — $0.25/1K tweets).
    
    Uses keyword search mode via 'twitterContent'. Output fields:
      text, likeCount, retweetCount, replyCount, viewCount,
      createdAt (Twitter native format), author.userName, url
    """
    apify = _apify_api_token()
    if not apify:
        print("[MEME RADAR] No APIFY_API_TOKEN found, skipping X fetch.")
        return []
        
    print("[MEME RADAR] Using Apify (kaitoeasyapi PPR) to scan X (Twitter)...")
    url = f"https://api.apify.com/v2/acts/kaitoeasyapi~twitter-x-data-tweet-scraper-pay-per-result-cheapest/run-sync-get-dataset-items?token={apify}"
    
    # Combine search queries with OR for a single search
    combined_search = " OR ".join(X_SEARCH_QUERIES[:5])
    
    payload = json.dumps({
        "twitterContent": combined_search,
        "maxItems": max_per_query,
    }).encode("utf-8")
    
    try:
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=90) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as err:
        print(f"[MEME RADAR] Apify X search failed: {err}")
        return []
    
    print(f"[MEME RADAR] X raw items: {len(data)}")
    now = time.time()
    posts: list[MemePost] = []
    
    for tweet in data:
        # Filter out {"noResults": true} placeholder objects
        if tweet.get("noResults"):
            continue
            
        text = tweet.get("text", tweet.get("full_text", ""))
        if not text:
            continue
        
        # danek scraper fields vary; handle both possible schemas
        likes = int(tweet.get("likeCount", tweet.get("favorite_count", 0)))
        retweets = int(tweet.get("retweetCount", tweet.get("retweet_count", 0)))
        replies = int(tweet.get("replyCount", tweet.get("reply_count", 0)))
        engagement = likes + retweets
            
        created_str = tweet.get("createdAt", tweet.get("created_at", ""))
        created_utc = now
        try:
            if created_str:
                # Try ISO format first
                created_dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
                created_utc = created_dt.timestamp()
        except Exception:
            try:
                # Try Twitter's native format
                created_dt = datetime.strptime(created_str, "%a %b %d %H:%M:%S %z %Y")
                created_utc = created_dt.timestamp()
            except Exception:
                pass
            
        age_hours = max((now - created_utc) / 3600.0, 0.01)
        
        # Author info may be nested or flat
        author_info = tweet.get("author", {})
        if isinstance(author_info, dict):
            username = author_info.get("userName", author_info.get("screen_name", "crypto_user"))
        else:
            username = tweet.get("user_name", tweet.get("screen_name", "crypto_user"))
        
        post = MemePost(
            title=text,
            subreddit="X Trending",
            url=tweet.get("url", tweet.get("tweet_url", "")),
            permalink=tweet.get("url", tweet.get("tweet_url", "")),
            score=engagement,
            num_comments=replies,
            created_utc=created_utc,
            thumbnail="",
            author=username,
            upvote_ratio=0.9,
            source="x",
            age_hours=age_hours,
            upvote_velocity=engagement / age_hours if age_hours > 0 else 0,
            comment_ratio=replies / max(engagement, 1),
        )
        post.keywords = extract_keywords(text)
        posts.append(post)
        
    return posts


# ---------------------------------------------------------------------------
# Keyword extraction
# ---------------------------------------------------------------------------

def _clean_title_words(title: str) -> list[str]:
    """Return lowercase cleaned words from a title (no stop-word filtering)."""
    text = re.sub(r"https?://\S+", "", title)
    text = re.sub(r"[^a-zA-Z0-9\s$#]", " ", text)
    return [w.strip("#$") for w in text.lower().split() if w.strip("#$")]


def extract_keywords(title: str) -> list[str]:
    """Extract search terms from a meme title.

    Generates:
      - Single words (≥3 chars, not in SKIP_KEYWORDS)
      - $TICKER and #hashtag patterns
      - Multi-word phrases (2-word, 3-word combos) from consecutive words
      - Full cleaned title as one phrase
    All returned as lowercase strings, deduplicated, phrases first.
    """
    raw_words = _clean_title_words(title)

    # --- Single keywords (filtered) ---
    singles: list[str] = []
    for w in raw_words:
        if len(w) < 3 or w in SKIP_KEYWORDS or w.isdigit():
            continue
        if w not in singles:
            singles.append(w)

    # --- $TICKER and #hashtag patterns ---
    tickers = re.findall(r"\$([A-Za-z]{2,10})", title)
    hashtags = re.findall(r"#([A-Za-z]{2,20})", title)
    for t in tickers + hashtags:
        t_lower = t.lower()
        if t_lower not in singles and t_lower not in SKIP_KEYWORDS:
            singles.append(t_lower)

    # --- Multi-word phrases (from ALL words, including stop words) ---
    # This catches coins named like "every bloody time", "baby doge", etc.
    phrases: list[str] = []
    seen: set[str] = set()

    # Full title (cleaned, joined)
    full = " ".join(raw_words)
    if len(raw_words) >= 2 and full not in seen:
        seen.add(full)
        phrases.append(full)

    # 3-word combos
    for i in range(len(raw_words) - 2):
        phrase = " ".join(raw_words[i : i + 3])
        if phrase not in seen:
            seen.add(phrase)
            phrases.append(phrase)

    # 2-word combos
    for i in range(len(raw_words) - 1):
        phrase = " ".join(raw_words[i : i + 2])
        if phrase not in seen:
            seen.add(phrase)
            phrases.append(phrase)

    # Return phrases first (more specific), then singles
    # Deduplicate across both lists
    result: list[str] = []
    result_set: set[str] = set()
    for term in phrases + singles:
        if term not in result_set:
            result_set.add(term)
            result.append(term)

    return result


# ---------------------------------------------------------------------------
# Virality scoring
# ---------------------------------------------------------------------------

def score_virality(post: MemePost) -> float:
    """
    Score a meme post 0-100 based on:
      - Upvote velocity (upvotes/hour) — 40% weight
      - Comment engagement ratio — 20% weight
      - Freshness (newer = better) — 20% weight
      - Upvote ratio (controversial = interesting) — 10% weight
      - Raw score magnitude — 10% weight
    """
    # Upvote velocity: log scale, cap at ~5000/hr
    velocity_score = min(math.log1p(post.upvote_velocity) / math.log1p(5000) * 100, 100)

    # Comment engagement: higher = more discussion
    comment_score = min(post.comment_ratio * 200, 100)

    # Freshness: exponential decay, half-life of 6 hours
    freshness_score = 100 * math.exp(-0.115 * post.age_hours)

    # Upvote ratio closeness to 1.0 (very high upvote ratio = universally liked)
    upvote_score = post.upvote_ratio * 100

    # Raw magnitude: log scale
    magnitude_score = min(math.log1p(post.score) / math.log1p(100000) * 100, 100)

    virality = (
        velocity_score * 0.40
        + comment_score * 0.20
        + freshness_score * 0.20
        + upvote_score * 0.10
        + magnitude_score * 0.10
    )

    return round(min(max(virality, 0), 100), 1)


def classify_growth_phase(post: MemePost) -> str:
    """Classify meme as rising, peaking, or declining."""
    # Very fresh with strong velocity = rising
    if post.age_hours < 4 and post.upvote_velocity > 100:
        return "rising"
    if post.age_hours < 8 and post.upvote_velocity > 50:
        return "rising"
    # Moderate age with high score = peaking
    if 4 <= post.age_hours <= 12 and post.score > 1000:
        return "peaking"
    # Old or low velocity = declining
    if post.age_hours > 12 or post.upvote_velocity < 10:
        return "declining"
    return "rising"


# ---------------------------------------------------------------------------
# CoinStats search
# ---------------------------------------------------------------------------

def _coinstats_headers() -> dict[str, str]:
    """Return CoinStats API headers."""
    api_key = os.getenv("COINSTATS_API_KEY", "").strip()
    return {
        "X-API-KEY": api_key,
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }


def search_coins_coinstats(keyword: str) -> list[RelatedCoin]:
    """Search CoinStats for coins matching a keyword.

    CoinStats /coins endpoint supports a 'name' query param for searching.
    We search by keyword and then check for exact matches.
    """
    api_key = os.getenv("COINSTATS_API_KEY", "").strip()
    if not api_key:
        return []

    query_params = {
        "name": keyword,
        "limit": "20",
    }
    url = f"{COINSTATS_SEARCH_URL}?{urllib.parse.urlencode(query_params)}"

    try:
        req = urllib.request.Request(url, headers=_coinstats_headers())
        with urllib.request.urlopen(req, timeout=5) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception as err:
        print(f"CoinStats search failed for '{keyword}': {err}")
        return []

    result_list = payload.get("result") or []
    coins: list[RelatedCoin] = []
    kw_lower = keyword.lower()

    for c in result_list:
        name_lower = (c.get("name") or "").lower()
        sym_lower = (c.get("symbol") or "").lower()

        if kw_lower == sym_lower:
            match_type = "exact_symbol"
        elif kw_lower == name_lower:
            match_type = "exact_name"
        elif " " in kw_lower and (
            # "baby doge" matches "Baby Doge Coin"  (coin name starts with keyword)
            name_lower.startswith(kw_lower)
            # "baby doge coin" matches "Baby Doge"  (keyword starts with coin name)
            or (" " in name_lower and kw_lower.startswith(name_lower))
        ):
            match_type = "prefix_name"
        else:
            # Skip everything else
            continue

        coins.append(RelatedCoin(
            coin_id=c.get("id", ""),
            symbol=c.get("symbol", ""),
            name=c.get("name", ""),
            rank=c.get("rank"),
            icon=c.get("icon", ""),
            price=c.get("price"),
            price_change_24h=c.get("priceChange1d"),
            market_cap=c.get("marketCap"),
            volume_24h=c.get("volume"),
            match_keyword=keyword,
            match_type=match_type,
        ))

    return coins


def find_exact_coin_matches(keywords: list[str], max_searches: int = 10) -> list[RelatedCoin]:
    """Search CoinStats for coins that exactly match meme keywords.

    Keywords can be single words OR multi-word phrases.
    Only returns coins where the name or symbol is an exact match.
    """
    all_coins: list[RelatedCoin] = []
    seen_ids: set[str] = set()
    searched: set[str] = set()
    api_calls = 0

    for kw in keywords[:max_searches * 2]:  # iterate more but cap API calls
        if api_calls >= max_searches:
            break
        if len(kw) < 2 or kw in searched:
            continue
        # Skip phrases where every word is a stop word
        phrase_words = kw.split()
        if all(w in SKIP_KEYWORDS or len(w) < 2 for w in phrase_words):
            continue
        searched.add(kw)

        api_calls += 1
        results = search_coins_coinstats(kw)
        for coin in results:
            if coin.coin_id not in seen_ids:
                seen_ids.add(coin.coin_id)
                all_coins.append(coin)
        # small delay only when not the last call
        if api_calls < max_searches:
            time.sleep(0.3)

    # Sort: exact_symbol first, exact_name second, prefix_name third, then by rank
    def sort_key(c: RelatedCoin) -> tuple:
        type_order = 0 if c.match_type == "exact_symbol" else (1 if c.match_type == "exact_name" else 2)
        return (type_order, c.rank or 999999)

    return sorted(all_coins, key=sort_key)


# ---------------------------------------------------------------------------
# Main radar pipeline
# ---------------------------------------------------------------------------

def run_meme_radar(
    limit_per_sub: int = 15,
    limit_reddit: int | None = None,
    limit_x: int | None = None,
    min_virality: float = 0.0,
    max_results: int = 30,
    search_coins: bool = True,
) -> dict[str, Any]:
    """
    Run the full meme radar pipeline:
      1. Fetch memes from all subreddits
      2. Score virality
      3. Deduplicate by keyword clusters
      4. Search CoinStats for exact coin matches per meme
      5. ONLY keep memes that have at least one exact coin match
      6. Return combined meme+coin results
    """
    # Support both old-style (limit_per_sub) and new-style (limit_reddit/limit_x) params
    reddit_limit = limit_reddit if limit_reddit is not None else limit_per_sub
    x_limit = limit_x if limit_x is not None else 10
    
    # 1. Fetch all posts from Reddit + X
    all_posts = fetch_all_subreddits(limit_per_sub=reddit_limit)
    x_posts = fetch_all_x(max_per_query=x_limit)
    all_posts.extend(x_posts)

    # 2. Score and classify
    for post in all_posts:
        post.virality_score = score_virality(post)
        post.growth_phase = classify_growth_phase(post)

    # 3. Filter by minimum virality
    viable = [p for p in all_posts if p.virality_score >= min_virality]
    viable.sort(key=lambda p: p.virality_score, reverse=True)

    # 4. Cluster by keyword overlap and deduplicate
    seen_keyword_sets: list[set[str]] = []
    deduplicated: list[MemePost] = []
    for post in viable:
        kw_set = set(post.keywords)
        if not kw_set:
            continue
        # Check overlap with already-seen clusters
        is_dup = False
        for existing in seen_keyword_sets:
            overlap = len(kw_set & existing) / max(len(kw_set | existing), 1)
            if overlap > 0.5:
                is_dup = True
                break
        if not is_dup:
            seen_keyword_sets.append(kw_set)
            deduplicated.append(post)

    # 5. Count cross-subreddit/source presence per keyword cluster
    cross_sub_map: dict[str, set[str]] = {}
    for post in all_posts:
        source_label = f"x:{post.subreddit}" if post.source == "x" else post.subreddit
        for kw in post.keywords:
            cross_sub_map.setdefault(kw, set()).add(source_label)

    # 6. Search CoinStats for exact matches — only keep memes WITH a match.
    # Serial loop with sleep between calls to avoid 429 rate-limiting.
    results: list[dict[str, Any]] = []
    total_searched = 0

    # Cap at max_results (not a multiple) — avoids spawning 60+ API calls
    candidates = deduplicated[:max_results]

    def _coin_candidates(keywords: list[str], n: int = 5) -> list[str]:
        """Pick the best keywords for CoinStats exact-match search.

        extract_keywords() returns long phrases first, but CoinStats matching
        works best on single-word names and short multi-word coin names.
        Order: singles (in title order — so "bitcoin" before "breaks") →
               2-word phrases → 3-word phrases.
        Long phrases (4+ words) are excluded — too unlikely to equal a coin name.
        """
        singles = [k for k in keywords if " " not in k]          # preserve title order
        two_word = [k for k in keywords if k.count(" ") == 1]
        three_word = [k for k in keywords if k.count(" ") == 2]
        return (singles + two_word + three_word)[:n]

    coin_results: dict[int, list] = {}
    if search_coins:
        for i, post in enumerate(candidates):
            search_kw = _coin_candidates(post.keywords, n=5)
            try:
                coins = find_exact_coin_matches(search_kw, max_searches=5)
            except Exception:
                coins = []
            coin_results[i] = coins
            if i < len(candidates) - 1:
                time.sleep(0.4)  # throttle between posts (~0.4 s × 20 posts ≈ 8 s overhead)
    else:
        coin_results = {i: [] for i in range(len(candidates))}

    for i, post in enumerate(candidates):
        if len(results) >= max_results:
            break

        cross_count = max(
            len(cross_sub_map.get(kw, set())) for kw in post.keywords
        ) if post.keywords else 0

        coins = coin_results.get(i, [])
        total_searched += 1
        matched_coins: list[dict[str, Any]] = []
        for coin in coins[:5]:
            matched_coins.append({
                "id": coin.coin_id,
                "symbol": coin.symbol,
                "name": coin.name,
                "rank": coin.rank,
                "icon": coin.icon,
                "price": coin.price,
                "priceChange24h": coin.price_change_24h,
                "marketCap": coin.market_cap,
                "volume24h": coin.volume_24h,
                "matchKeyword": coin.match_keyword,
                "matchType": coin.match_type,
            })

        # ONLY include meme if it has at least one exact coin match
        if not matched_coins:
            continue

        results.append({
            "title": post.title,
            "subreddit": post.subreddit,
            "source": post.source,
            "url": post.url,
            "permalink": post.permalink,
            "score": post.score,
            "numComments": post.num_comments,
            "ageHours": round(post.age_hours, 1),
            "upvoteVelocity": round(post.upvote_velocity, 1),
            "commentRatio": round(post.comment_ratio, 3),
            "viralityScore": post.virality_score,
            "growthPhase": post.growth_phase,
            "keywords": post.keywords[:8],
            "crossSubCount": cross_count,
            "relatedCoins": matched_coins,
            "thumbnail": post.thumbnail if post.thumbnail.startswith("http") else "",
        })

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "totalScanned": len(all_posts),
        "totalViable": len(viable),
        "memesSearched": total_searched,
        "results": results,
    }
