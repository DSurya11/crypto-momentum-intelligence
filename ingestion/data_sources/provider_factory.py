from __future__ import annotations

import os

from .alchemy_provider import AlchemyProvider
from .gecko_provider import GeckoProvider


def build_provider(name: str, network: str, gecko_base_url: str):
    lowered = name.strip().lower()
    if lowered == "gecko":
        return GeckoProvider(base_url=gecko_base_url, network=network)
    if lowered == "alchemy":
        api_key = os.getenv("ALCHEMY_API_KEY", "").strip()
        if not api_key:
            raise ValueError("ALCHEMY_API_KEY is required when provider is set to alchemy")
        return AlchemyProvider(api_key=api_key, network=network)
    raise ValueError(f"Unsupported provider: {name}")