import os
from functools import lru_cache
from anthropic import AsyncAnthropic

AGENT_MODEL = "claude-sonnet-4-6"
CHEAP_MODEL = "claude-haiku-4-5-20251001"


@lru_cache(maxsize=1)
def get_anthropic_client() -> AsyncAnthropic:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY env var not set")
    return AsyncAnthropic(api_key=api_key)
