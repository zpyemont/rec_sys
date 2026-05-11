#!/usr/bin/env python
"""
Eval harness for vibe translator.
Runs the styling agent against 10 prompts and dumps results to eval_outputs/<timestamp>/.

Usage:
    python scripts/eval_vibe.py
    python scripts/eval_vibe.py --prompts-file custom_prompts.txt
    python scripts/eval_vibe.py --output-dir /tmp/my_eval

NOT run in CI. Run manually and review outputs before signing off on backend.
"""
import argparse
import asyncio
import json
import pathlib
import sys
import time
from datetime import datetime

DEFAULT_PROMPTS = [
    "Fleabag hot priest energy but for a summer wedding",
    "Saltburn but work-appropriate",
    "First date with someone way out of my league",
    "Wedding in Italy and my ex will be there",
    "Low-key but expensive-looking, under £300",
    "Dark academia — Oxford, autumn, studying something you'll regret",
    "Copenhagen street style on a Tuesday",
    "Quiet luxury: no logos, all texture",
    "Hot girl summer but British, so it might rain",
    "New in town, first impression at a creative agency",
    "Build an outfit around this hero dress: champagne satin slip midi — find shoes, bag, and jewellery that work with it",
]


async def run_eval(output_dir: pathlib.Path, prompts: list[str]):
    import os
    sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

    from app.settings import get_settings
    from app.connectors.postgres import AsyncPostgresClient
    from app.connectors.redis_client import get_async_redis_client
    from app.ranker.search import EmbeddingService
    from app.styling.agent import run_styling_agent
    from app.styling.schemas import StyleRequest
    from app.styling.anthropic_client import get_anthropic_client

    settings = get_settings()
    pg = AsyncPostgresClient.from_settings(settings)
    redis = get_async_redis_client(settings)
    anthropic_client = get_anthropic_client()
    embedding_service = EmbeddingService(settings.embedding_service_url)

    output_dir.mkdir(parents=True, exist_ok=True)
    summary = []

    for i, prompt in enumerate(prompts, 1):
        print(f"\n[{i}/{len(prompts)}] {prompt}")
        t0 = time.time()
        events = []
        try:
            async for event in run_styling_agent(
                request=StyleRequest(prompt=prompt, num_outfits=3),
                pg=pg,
                anthropic_client=anthropic_client,
                embedding_service=embedding_service,
                redis=redis,
            ):
                events.append(event)
                if event.get("type") == "tool_call_end":
                    print(f"  → {event.get('human', '')}")
        except Exception as e:
            print(f"  ERROR: {e}")
            events.append({"type": "error", "message": str(e)})

        elapsed = time.time() - t0
        slug = prompt[:40].lower().replace(" ", "_").replace(",", "").replace("'", "").replace("—", "")
        out_file = output_dir / f"{i:02d}_{slug.strip('_')}.json"
        out_file.write_text(json.dumps({"prompt": prompt, "events": events}, indent=2))

        final = next((e for e in events if e["type"] == "final"), None)
        outfit_count = len(final.get("outfits", [])) if final else 0
        tool_calls = final.get("debug", {}).get("tool_calls", "?") if final else "?"
        print(f"  Done in {elapsed:.1f}s — {outfit_count} outfits, {tool_calls} tool calls")

        if final:
            for outfit in final.get("outfits", []):
                preview_url = outfit.get("preview_image_url")
                if preview_url:
                    print(f"  Preview: {preview_url}")

        summary.append({
            "prompt": prompt,
            "outfits": outfit_count,
            "tool_calls": tool_calls,
            "latency_s": round(elapsed, 1),
        })

    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    print(f"\nEval complete. Results in {output_dir}")
    _print_summary(summary)


def _print_summary(summary: list[dict]):
    print("\n── Summary ──────────────────────────────")
    for row in summary:
        status = "✓" if row["outfits"] >= 1 else "✗"
        print(f"  {status}  {row['prompt'][:50]:<50}  {row['outfits']} outfits  {row['tool_calls']} calls  {row['latency_s']}s")
    passed = sum(1 for r in summary if r["outfits"] >= 1)
    print(f"\n  {passed}/{len(summary)} prompts produced at least one outfit")


def main():
    parser = argparse.ArgumentParser(description="Eval harness for vibe translator")
    parser.add_argument("--prompts-file", help="Text file with one prompt per line")
    parser.add_argument(
        "--output-dir",
        default=f"eval_outputs/{datetime.now():%Y%m%d_%H%M%S}",
        help="Directory to write results (default: eval_outputs/<timestamp>)",
    )
    args = parser.parse_args()

    prompts = DEFAULT_PROMPTS
    if args.prompts_file:
        prompts = pathlib.Path(args.prompts_file).read_text().strip().splitlines()

    asyncio.run(run_eval(pathlib.Path(args.output_dir), prompts))


if __name__ == "__main__":
    main()
