#!/usr/bin/env python3
"""Show in the job log what Claude Code does while it does it.

Reads `claude -p --output-format stream-json --verbose` on stdin and prints a line for
each thing it says and each tool it calls, so the log follows the session live, and
still shows it when the deadline stops the session before its final result. Tool
output is not printed. Every event also goes, unfiltered, to the file named by the
first argument, which stays on the runner rather than in the run's public artifact.

A line it cannot read is skipped: this sits in Claude Code's output pipe, and dying
would take the session with it.
"""

import json
import sys

TEXT_LIMIT = 800


def short(value, limit: int = TEXT_LIMIT) -> str:
    text = " ".join(str(value).split())
    return text if len(text) <= limit else text[:limit] + " ..."


def show(event: dict) -> None:
    kind = event.get("type")
    if kind == "assistant":
        for part in event.get("message", {}).get("content", []):
            if part.get("type") == "text" and part.get("text", "").strip():
                print(f"Claude: {short(part['text'])}")
            elif part.get("type") == "tool_use":
                args = part.get("input") or {}
                detail = args.get("command") or args.get("file_path") or args.get("pattern") or ""
                print(f"[{part.get('name', '?')}] {short(detail, 300)}")
    elif kind == "result":
        minutes = round((event.get("duration_ms") or 0) / 60000)
        print(f"== Claude Code finished ({event.get('subtype')}): {event.get('num_turns')} turns, "
              f"{minutes} min, ${event.get('total_cost_usd', '?')}")
        if event.get("result"):
            print(str(event["result"])[-4000:])


def main() -> int:
    raw = open(sys.argv[1], "a") if len(sys.argv) > 1 else None
    for line in sys.stdin:
        try:
            if raw:
                raw.write(line)
                raw.flush()
            show(json.loads(line))
            sys.stdout.flush()
        except Exception:  # noqa: BLE001 - see the module docstring
            continue
    return 0


if __name__ == "__main__":
    sys.exit(main())
