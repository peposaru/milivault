"""JSON-lines worker process for post-scrape classification."""

from __future__ import annotations

import argparse
from contextlib import redirect_stdout
import json
import sys
import traceback

from .core import ClassificationPipeline


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", required=True)
    parser.add_argument("--openai-credentials")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    protocol_stdout = sys.stdout
    with redirect_stdout(sys.stderr):
        pipeline = ClassificationPipeline(
            args.registry,
            openai_credentials=args.openai_credentials,
        )
    for raw in sys.stdin:
        if not raw.strip():
            continue
        request_id = ""
        try:
            request = json.loads(raw)
            request_id = str(request.get("request_id") or "")
            action = request.get("action")
            if action == "shutdown":
                response = {"request_id": request_id, "ok": True}
                protocol_stdout.write(json.dumps(response) + "\n")
                protocol_stdout.flush()
                return
            with redirect_stdout(sys.stderr):
                if action == "health":
                    response = {
                        "request_id": request_id,
                        "ok": True,
                        "health": pipeline.health(),
                    }
                elif action == "classify":
                    result = pipeline.classify_product(dict(request.get("product") or {}))
                    response = {
                        "request_id": request_id,
                        "ok": True,
                        "result": result.as_dict(),
                    }
                else:
                    raise ValueError(f"Unknown worker action: {action!r}")
        except Exception as exc:
            traceback.print_exc(file=sys.stderr)
            response = {
                "request_id": request_id,
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}"[:1000],
            }
        protocol_stdout.write(json.dumps(response, ensure_ascii=False, default=str) + "\n")
        protocol_stdout.flush()


if __name__ == "__main__":
    main()

