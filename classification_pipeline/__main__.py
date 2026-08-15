"""Validate the registry and installed model artifacts."""

from __future__ import annotations

import argparse
import json

from .core import ClassificationPipeline


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", required=True)
    parser.add_argument("--openai-credentials")
    parser.add_argument("--verify-checksums", action="store_true")
    args = parser.parse_args()
    pipeline = ClassificationPipeline(
        args.registry,
        openai_credentials=args.openai_credentials,
    )
    health = pipeline.health(verify_checksums=args.verify_checksums)
    print(json.dumps(health, indent=2))
    if any(row["status"] == "unavailable" for row in health["models"]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
