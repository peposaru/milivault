"""Atomically provision one trusted model artifact after checksum validation."""

from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path
import shutil


def file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--destination", required=True, type=Path)
    parser.add_argument("--expected-sha256")
    args = parser.parse_args()
    source = args.source.expanduser().resolve()
    destination = args.destination.expanduser().resolve()
    actual = file_sha256(source)
    expected = str(args.expected_sha256 or "").strip().casefold()
    if expected and actual != expected:
        raise SystemExit(f"Checksum mismatch: expected {expected}, got {actual}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    shutil.copyfile(source, temporary)
    if file_sha256(temporary) != actual:
        temporary.unlink(missing_ok=True)
        raise SystemExit("Copied artifact failed checksum verification")
    temporary.replace(destination)
    print(json.dumps({
        "source": str(source),
        "destination": str(destination),
        "sha256": actual,
        "bytes": destination.stat().st_size,
    }, indent=2))


if __name__ == "__main__":
    main()

