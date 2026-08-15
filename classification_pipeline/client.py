"""Scraper-side client for the isolated, persistent classification worker."""

from __future__ import annotations

import atexit
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import threading
import uuid


logger = logging.getLogger(__name__)


class ClassificationPipelineClient:
    """Keep heavyweight ML dependencies out of the scraper's base environment."""

    def __init__(self, settings: dict):
        self.settings = settings or {}
        self.enabled = bool(self.settings.get("enableClassificationPipeline", True))
        self.mode = str(self.settings.get("classificationPipelineMode") or "shadow").strip().casefold()
        if self.mode not in {"shadow", "apply"}:
            raise ValueError("classificationPipelineMode must be 'shadow' or 'apply'")
        self.allow_remote_embeddings = bool(
            self.settings.get("classificationAllowRemoteEmbeddings", False)
        )
        self.registry_path = Path(
            self.settings.get("classificationRegistry") or "classification_models/registry.json"
        ).expanduser().resolve()
        self.worker_python = Path(
            self.settings.get("classificationPython") or sys.executable
        ).expanduser().resolve()
        self.openai_credentials = str(self.settings.get("openaiCred") or "")
        self.audit_dir = Path(
            self.settings.get("classificationAuditDir")
            or (self.registry_path.parent / "audit")
        ).expanduser().resolve()
        self._process: subprocess.Popen | None = None
        self._lock = threading.Lock()
        atexit.register(self.close)

    def _audit(self, event: dict) -> None:
        try:
            self.audit_dir.mkdir(parents=True, exist_ok=True)
            path = self.audit_dir / f"classification-{datetime.now(timezone.utc):%Y-%m-%d}.jsonl"
            with path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")
        except Exception as exc:
            logger.warning("CLASSIFICATION: could not write audit event: %s", exc)

    def _start(self) -> None:
        if not self.enabled:
            raise RuntimeError("classification pipeline is disabled")
        if self._process and self._process.poll() is None:
            return
        if not self.registry_path.exists():
            raise RuntimeError(f"classification registry is missing: {self.registry_path}")
        if not self.worker_python.exists():
            raise RuntimeError(f"classification Python is missing: {self.worker_python}")
        project_root = Path(__file__).resolve().parents[1]
        environment = os.environ.copy()
        existing_pythonpath = environment.get("PYTHONPATH", "")
        environment["PYTHONPATH"] = str(project_root) + (
            os.pathsep + existing_pythonpath if existing_pythonpath else ""
        )
        command = [
            str(self.worker_python),
            "-B",
            "-u",
            "-m",
            "classification_pipeline.worker",
            "--registry",
            str(self.registry_path),
        ]
        if self.openai_credentials and self.allow_remote_embeddings:
            command.extend(["--openai-credentials", self.openai_credentials])
        self._process = subprocess.Popen(
            command,
            cwd=str(project_root),
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None,
            text=True,
            encoding="utf-8",
            bufsize=1,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        logger.info("CLASSIFICATION: worker started with %s", self.worker_python)

    def _request(self, action: str, payload: dict | None = None) -> dict:
        request_id = uuid.uuid4().hex
        with self._lock:
            self._start()
            assert self._process and self._process.stdin and self._process.stdout
            request = {"request_id": request_id, "action": action, **(payload or {})}
            try:
                self._process.stdin.write(json.dumps(request, ensure_ascii=False, default=str) + "\n")
                self._process.stdin.flush()
                raw = self._process.stdout.readline()
            except Exception as exc:
                self.close()
                raise RuntimeError(f"classification worker communication failed: {exc}") from exc
            if not raw:
                exit_code = self._process.poll()
                self.close()
                raise RuntimeError(f"classification worker exited unexpectedly ({exit_code})")
            response = json.loads(raw)
            if response.get("request_id") != request_id:
                raise RuntimeError("classification worker returned a mismatched request_id")
            if not response.get("ok"):
                raise RuntimeError(str(response.get("error") or "classification worker failed"))
            return response

    def classify_product(self, product: dict) -> dict[str, str]:
        if not self.enabled:
            return {}
        try:
            response = self._request("classify", {"product": product})
            result = response.get("result") or {}
            for event in result.get("events") or []:
                self._audit(event)
            proposed_updates = dict(result.get("updates") or {})
            if self.mode == "apply":
                return proposed_updates
            if proposed_updates:
                self._audit({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "product_id": product.get("product_id"),
                    "url": product.get("url") or "",
                    "status": "shadow_proposed_updates",
                    "updates": proposed_updates,
                })
            return {}
        except Exception as exc:
            logger.error("CLASSIFICATION: product %s failed safely: %s", product.get("product_id"), exc)
            self._audit({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "product_id": product.get("product_id"),
                "url": product.get("url") or "",
                "status": "client_failed",
                "error": f"{type(exc).__name__}: {exc}"[:1000],
            })
            return {}

    def health(self) -> dict:
        if not self.enabled:
            return {"status": "disabled", "models": []}
        return self._request("health").get("health") or {}

    def close(self) -> None:
        process, self._process = self._process, None
        if not process:
            return
        try:
            if process.poll() is None and process.stdin:
                process.stdin.write(json.dumps({"request_id": uuid.uuid4().hex, "action": "shutdown"}) + "\n")
                process.stdin.flush()
                process.wait(timeout=5)
        except Exception:
            try:
                process.terminate()
            except Exception:
                pass
