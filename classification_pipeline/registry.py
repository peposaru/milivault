"""Load and validate declarative classification model stages."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
from typing import Any, Iterable


REGISTRY_SCHEMA_VERSION = 1
ALLOWED_RUNTIME_KINDS = {
    "constant",
    "clip_openai_combined",
    "openai_text_embedding",
    "gallery_tfidf_clip_late_fusion",
}
ALLOWED_TARGET_COLUMNS = {
    "conflict_ml_designated",
    "nation_ml_designated",
    "item_type_ml_designated",
    "sub_item_type_ml_designated",
    "mil_branch_ml_designated",
}
ALLOWED_HUMAN_COLUMNS = {
    "user_confirmed_conflict",
    "user_confirmed_nation",
    "user_confirmed_item_type",
    "user_confirmed_sub_item_type",
    "user_confirmed_mil_branch",
}


class RegistryError(ValueError):
    pass


def normalize_value(value: Any) -> str:
    return str(value or "").strip().casefold()


def usable_value(value: Any) -> bool:
    return normalize_value(value) not in {"", "unknown", "none", "null"}


def _resolve_path(raw_path: str, base_dir: Path) -> Path:
    expanded = os.path.expandvars(os.path.expanduser(str(raw_path or "").strip()))
    path = Path(expanded)
    return path if path.is_absolute() else (base_dir / path).resolve()


@dataclass(frozen=True)
class ModelStage:
    manifest_path: Path
    config: dict[str, Any]

    @property
    def model_id(self) -> str:
        return str(self.config["model_id"])

    @property
    def version(self) -> str:
        return str(self.config["version"])

    @property
    def order(self) -> int:
        return int(self.config.get("order", 100))

    @property
    def enabled(self) -> bool:
        return bool(self.config.get("enabled", True))

    @property
    def runtime(self) -> dict[str, Any]:
        return dict(self.config.get("runtime") or {})

    @property
    def runtime_kind(self) -> str:
        return str(self.runtime.get("kind") or "")

    @property
    def target(self) -> dict[str, str]:
        return dict(self.config.get("target") or {})

    @property
    def artifact_path(self) -> Path | None:
        raw = str(self.config.get("artifact") or "").strip()
        return _resolve_path(raw, self.manifest_path.parent) if raw else None

    @property
    def clip_weights_path(self) -> Path | None:
        raw = str(self.runtime.get("clip_weights") or "").strip()
        return _resolve_path(raw, self.manifest_path.parent) if raw else None

    def matches_scope(self, product: dict[str, Any]) -> bool:
        for field, expected in (self.config.get("scope") or {}).items():
            candidates = expected if isinstance(expected, list) else [expected]
            normalized = {normalize_value(value) for value in candidates}
            if "*" in normalized:
                continue
            if normalize_value(product.get(field)) not in normalized:
                return False
        exclusion_text = "\n".join(str(product.get(field) or "") for field in (
            "title", "description", "url",
        ))
        for pattern in (self.config.get("scope_exclusions") or {}).get("text_regex", []):
            if re.search(pattern, exclusion_text, flags=re.IGNORECASE):
                return False
        return True


class ModelRegistry:
    def __init__(self, registry_path: str | Path):
        self.path = Path(registry_path).expanduser().resolve()
        self.stages = self._load()

    def _load(self) -> list[ModelStage]:
        if not self.path.exists():
            raise RegistryError(f"Classification registry does not exist: {self.path}")
        try:
            registry = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RegistryError(f"Cannot read classification registry {self.path}: {exc}") from exc
        if int(registry.get("schema_version") or 0) != REGISTRY_SCHEMA_VERSION:
            raise RegistryError("Unsupported classification registry schema_version")

        stages = []
        for entry in registry.get("models") or []:
            raw_path = entry.get("manifest") if isinstance(entry, dict) else entry
            if not raw_path:
                raise RegistryError("Every registry model needs a manifest path")
            path = _resolve_path(str(raw_path), self.path.parent)
            try:
                config = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise RegistryError(f"Cannot read model manifest {path}: {exc}") from exc
            stage = ModelStage(path, config)
            self._validate_stage(stage)
            stages.append(stage)

        model_ids = [stage.model_id for stage in stages]
        duplicates = sorted({value for value in model_ids if model_ids.count(value) > 1})
        if duplicates:
            raise RegistryError(f"Duplicate model_id values: {', '.join(duplicates)}")
        known = set(model_ids)
        for stage in stages:
            missing = set(stage.config.get("depends_on") or []) - known
            if missing:
                raise RegistryError(
                    f"{stage.model_id} has unknown dependencies: {', '.join(sorted(missing))}"
                )
        ordered = sorted(stages, key=lambda stage: (stage.order, stage.model_id))
        seen = set()
        for stage in ordered:
            unmet = set(stage.config.get("depends_on") or []) - seen
            if unmet:
                raise RegistryError(
                    f"{stage.model_id} must run after: {', '.join(sorted(unmet))}"
                )
            seen.add(stage.model_id)
        return ordered

    @staticmethod
    def _validate_stage(stage: ModelStage) -> None:
        config = stage.config
        if int(config.get("schema_version") or 0) != REGISTRY_SCHEMA_VERSION:
            raise RegistryError(f"{stage.manifest_path} has an unsupported schema_version")
        if not re.fullmatch(r"[a-z0-9][a-z0-9_.-]{2,119}", stage.model_id):
            raise RegistryError(f"Invalid model_id: {stage.model_id!r}")
        if not stage.version:
            raise RegistryError(f"{stage.model_id} needs a version")
        if stage.runtime_kind not in ALLOWED_RUNTIME_KINDS:
            raise RegistryError(
                f"{stage.model_id} has unsupported runtime kind {stage.runtime_kind!r}"
            )
        exclusions = config.get("scope_exclusions") or {}
        if not isinstance(exclusions, dict):
            raise RegistryError(f"{stage.model_id} scope_exclusions must be an object")
        patterns = exclusions.get("text_regex") or []
        if not isinstance(patterns, list):
            raise RegistryError(f"{stage.model_id} scope_exclusions.text_regex must be a list")
        for pattern in patterns:
            try:
                re.compile(str(pattern), flags=re.IGNORECASE)
            except re.error as exc:
                raise RegistryError(
                    f"{stage.model_id} has invalid scope exclusion regex {pattern!r}: {exc}"
                ) from exc
        target = stage.target
        if not target.get("field"):
            raise RegistryError(f"{stage.model_id} needs target.field")
        if target.get("ml_column") not in ALLOWED_TARGET_COLUMNS:
            raise RegistryError(f"{stage.model_id} has an unsafe target.ml_column")
        if target.get("human_column") not in ALLOWED_HUMAN_COLUMNS:
            raise RegistryError(f"{stage.model_id} has an unsafe target.human_column")
        if stage.runtime_kind != "constant" and stage.artifact_path is None:
            raise RegistryError(f"{stage.model_id} needs an artifact")
        expected_sha = str(config.get("artifact_sha256") or "")
        if expected_sha and not re.fullmatch(r"[0-9a-f]{64}", expected_sha):
            raise RegistryError(f"{stage.model_id} has an invalid artifact_sha256")
        clip_sha = str(stage.runtime.get("clip_weights_sha256") or "")
        if clip_sha and not re.fullmatch(r"[0-9a-f]{64}", clip_sha):
            raise RegistryError(f"{stage.model_id} has an invalid clip_weights_sha256")

    def enabled_stages(self) -> Iterable[ModelStage]:
        return (stage for stage in self.stages if stage.enabled)

    def describe(self) -> list[dict[str, Any]]:
        return [
            {
                "model_id": stage.model_id,
                "version": stage.version,
                "order": stage.order,
                "enabled": stage.enabled,
                "runtime": stage.runtime_kind,
                "target": stage.target.get("field"),
                "artifact": str(stage.artifact_path or ""),
            }
            for stage in self.stages
        ]
