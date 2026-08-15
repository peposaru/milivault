"""Inference runtimes and the ordered post-scrape pipeline."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from hashlib import sha256
import importlib
import io
import json
import math
from pathlib import Path
import sys
import time
from typing import Any
import urllib.request

import numpy as np

from .registry import ModelRegistry, ModelStage, normalize_value, usable_value


class ModelUnavailable(RuntimeError):
    pass


@dataclass
class Decision:
    label: str
    confidence: float
    source: str
    confidence_margin: float = 0.0
    top_predictions: list[dict[str, Any]] = field(default_factory=list)
    evidence_image_url: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineResult:
    updates: dict[str, str] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {"updates": self.updates, "events": self.events}


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _normalized_vector(values: Any) -> np.ndarray:
    vector = np.asarray(values, dtype=np.float32).reshape(-1)
    norm = float(np.linalg.norm(vector))
    if not norm:
        raise ModelUnavailable("Embedding vector has zero magnitude")
    return vector / norm


def _ranked_decision(classes: Any, probabilities: Any, source: str) -> Decision:
    classes = np.asarray(classes)
    probabilities = np.asarray(probabilities, dtype=np.float64).reshape(-1)
    total = float(probabilities.sum())
    if total <= 0:
        raise ModelUnavailable("Classifier returned invalid probabilities")
    probabilities = probabilities / total
    ranked = np.argsort(probabilities)[::-1]
    top = float(probabilities[ranked[0]])
    second = float(probabilities[ranked[1]]) if len(ranked) > 1 else 0.0
    return Decision(
        label=str(classes[ranked[0]]),
        confidence=top,
        confidence_margin=top - second,
        source=source,
        top_predictions=[
            {"label": str(classes[index]), "probability": float(probabilities[index])}
            for index in ranked[:3]
        ],
    )


class ArtifactStore:
    """Validate trusted local artifacts once, then keep them loaded."""

    def __init__(self):
        self._objects: dict[Path, Any] = {}
        self._digests: dict[Path, str] = {}

    def digest(self, path: Path) -> str:
        if path not in self._digests:
            self._digests[path] = _file_sha256(path)
        return self._digests[path]

    def verify(self, path: Path | None, expected_sha: str = "") -> Path:
        if path is None or not path.exists():
            raise ModelUnavailable(f"Required artifact is missing: {path}")
        if expected_sha and self.digest(path) != expected_sha:
            raise ModelUnavailable(f"Artifact checksum mismatch: {path}")
        return path

    def load(self, stage: ModelStage) -> Any:
        path = self.verify(stage.artifact_path, str(stage.config.get("artifact_sha256") or ""))
        expected_sklearn = str(stage.runtime.get("sklearn_version") or "").strip()
        if expected_sklearn:
            import sklearn

            if sklearn.__version__ != expected_sklearn:
                raise ModelUnavailable(
                    f"{stage.model_id} requires scikit-learn {expected_sklearn}; "
                    f"worker has {sklearn.__version__}"
                )
        if path not in self._objects:
            import joblib

            self._objects[path] = joblib.load(path)
        return self._objects[path]


class OpenAIEmbeddingProvider:
    """Small dependency-free client used only by embedding-based stages."""

    def __init__(self, credentials_path: str | Path | None, timeout: int = 60):
        self.credentials_path = Path(credentials_path).expanduser() if credentials_path else None
        self.timeout = int(timeout)
        self._key: str | None = None
        self._cache: dict[tuple[str, str], np.ndarray] = {}

    def _api_key(self) -> str:
        if self._key:
            return self._key
        if not self.credentials_path or not self.credentials_path.exists():
            raise ModelUnavailable("OpenAI embedding credentials are not configured")
        payload = json.loads(self.credentials_path.read_text(encoding="utf-8"))
        self._key = str(payload.get("key") or payload.get("api_key") or "").strip()
        if not self._key:
            raise ModelUnavailable("OpenAI credential file does not contain key or api_key")
        return self._key

    def embed(self, text: str, model: str) -> np.ndarray:
        cache_key = (model, sha256(text.encode("utf-8")).hexdigest())
        if cache_key in self._cache:
            return self._cache[cache_key]
        body = json.dumps({"input": [text], "model": model}).encode("utf-8")
        request = urllib.request.Request(
            "https://api.openai.com/v1/embeddings",
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {self._api_key()}",
                "Content-Type": "application/json",
                "User-Agent": "MiliVault-Classification-Worker/1.0",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
            vector = _normalized_vector(payload["data"][0]["embedding"])
        except Exception as exc:
            raise ModelUnavailable(f"OpenAI text embedding failed: {type(exc).__name__}: {exc}") from exc
        self._cache[cache_key] = vector
        return vector


class ClipImageEncoder:
    """Lazy shared CLIP encoder with an in-memory URL cache."""

    MAX_IMAGE_BYTES = 25 * 1024 * 1024

    def __init__(self, artifacts: ArtifactStore):
        self.artifacts = artifacts
        self._models: dict[tuple[str, Path], tuple[Any, Any, str]] = {}
        self._vectors: dict[tuple[str, Path, str], np.ndarray] = {}

    def _model(self, stage: ModelStage):
        weights = self.artifacts.verify(
            stage.clip_weights_path,
            str(stage.runtime.get("clip_weights_sha256") or ""),
        )
        model_name = str(stage.runtime.get("clip_model") or "ViT-B-32")
        key = (model_name, weights)
        if key not in self._models:
            try:
                import open_clip
                import torch
            except ImportError as exc:
                raise ModelUnavailable(
                    "CLIP runtime is missing; install requirements-classification.txt"
                ) from exc
            device = "cuda" if torch.cuda.is_available() else "cpu"
            model, _, preprocess = open_clip.create_model_and_transforms(
                model_name, pretrained=str(weights), device=device
            )
            model.eval()
            self._models[key] = (model, preprocess, device)
        return key, self._models[key]

    @classmethod
    def _download_image(cls, url: str, timeout: int):
        from PIL import Image

        request = urllib.request.Request(
            url,
            headers={"User-Agent": "MiliVault-Classification-Worker/1.0"},
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            data = response.read(cls.MAX_IMAGE_BYTES + 1)
        if len(data) > cls.MAX_IMAGE_BYTES:
            raise ValueError("image exceeds 25 MB")
        with Image.open(io.BytesIO(data)) as image:
            return image.convert("RGB")

    def encode(self, stage: ModelStage, urls: list[str]) -> list[tuple[str, np.ndarray]]:
        unique_urls = list(dict.fromkeys(str(url or "").strip() for url in urls if str(url or "").strip()))
        if not unique_urls:
            return []
        strategy = str(stage.runtime.get("image_strategy") or "all")
        if strategy == "primary":
            unique_urls = unique_urls[: int(stage.runtime.get("primary_fallback_count") or 3)]
        key, (model, preprocess, device) = self._model(stage)
        timeout = int(stage.runtime.get("image_timeout_seconds") or 30)
        batch_size = max(1, int(stage.runtime.get("clip_batch_size") or 16))
        output: list[tuple[str, np.ndarray]] = []
        pending: list[tuple[str, Any]] = []
        for url in unique_urls:
            cached = self._vectors.get((key[0], key[1], url))
            if cached is not None:
                output.append((url, cached))
                if strategy == "primary":
                    return output
                continue
            try:
                image = self._download_image(url, timeout)
                pending.append((url, preprocess(image)))
            except Exception:
                continue
            if strategy == "primary" and pending:
                break

        if pending:
            import torch

            with torch.inference_mode():
                for start in range(0, len(pending), batch_size):
                    batch = pending[start:start + batch_size]
                    vectors = model.encode_image(torch.stack([item[1] for item in batch]).to(device))
                    vectors = vectors / vectors.norm(dim=-1, keepdim=True)
                    for (url, _tensor), vector in zip(
                        batch, vectors.detach().cpu().numpy().astype(np.float32), strict=True
                    ):
                        normalized = _normalized_vector(vector)
                        self._vectors[(key[0], key[1], url)] = normalized
                        output.append((url, normalized))
        return output[:1] if strategy == "primary" else output


class RuntimeResolver:
    def __init__(
        self,
        artifacts: ArtifactStore,
        text_embeddings: OpenAIEmbeddingProvider,
        images: ClipImageEncoder,
        overrides: dict[str, Any] | None = None,
    ):
        self.artifacts = artifacts
        self.text_embeddings = text_embeddings
        self.images = images
        self.overrides = overrides or {}

    def predict(self, stage: ModelStage, product: dict[str, Any]) -> Decision:
        if stage.runtime_kind in self.overrides:
            return self.overrides[stage.runtime_kind].predict(stage, product)
        if stage.runtime_kind == "constant":
            return Decision(
                label=str(stage.runtime["value"]),
                confidence=1.0,
                source="constant_scope_rule",
            )
        if stage.runtime_kind == "clip_openai_combined":
            return self._clip_openai_combined(stage, product)
        if stage.runtime_kind == "openai_text_embedding":
            return self._openai_text(stage, product)
        if stage.runtime_kind == "gallery_tfidf_clip_late_fusion":
            return self._gallery_tfidf_clip(stage, product)
        raise ModelUnavailable(f"Unsupported runtime: {stage.runtime_kind}")

    @staticmethod
    def _text(product: dict[str, Any]) -> str:
        return "\n\n".join(
            value for value in (
                str(product.get("title") or "").strip(),
                str(product.get("description") or "").strip(),
            ) if value
        )

    @staticmethod
    def _bundle_value(bundle: dict[str, Any], key: str, stage: ModelStage):
        configured = str((stage.runtime.get("bundle_keys") or {}).get(key) or key)
        value = bundle.get(configured)
        if value is None:
            raise ModelUnavailable(f"{stage.model_id} artifact is missing {configured!r}")
        return value

    def _clip_openai_combined(self, stage: ModelStage, product: dict[str, Any]) -> Decision:
        bundle = self.artifacts.load(stage)
        classifier = self._bundle_value(bundle, "classifier", stage)
        text_model = str(stage.runtime.get("text_embedding_model") or "text-embedding-3-large")
        # Resolve the remote text input first. In the safe default configuration
        # credentials are intentionally absent, so this fails before loading
        # CLIP or downloading any gallery images.
        text = self.text_embeddings.embed(self._text(product), text_model)
        images = self.images.encode(stage, list(product.get("image_urls") or []))
        if not images:
            raise ModelUnavailable("No usable product image for combined classifier")
        image = _normalized_vector(images[0][1])
        features = np.concatenate([image / math.sqrt(2), text / math.sqrt(2)])[None, :]
        decision = _ranked_decision(
            classifier.classes_, classifier.predict_proba(features)[0], "primary_clip_openai_text"
        )
        decision.evidence_image_url = images[0][0]
        decision.metadata["gallery_image_count"] = len(images)
        return decision

    def _openai_text(self, stage: ModelStage, product: dict[str, Any]) -> Decision:
        bundle = self.artifacts.load(stage)
        classifier = self._bundle_value(bundle, "classifier", stage)
        text_model = str(stage.runtime.get("text_embedding_model") or "text-embedding-3-large")
        text = self.text_embeddings.embed(self._text(product), text_model)[None, :]
        return _ranked_decision(
            classifier.classes_, classifier.predict_proba(text)[0], "openai_text_embedding"
        )

    def _gallery_tfidf_clip(self, stage: ModelStage, product: dict[str, Any]) -> Decision:
        from sklearn.preprocessing import normalize

        bundle = self.artifacts.load(stage)
        vectorizer = self._bundle_value(bundle, "vectorizer", stage)
        text_classifier = self._bundle_value(bundle, "text_classifier", stage)
        image_classifier = self._bundle_value(bundle, "image_classifier", stage)
        text_features = normalize(vectorizer.transform([self._text(product)]).astype(np.float32))
        text_probabilities = text_classifier.predict_proba(text_features)[0]
        classes = np.asarray(text_classifier.classes_)
        images = self.images.encode(stage, list(product.get("image_urls") or []))
        if not images:
            return _ranked_decision(classes, text_probabilities, "local_text_no_usable_gallery")

        vectors = np.stack([item[1] for item in images]).astype(np.float32)
        per_image = image_classifier.predict_proba(vectors)
        if not np.array_equal(image_classifier.classes_, classes):
            raise ModelUnavailable("Image and text classifier labels do not match")
        pooling = str(stage.runtime.get("image_pooling") or "max")
        if pooling != "max":
            raise ModelUnavailable(f"Unsupported gallery pooling method: {pooling}")
        image_probabilities = per_image.max(axis=0)
        image_probabilities = image_probabilities / image_probabilities.sum()
        image_weight = float(stage.runtime.get("image_weight", 0.5))
        probabilities = image_weight * image_probabilities + (1.0 - image_weight) * text_probabilities
        decision = _ranked_decision(classes, probabilities, "gallery_clip_local_text_late_fusion")
        predicted_index = list(image_classifier.classes_).index(decision.label)
        evidence_index = int(np.argmax(per_image[:, predicted_index]))
        decision.evidence_image_url = images[evidence_index][0]
        decision.metadata["gallery_image_count"] = len(images)
        return decision


class ClassificationPipeline:
    def __init__(
        self,
        registry_path: str | Path,
        *,
        openai_credentials: str | Path | None = None,
        runtime_overrides: dict[str, Any] | None = None,
    ):
        self.registry = ModelRegistry(registry_path)
        self.artifacts = ArtifactStore()
        self.text_embeddings = OpenAIEmbeddingProvider(openai_credentials)
        self.images = ClipImageEncoder(self.artifacts)
        self.runtimes = RuntimeResolver(
            self.artifacts, self.text_embeddings, self.images, runtime_overrides
        )
        self._plugins: dict[str, Any] = {}

    def _plugin(self, stage: ModelStage):
        dotted = str(stage.config.get("plugin") or "").strip()
        if not dotted:
            return None
        if dotted not in self._plugins:
            module = importlib.import_module(dotted)
            expected = str(stage.config.get("plugin_version") or "").strip()
            actual = str(getattr(module, "PLUGIN_VERSION", ""))
            if expected and expected != actual:
                raise ModelUnavailable(
                    f"{stage.model_id} requires plugin {dotted} version {expected}; found {actual}"
                )
            self._plugins[dotted] = module
        return self._plugins[dotted]

    @staticmethod
    def _event(stage: ModelStage, product: dict[str, Any], status: str, **extra):
        return {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "product_id": product.get("product_id"),
            "url": product.get("url") or "",
            "model_id": stage.model_id,
            "model_version": stage.version,
            "target_field": stage.target.get("field"),
            "status": status,
            **extra,
        }

    def classify_product(self, product: dict[str, Any]) -> PipelineResult:
        context = dict(product)
        context["image_urls"] = list(dict.fromkeys(context.get("image_urls") or []))
        result = PipelineResult()
        for stage in self.registry.enabled_stages():
            if not stage.matches_scope(context):
                result.events.append(self._event(stage, context, "scope_skipped"))
                continue
            human_column = stage.target["human_column"]
            ml_column = stage.target["ml_column"]
            target_field = stage.target["field"]
            if usable_value(context.get(human_column)):
                result.events.append(self._event(stage, context, "human_preserved"))
                continue
            if usable_value(context.get(target_field)) and not bool(stage.config.get("overwrite_ml", False)):
                result.events.append(self._event(stage, context, "existing_value_preserved"))
                continue
            try:
                plugin = self._plugin(stage)
                decision = None
                if plugin and callable(getattr(plugin, "pre_predict", None)):
                    decision = plugin.pre_predict(context, stage.config)
                if decision is None:
                    decision = self.runtimes.predict(stage, context)
                if plugin and callable(getattr(plugin, "post_predict", None)):
                    decision = plugin.post_predict(decision, context, stage.config)
                if not isinstance(decision, Decision):
                    raise ModelUnavailable("Runtime or plugin returned an invalid decision")
                minimum = float((stage.config.get("acceptance") or {}).get("minimum_confidence", 0.0))
                if decision.confidence < minimum:
                    result.events.append(self._event(
                        stage,
                        context,
                        "review_only",
                        predicted_label=decision.label,
                        confidence=decision.confidence,
                        confidence_margin=decision.confidence_margin,
                        minimum_confidence=minimum,
                        source=decision.source,
                    ))
                    continue
                context[target_field] = decision.label
                context[ml_column] = decision.label
                result.updates[ml_column] = decision.label
                review_below = (stage.config.get("acceptance") or {}).get("review_below")
                result.events.append(self._event(
                    stage,
                    context,
                    "applied_review_recommended"
                    if review_below is not None and decision.confidence < float(review_below)
                    else "applied",
                    predicted_label=decision.label,
                    confidence=decision.confidence,
                    confidence_margin=decision.confidence_margin,
                    source=decision.source,
                    top_predictions=decision.top_predictions,
                    evidence_image_url=decision.evidence_image_url,
                    metadata=decision.metadata,
                ))
            except Exception as exc:
                result.events.append(self._event(
                    stage,
                    context,
                    "failed",
                    error=f"{type(exc).__name__}: {exc}"[:1000],
                ))
        return result

    def health(self, *, verify_checksums: bool = False) -> dict[str, Any]:
        rows = []
        for stage in self.registry.stages:
            status = "disabled" if not stage.enabled else "ready"
            error = ""
            if stage.enabled and stage.runtime_kind != "constant":
                try:
                    if not stage.artifact_path or not stage.artifact_path.exists():
                        raise ModelUnavailable(f"missing artifact: {stage.artifact_path}")
                    expected_sklearn = str(stage.runtime.get("sklearn_version") or "").strip()
                    if expected_sklearn:
                        import sklearn

                        if sklearn.__version__ != expected_sklearn:
                            raise ModelUnavailable(
                                f"requires scikit-learn {expected_sklearn}; worker has {sklearn.__version__}"
                            )
                    if verify_checksums:
                        self.artifacts.verify(
                            stage.artifact_path,
                            str(stage.config.get("artifact_sha256") or ""),
                        )
                    if stage.clip_weights_path and not stage.clip_weights_path.exists():
                        raise ModelUnavailable(f"missing CLIP weights: {stage.clip_weights_path}")
                    if verify_checksums and stage.clip_weights_path:
                        self.artifacts.verify(
                            stage.clip_weights_path,
                            str(stage.runtime.get("clip_weights_sha256") or ""),
                        )
                except Exception as exc:
                    status, error = "unavailable", str(exc)
            rows.append({
                "model_id": stage.model_id,
                "version": stage.version,
                "runtime": stage.runtime_kind,
                "status": status,
                "error": error,
            })
        return {"registry": str(self.registry.path), "models": rows}
