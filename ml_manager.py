# ml_manager.py — upgraded, streamlined, with detailed logging

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple

import joblib
import numpy as np
import sklearn
from sklearn.pipeline import Pipeline


# ---------------------------
# Logging
# ---------------------------

logger = logging.getLogger(__name__)


# ---------------------------
# Helpers
# ---------------------------

@dataclass(frozen=True)
class Thresholds:
    per_class: Dict[str, float]
    fallback: float

    @classmethod
    def from_path_or_dict(cls, path_or_dict: Optional[object], fallback: float) -> "Thresholds":
        """
        Load thresholds either from a JSON filepath or a dict. If missing, returns empty with fallback.
        JSON format: {"LABEL_A": 0.93, "LABEL_B": 0.88, ...}
        """
        if isinstance(path_or_dict, str) and path_or_dict:
            try:
                with open(path_or_dict, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    return cls(per_class=data, fallback=fallback)
            except Exception as e:
                logger.debug(f"MLManager: thresholds load fallback ({path_or_dict}): {e}")
        elif isinstance(path_or_dict, dict):
            return cls(per_class=path_or_dict, fallback=fallback)
        return cls(per_class={}, fallback=fallback)

    def tau(self, label: str) -> float:
        return float(self.per_class.get(label, self.fallback))


def _preview(s: str, n: int = 160) -> str:
    s = (s or "").replace("\n", " ").strip()
    return s if len(s) <= n else s[:n] + "…"


def _topk(proba_row: np.ndarray, classes: np.ndarray, k: int = 3):
    if proba_row.ndim != 1:
        proba_row = proba_row.ravel()
    k = max(1, min(k, proba_row.shape[0]))
    idxs = np.argpartition(-proba_row, k - 1)[:k]
    idxs = idxs[np.argsort(-proba_row[idxs])]
    return [(str(classes[i]), float(proba_row[i])) for i in idxs]


def _load_pipeline(path: Optional[str]) -> Tuple[Optional[object], Optional[np.ndarray]]:
    """
    Load a saved model artifact (either a bundle dict or a raw pipeline).
    Returns (pipeline, classes_) or (None, None) if path is falsy.
    """
    if not path:
        logger.warning("MLManager: _load_pipeline called with empty path.")
        return None, None

    logger.info(f"MLManager: loading model from {path}")
    obj = joblib.load(path)
    logger.debug(f"MLManager: loaded object type = {type(obj)}")

    # --- New format: bundle dict with metadata ---
    if isinstance(obj, dict) and "pipeline" in obj:
        trained_ver = obj.get("sklearn_version")
        logger.info(f"MLManager: bundle detected | trained sklearn={trained_ver} | runtime sklearn={sklearn.__version__}")
        if trained_ver and trained_ver != sklearn.__version__:
            raise RuntimeError(
                f"Model '{path}' was trained with scikit-learn {trained_ver}, "
                f"but runtime is {sklearn.__version__}. "
                f"Install scikit-learn=={trained_ver} or retrain & resave."
            )
        pipe = obj["pipeline"]
        classes = obj.get("classes_")
        steps = getattr(pipe, "named_steps", {})
        logger.debug(f"MLManager: pipeline steps = {list(steps.keys()) or ['<unknown>']}")
        estimator = steps.get("clf", pipe)
        logger.debug(f"MLManager: final estimator = {type(estimator)}")

        if classes is None:
            classes = getattr(estimator, "classes_", getattr(pipe, "classes_", None))
        if classes is None:
            raise ValueError(f"Pipeline in bundle '{path}' has no classes_.")
        if not hasattr(estimator, "predict_proba"):
            raise ValueError(f"Pipeline in bundle '{path}' must implement predict_proba on its final estimator.")
        logger.info(f"MLManager: model ready with {len(classes)} classes.")
        return pipe, np.array(classes)

    # --- Legacy format: raw pipeline/estimator pickle ---
    pipe = obj
    steps = getattr(pipe, "named_steps", {})
    estimator = steps.get("clf", pipe)
    logger.debug(f"MLManager: legacy object; final estimator = {type(estimator)}")

    if not hasattr(estimator, "predict_proba"):
        raise ValueError(
            f"Pipeline at {path} must implement predict_proba on its final estimator. "
            f"If this was trained with SGD(loss='log') on an older scikit-learn, "
            f"retrain with loss='log_loss' or load under the original sklearn version."
        )
    classes = getattr(estimator, "classes_", getattr(pipe, "classes_", None))
    if classes is None:
        raise ValueError(f"Pipeline at {path} has no classes_ on final estimator.")
    logger.info(f"MLManager: legacy pipeline ready with {len(classes)} classes.")
    return pipe, np.array(classes)


def _env_disabled(var_name: str) -> bool:
    """Treat 1/true/yes/on as disabled flags from env."""
    val = os.getenv(var_name, "").strip().lower()
    return val in ("1", "true", "yes", "on")


# ---------------------------
# MLManager
# ---------------------------

class MLManager:
    """
    Local ML classifier facade.

    Public API:
      - load() -> MLManager
      - info() -> dict
      - predict(title, description, image_url=None) -> per-label ML results (no OpenAI)
      - classify(title, description, image_url=None) -> alias of predict() for back-compat
      - classify_with_meta(...) -> (result, meta)
      - classify_single_product(...) -> result

    Behavior:
      • Runs local models (item_type / conflict / nation) with thresholds.
      • Optional OpenAI fallback to fill missing fields.
      • Returns OpenAI-shaped `result`; `meta` reports provenance/confidence.
    """

    DEFAULT_ITEM_TYPE_TAU = 0.85
    DEFAULT_CONFLICT_TAU  = 0.85
    DEFAULT_NATION_TAU    = 0.85

    def __init__(self, settings: dict, openai_manager: Optional[object] = None):
        self.settings = settings or {}
        self.openai = openai_manager

        # --- artifact paths from settings ---
        self.item_type_path = self.settings.get("itemTypeModel")
        self.item_type_thresholds_path = self.settings.get("itemTypeThresholdsJson")
        self.conflict_path = self.settings.get("conflictModel")
        self.conflict_thresholds_path = self.settings.get("conflictThresholdsJson")
        self.nation_path = self.settings.get("nationModel")
        self.nation_thresholds_path = self.settings.get("nationThresholdsJson")

        # --- toggles from settings (ANDed with file presence) + env overrides ---
        st_itm = bool(self.settings.get("enableItemTypeModel", True))
        st_con = bool(self.settings.get("enableConflictModel", False))
        st_nat = bool(self.settings.get("enableNationModel", False))

        self.enable_item_type = (
            st_itm and bool(self.item_type_path and os.path.exists(self.item_type_path)) and not _env_disabled("ML_DISABLE_ITEM_TYPE")
        )
        self.enable_conflict = (
            st_con and bool(self.conflict_path and os.path.exists(self.conflict_path)) and not _env_disabled("ML_DISABLE_CONFLICT")
        )
        self.enable_nation = (
            st_nat and bool(self.nation_path and os.path.exists(self.nation_path)) and not _env_disabled("ML_DISABLE_NATION")
        )

        # --- thresholds ---
        self.item_type_thresholds = Thresholds.from_path_or_dict(
            self.item_type_thresholds_path, self.DEFAULT_ITEM_TYPE_TAU
        )
        self.conflict_tau = float(self.DEFAULT_CONFLICT_TAU)
        self.nation_tau   = float(self.DEFAULT_NATION_TAU)
        if self.enable_conflict:
            self.conflict_tau = self._load_global_tau(self.conflict_thresholds_path, self.DEFAULT_CONFLICT_TAU)
        if self.enable_nation:
            self.nation_tau = self._load_global_tau(self.nation_thresholds_path, self.DEFAULT_NATION_TAU)

        # --- model holders (lazy) ---
        self.item_type_pipe = None
        self.item_type_classes = None
        self.conflict_pipe = None
        self.conflict_classes = None
        self.nation_pipe = None
        self.nation_classes = None
        self._loaded = False

    # ---------------------------
    # Public API
    # ---------------------------

    def load(self) -> "MLManager":
        """Load pipelines lazily; idempotent."""
        if self._loaded:
            logger.debug("MLManager.load: already loaded; returning cached state.")
            return self

        logger.info("MLManager.load: starting")
        logger.info(f"Settings: enable_item_type={self.enable_item_type}, enable_conflict={self.enable_conflict}, enable_nation={self.enable_nation}")

        try:
            if self.enable_item_type:
                self.item_type_pipe, self.item_type_classes = _load_pipeline(self.item_type_path)

                # --- Sanity check: expect a Pipeline([('feats', FeatureUnion/Vectorizers), ('clf', ...)]) ---
                if not isinstance(self.item_type_pipe, Pipeline):
                    raise RuntimeError(
                        f"Item-type model is not an sklearn Pipeline (got {type(self.item_type_pipe)}). "
                        "Point to the bundle saved from the notebook that stores the full Pipeline in 'pipeline'."
                    )
                steps = getattr(self.item_type_pipe, "named_steps", {})
                if "feats" not in steps:
                    raise RuntimeError(
                        "Loaded item_type model is missing the 'feats' step. "
                        "Expected Pipeline([('feats', FeatureUnion/Tfidf), ('clf', ...)]). "
                        "Ensure you saved 'best' (the full pipeline) to the bundle."
                    )
                if not hasattr(steps["feats"], "transform"):
                    raise RuntimeError("Pipeline 'feats' step has no .transform; transformer appears invalid.")
                logger.info("MLManager.load: item_type pipeline loaded and validated.")
        except Exception as e:
            msg = str(e)
            if "_sgd_fast.Log" in msg or "trained with scikit-learn" in msg:
                logger.error(
                    "MLManager.load: item_type model incompatible with current scikit-learn. "
                    "Install the training version or retrain with SGD(loss='log_loss') and resave as a bundle."
                )
            else:
                logger.error(f"MLManager.load: failed to load item_type model: {e}", exc_info=True)
            self.enable_item_type = False

        try:
            if self.enable_conflict:
                self.conflict_pipe, self.conflict_classes = _load_pipeline(self.conflict_path)
                logger.info("MLManager.load: conflict pipeline loaded.")
        except Exception as e:
            logger.error(f"MLManager.load: failed to load conflict model: {e}", exc_info=True)
            self.enable_conflict = False

        try:
            if self.enable_nation:
                self.nation_pipe, self.nation_classes = _load_pipeline(self.nation_path)
                logger.info("MLManager.load: nation pipeline loaded.")
        except Exception as e:
            logger.error(f"MLManager.load: failed to load nation model: {e}", exc_info=True)
            self.enable_nation = False

        self._loaded = True
        logger.info("MLManager.load: completed")
        return self

    def info(self) -> dict:
        """Minimal metadata for debugging/telemetry."""
        return {
            "item_type": {
                "enabled": self.enable_item_type,
                "path": self.item_type_path,
                "thresholds_source": self.item_type_thresholds_path,
                "settings_enabled": bool(self.settings.get("enableItemTypeModel", True)),
                "env_disabled": _env_disabled("ML_DISABLE_ITEM_TYPE"),
            },
            "conflict": {
                "enabled": self.enable_conflict,
                "path": self.conflict_path,
                "tau": self.conflict_tau,
                "settings_enabled": bool(self.settings.get("enableConflictModel", False)),
                "env_disabled": _env_disabled("ML_DISABLE_CONFLICT"),
            },
            "nation": {
                "enabled": self.enable_nation,
                "path": self.nation_path,
                "tau": self.nation_tau,
                "settings_enabled": bool(self.settings.get("enableNationModel", False)),
                "env_disabled": _env_disabled("ML_DISABLE_NATION"),
            },
            "loaded": self._loaded,
        }

    # --- ML-only API (no OpenAI) ---

    def predict(self, title: str, description: str, image_url: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """
        ML-only inference for compatibility with callers expecting .predict().
        Returns per-label dicts like:
          {"item_type": {"value": str|None, "conf": float|None, "threshold": float|None, "accepted": bool}, ...}
        """
        self.load()
        out: Dict[str, Dict[str, Any]] = {}

        if not self.enable_item_type:
            logger.warning("predict: item_type ML disabled (either not configured or failed to load).")
        else:
            try:
                label, conf = self._predict_one(self.item_type_pipe, self.item_type_classes, title, description)
                tau = float(self.item_type_thresholds.tau(label))
                accepted = conf >= tau
                out["item_type"] = {"value": label, "conf": conf, "threshold": tau, "accepted": accepted}
                logger.info(f"predict[item_type]: label='{label}' conf={conf:.4f} tau={tau:.4f} accepted={accepted}")
            except Exception as e:
                logger.error(f"MLManager.predict: item_type failed: {e}", exc_info=True)

        if self.enable_conflict and self.conflict_pipe is not None:
            try:
                label, conf = self._predict_one(self.conflict_pipe, self.conflict_classes, title, description)
                tau = float(self.conflict_tau)
                accepted = conf >= tau
                out["conflict"] = {"value": label, "conf": conf, "threshold": tau, "accepted": accepted}
                logger.info(f"predict[conflict]: label='{label}' conf={conf:.4f} tau={tau:.4f} accepted={accepted}")
            except Exception as e:
                logger.error(f"MLManager.predict: conflict failed: {e}", exc_info=True)
        else:
            if not self.enable_conflict:
                logger.debug("predict: conflict ML disabled by settings or missing model.")

        if self.enable_nation and self.nation_pipe is not None:
            try:
                label, conf = self._predict_one(self.nation_pipe, self.nation_classes, title, description)
                tau = float(self.nation_tau)
                accepted = conf >= tau
                out["nation"] = {"value": label, "conf": conf, "threshold": tau, "accepted": accepted}
                logger.info(f"predict[nation]: label='{label}' conf={conf:.4f} tau={tau:.4f} accepted={accepted}")
            except Exception as e:
                logger.error(f"MLManager.predict: nation failed: {e}", exc_info=True)
        else:
            if not self.enable_nation:
                logger.debug("predict: nation ML disabled by settings or missing model.")

        return out

    def classify(self, title: str, description: str, image_url: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """Back-compat alias used by some callers; same as .predict(), ML-only."""
        return self.predict(title=title, description=description, image_url=image_url)

    # --- ML + optional OpenAI fallback ---

    def classify_with_meta(
        self,
        title: str,
        description: str,
        image_url: Optional[str] = None,
        use_openai_fallback: bool = True,
    ):
        """
        Returns (result, meta)
        """
        self.load()

        result = {
            "conflict_ai_generated": None,
            "nation_ai_generated": None,
            "item_type_ai_generated": None,
            "supergroup_ai_generated": None,
        }
        meta = {
            "item_type": {"source": "disabled" if not self.enable_item_type else "none",
                          "accepted": None, "conf": None, "tau": None, "label": None},
            "conflict":  {"source": "disabled" if not self.enable_conflict else "none",
                          "accepted": None, "conf": None, "tau": None, "label": None},
            "nation":    {"source": "disabled" if not self.enable_nation else "none",
                          "accepted": None, "conf": None, "tau": None, "label": None},
            "used_openai": False,
        }

        # --- item_type via ML
        if self.enable_item_type and self.item_type_pipe is not None:
            try:
                label, conf = self._predict_one(self.item_type_pipe, self.item_type_classes, title, description)
                tau = float(self.item_type_thresholds.tau(label))
                accepted = conf >= tau
                meta["item_type"].update({
                    "source": "ml" if accepted else "none",
                    "accepted": accepted, "conf": conf, "tau": tau, "label": label
                })
                if accepted:
                    result["item_type_ai_generated"] = label  # caller decides which column to update
                logger.info(f"classify[item_type]: label='{label}' conf={conf:.4f} tau={tau:.4f} accepted={accepted}")
            except Exception as e:
                logger.error(f"MLManager: item_type prediction failed: {e}", exc_info=True)
        else:
            logger.warning("classify: item_type ML disabled; will rely on fallback if enabled.")

        # --- conflict via ML
        if self.enable_conflict and self.conflict_pipe is not None:
            try:
                label, conf = self._predict_one(self.conflict_pipe, self.conflict_classes, title, description)
                tau = float(self.conflict_tau)
                accepted = conf >= tau
                meta["conflict"].update({
                    "source": "ml" if accepted else "none",
                    "accepted": accepted, "conf": conf, "tau": tau, "label": label
                })
                if accepted:
                    result["conflict_ai_generated"] = label
                logger.info(f"classify[conflict]: label='{label}' conf={conf:.4f} tau={tau:.4f} accepted={accepted}")
            except Exception as e:
                logger.error(f"MLManager: conflict prediction failed: {e}", exc_info=True)
        else:
            if not self.enable_conflict:
                logger.debug("classify: conflict ML disabled by settings or missing model.")

        # --- nation via ML
        if self.enable_nation and self.nation_pipe is not None:
            try:
                label, conf = self._predict_one(self.nation_pipe, self.nation_classes, title, description)
                tau = float(self.nation_tau)
                accepted = conf >= tau
                meta["nation"].update({
                    "source": "ml" if accepted else "none",
                    "accepted": accepted, "conf": conf, "tau": tau, "label": label
                })
                if accepted:
                    result["nation_ai_generated"] = label
                logger.info(f"classify[nation]: label='{label}' conf={conf:.4f} tau={tau:.4f} accepted={accepted}")
            except Exception as e:
                logger.error(f"MLManager: nation prediction failed: {e}", exc_info=True)
        else:
            if not self.enable_nation:
                logger.debug("classify: nation ML disabled by settings or missing model.")

        # --- OpenAI fallback for any missing fields
        need_openai = (
            (result["item_type_ai_generated"] is None)
            or (result["conflict_ai_generated"] is None)
            or (result["nation_ai_generated"] is None)
        )
        if use_openai_fallback and self.openai and need_openai:
            missing = [k for k in ("item_type_ai_generated", "conflict_ai_generated", "nation_ai_generated")
                       if result[k] is None]
            logger.info(f"classify: using OpenAI fallback for fields: {missing}")
            try:
                ai = self.openai.classify_single_product(title=title, description=description, image_url=image_url) or {}
                meta["used_openai"] = True

                if result["item_type_ai_generated"] is None and ai.get("item_type_ai_generated"):
                    result["item_type_ai_generated"] = ai["item_type_ai_generated"]
                    meta["item_type"].update({"source": "openai", "accepted": True, "label": ai["item_type_ai_generated"]})
                    logger.info(f"classify[item_type]: filled by OpenAI -> {ai['item_type_ai_generated']}")

                if result["conflict_ai_generated"] is None and ai.get("conflict_ai_generated"):
                    result["conflict_ai_generated"] = ai["conflict_ai_generated"]
                    meta["conflict"].update({"source": "openai", "accepted": True, "label": ai["conflict_ai_generated"]})
                    logger.info(f"classify[conflict]: filled by OpenAI -> {ai['conflict_ai_generated']}")

                if result["nation_ai_generated"] is None and ai.get("nation_ai_generated"):
                    result["nation_ai_generated"] = ai["nation_ai_generated"]
                    meta["nation"].update({"source": "openai", "accepted": True, "label": ai["nation_ai_generated"]})
                    logger.info(f"classify[nation]: filled by OpenAI -> {ai['nation_ai_generated']}")

                if ai.get("supergroup_ai_generated"):
                    result["supergroup_ai_generated"] = ai["supergroup_ai_generated"]

            except Exception as e:
                logger.error(f"MLManager: OpenAI fallback failed: {e}", exc_info=True)
        else:
            if not need_openai:
                logger.info("classify: all fields satisfied by ML; no fallback needed.")
            elif not use_openai_fallback:
                logger.info("classify: fallback disabled by caller.")
            elif not self.openai:
                logger.info("classify: no OpenAI manager provided for fallback.")

        return result, meta

    def classify_single_product(
        self,
        title: str,
        description: str,
        image_url: Optional[str] = None,
        use_openai_fallback: bool = True,
    ) -> Dict[str, Optional[str]]:
        """Public API compatible with OpenAIManager: returns only the flat result dict."""
        result, _meta = self.classify_with_meta(
            title=title,
            description=description,
            image_url=image_url,
            use_openai_fallback=use_openai_fallback,
        )
        return result

    # ---------------------------
    # Internal helpers
    # ---------------------------

    @staticmethod
    def _mk_text(title: str, description: str) -> str:
        t = (title or "").strip()
        d = (description or "").strip()
        return f"{t} {d}".strip()

    @staticmethod
    def _predict_one(pipe, classes, title: str, description: str) -> Tuple[str, float]:
        """
        Your training pipeline expects a 1-D iterable of raw text strings.
        Build 'title + description' and send [combined] to the pipeline.
        """
        combined = f"{(title or '').strip()} {(description or '').strip()}".strip()
        logger.debug(f"Predict: combined_text_len={len(combined)} | preview='{_preview(combined)}'")

        t0 = time.perf_counter()
        proba = pipe.predict_proba([combined])  # shape [1, C]
        dt_ms = (time.perf_counter() - t0) * 1000.0

        idx = int(np.argmax(proba[0]))
        label = str(classes[idx])
        conf  = float(proba[0, idx])
        top = _topk(proba[0], classes, k=3)

        logger.info(f"Predict: top1='{label}' conf={conf:.4f} | top3={[(l, round(p,4)) for l,p in top]} | latency_ms={dt_ms:.1f}")
        return label, conf

    @staticmethod
    def _load_global_tau(path: Optional[str], default_tau: float) -> float:
        """
        Loads a single float value from a JSON file or returns default.
        Accepts either {"tau": 0.9} or a plain float (e.g., 0.9).
        Quiet when file is missing.
        """
        if not path or not os.path.exists(path):
            return float(default_tau)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and "tau" in data:
                return float(data["tau"])
            if isinstance(data, (int, float)):
                return float(data)
        except Exception as e:
            logger.debug(f"MLManager: global tau load fallback ({path}): {e}")
        return float(default_tau)
