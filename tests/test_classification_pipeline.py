from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from classification_pipeline.client import ClassificationPipelineClient
from classification_pipeline.backfill import _image_urls
from classification_pipeline.core import ClassificationPipeline
from classification_pipeline.plugins import ww2_german_helmet_service
from classification_pipeline.registry import ModelRegistry, RegistryError


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def constant_manifest(model_id, order, target_field, ml_column, human_column, value, scope, depends=None):
    return {
        "schema_version": 1,
        "model_id": model_id,
        "version": "1",
        "enabled": True,
        "order": order,
        "depends_on": depends or [],
        "scope": scope,
        "target": {
            "field": target_field,
            "ml_column": ml_column,
            "human_column": human_column,
        },
        "runtime": {"kind": "constant", "value": value},
        "acceptance": {"minimum_confidence": 1.0},
    }


class ClassificationPipelineTests(unittest.TestCase):
    def build_registry(self, root: Path):
        subtype = constant_manifest(
            "test_subtype_v1", 100, "sub_item_type",
            "sub_item_type_ml_designated", "user_confirmed_sub_item_type",
            "helmet", {"conflict": ["WW2"], "nation": ["GERMANY"]},
        )
        branch = constant_manifest(
            "test_branch_v1", 200, "mil_branch",
            "mil_branch_ml_designated", "user_confirmed_mil_branch",
            "army", {"sub_item_type": ["helmet"]}, ["test_subtype_v1"],
        )
        write_json(root / "subtype.json", subtype)
        write_json(root / "branch.json", branch)
        write_json(root / "registry.json", {
            "schema_version": 1,
            "models": ["branch.json", "subtype.json"],
        })
        return root / "registry.json"

    def test_registry_orders_declared_dependencies(self):
        with tempfile.TemporaryDirectory() as directory:
            registry = ModelRegistry(self.build_registry(Path(directory)))
            self.assertEqual(
                [stage.model_id for stage in registry.stages],
                ["test_subtype_v1", "test_branch_v1"],
            )

    def test_pipeline_chains_stage_outputs_into_later_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            pipeline = ClassificationPipeline(self.build_registry(Path(directory)))
            result = pipeline.classify_product({
                "product_id": 10,
                "conflict": "ww2",
                "nation": "germany",
                "sub_item_type": "",
                "mil_branch": "",
            })
            self.assertEqual(result.updates, {
                "sub_item_type_ml_designated": "helmet",
                "mil_branch_ml_designated": "army",
            })
            self.assertEqual([event["status"] for event in result.events], ["applied", "applied"])

    def test_pipeline_preserves_human_target(self):
        with tempfile.TemporaryDirectory() as directory:
            pipeline = ClassificationPipeline(self.build_registry(Path(directory)))
            result = pipeline.classify_product({
                "product_id": 11,
                "conflict": "WW2",
                "nation": "GERMANY",
                "sub_item_type": "helmet",
                "mil_branch": "navy",
                "user_confirmed_sub_item_type": "helmet",
                "user_confirmed_mil_branch": "navy",
            })
            self.assertEqual(result.updates, {})
            self.assertEqual([event["status"] for event in result.events], [
                "human_preserved", "human_preserved",
            ])

    def test_pipeline_scope_skips_unrelated_products(self):
        with tempfile.TemporaryDirectory() as directory:
            pipeline = ClassificationPipeline(self.build_registry(Path(directory)))
            result = pipeline.classify_product({
                "product_id": 12,
                "conflict": "WW1",
                "nation": "UNITED STATES",
                "sub_item_type": "",
                "mil_branch": "",
            })
            self.assertEqual(result.updates, {})
            self.assertTrue(all(event["status"] == "scope_skipped" for event in result.events))

    def test_pipeline_scope_excludes_explicit_wwi_text(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subtype = constant_manifest(
                "test_subtype_v1", 100, "sub_item_type",
                "sub_item_type_ml_designated", "user_confirmed_sub_item_type",
                "helmet", {"conflict": ["WW2"], "nation": ["GERMANY"]},
            )
            subtype["scope_exclusions"] = {"text_regex": [r"\b(?:ww1|wo1)\b"]}
            write_json(root / "subtype.json", subtype)
            write_json(root / "registry.json", {
                "schema_version": 1,
                "models": ["subtype.json"],
            })
            pipeline = ClassificationPipeline(root / "registry.json")
            result = pipeline.classify_product({
                "product_id": 14,
                "title": "WO1 Duitse helm",
                "conflict": "WW2",
                "nation": "GERMANY",
                "sub_item_type": "",
            })
            self.assertEqual(result.updates, {})
            self.assertEqual(result.events[0]["status"], "scope_skipped")

    def test_backfill_image_urls_accepts_json_and_deduplicates(self):
        self.assertEqual(
            _image_urls('["https://example.test/a.jpg"]', [
                "https://example.test/a.jpg", "https://example.test/b.jpg",
            ]),
            ["https://example.test/a.jpg", "https://example.test/b.jpg"],
        )

    def test_service_plugin_keeps_no_decal_broad(self):
        decision = ww2_german_helmet_service.pre_predict({
            "title": "Original German M40 no decal combat helmet",
            "description": "",
            "sub_item_type": "helmet",
        }, {})
        self.assertEqual(decision.label, "wehrmacht_or_waffen_ss")
        self.assertEqual(decision.source, "no_decal_armed_service_fallback")

    def test_shadow_client_never_returns_production_updates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry = self.build_registry(root)
            settings = {
                "enableClassificationPipeline": True,
                "classificationPipelineMode": "shadow",
                "classificationAllowRemoteEmbeddings": False,
                "classificationRegistry": str(registry),
                "classificationPython": str(Path(__file__)),
                "classificationAuditDir": str(root / "audit"),
            }
            client = ClassificationPipelineClient(settings)
            response = {
                "result": {
                    "updates": {"mil_branch_ml_designated": "army"},
                    "events": [{"status": "applied", "product_id": 13}],
                }
            }
            with patch.object(client, "_request", return_value=response):
                self.assertEqual(client.classify_product({"product_id": 13}), {})
            audit_text = "".join(path.read_text(encoding="utf-8") for path in (root / "audit").iterdir())
            self.assertIn("shadow_proposed_updates", audit_text)

    def test_registry_rejects_unknown_dependency(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = constant_manifest(
                "orphan_model_v1", 100, "mil_branch",
                "mil_branch_ml_designated", "user_confirmed_mil_branch",
                "army", {}, ["missing_model"],
            )
            write_json(root / "orphan.json", manifest)
            write_json(root / "registry.json", {"schema_version": 1, "models": ["orphan.json"]})
            with self.assertRaises(RegistryError):
                ModelRegistry(root / "registry.json")


if __name__ == "__main__":
    unittest.main()
