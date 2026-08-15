"""Targeted database backfills through the registered classification pipeline.

The command is intentionally dry-run (shadow) by default.  Database writes and
remote embeddings each require an explicit flag so scheduled or manual runs
cannot silently widen their behavior.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable

from psycopg2 import connect, sql
from psycopg2.extras import RealDictCursor

from .client import ClassificationPipelineClient
from .registry import ModelRegistry


ALLOWED_UPDATE_COLUMNS = {
    "conflict_ml_designated",
    "nation_ml_designated",
    "item_type_ml_designated",
    "sub_item_type_ml_designated",
    "mil_branch_ml_designated",
}

SELECT_COLUMNS = (
    "id", "url", "title", "description", "s3_image_urls", "original_image_urls",
    "user_confirmed_conflict", "canonical_conflict", "conflict_ml_designated",
    "conflict_ai_generated", "conflict_site_designated",
    "user_confirmed_nation", "canonical_nation", "nation_ml_designated",
    "nation_ai_generated", "nation_site_designated",
    "user_confirmed_item_type", "canonical_item_type", "item_type_ml_designated",
    "item_type_ai_generated", "item_type_site_designated",
    "user_confirmed_sub_item_type", "sub_item_type_ml_designated",
    "sub_item_type_ai_designated", "user_confirmed_mil_branch",
    "mil_branch_ml_designated", "mil_branch_ai_designated", "date_modified",
)


def _usable(value: Any) -> bool:
    return str(value or "").strip().casefold() not in {"", "unknown", "none", "null"}


def _effective(row: dict[str, Any], fields: Iterable[str]) -> str:
    for field in fields:
        value = row.get(field)
        if _usable(value):
            return str(value).strip()
    return ""


def _image_urls(*values: Any) -> list[str]:
    urls: list[str] = []
    for value in values:
        if not value:
            continue
        parsed = value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except (TypeError, ValueError):
                parsed = [value]
        if isinstance(parsed, dict):
            parsed = list(parsed.values())
        if not isinstance(parsed, (list, tuple, set)):
            parsed = [parsed]
        for url in parsed:
            text = str(url or "").strip()
            if text and text not in urls:
                urls.append(text)
    return urls
def _context(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "product_id": row["id"],
        "url": row.get("url") or "",
        "title": row.get("title") or "",
        "description": row.get("description") or "",
        "image_urls": _image_urls(row.get("s3_image_urls"), row.get("original_image_urls")),
        "conflict": _effective(row, (
            "user_confirmed_conflict", "canonical_conflict", "conflict_ml_designated",
            "conflict_ai_generated", "conflict_site_designated",
        )),
        "nation": _effective(row, (
            "user_confirmed_nation", "canonical_nation", "nation_ml_designated",
            "nation_ai_generated", "nation_site_designated",
        )),
        "item_type": _effective(row, (
            "user_confirmed_item_type", "canonical_item_type", "item_type_ml_designated",
            "item_type_ai_generated", "item_type_site_designated",
        )),
        "sub_item_type": _effective(row, (
            "user_confirmed_sub_item_type", "sub_item_type_ml_designated",
            "sub_item_type_ai_designated",
        )),
        "mil_branch": _effective(row, (
            "user_confirmed_mil_branch", "mil_branch_ml_designated",
            "mil_branch_ai_designated",
        )),
        "user_confirmed_conflict": row.get("user_confirmed_conflict"),
        "user_confirmed_nation": row.get("user_confirmed_nation"),
        "user_confirmed_item_type": row.get("user_confirmed_item_type"),
        "user_confirmed_sub_item_type": row.get("user_confirmed_sub_item_type"),
        "user_confirmed_mil_branch": row.get("user_confirmed_mil_branch"),
        "sub_item_type_ml_designated": row.get("sub_item_type_ml_designated"),
        "mil_branch_ml_designated": row.get("mil_branch_ml_designated"),
    }


def _load_db_credentials(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    return {
        "user": payload.get("userName"),
        "password": payload.get("pwd"),
        "host": payload.get("hostName"),
        "dbname": payload.get("dataBase"),
        "port": payload.get("portId"),
        "connect_timeout": 15,
    }


def _missing_clause(target: str) -> str:
    subtype = (
        "LOWER(COALESCE(NULLIF(BTRIM(user_confirmed_sub_item_type), ''), "
        "NULLIF(BTRIM(sub_item_type_ml_designated), ''), "
        "NULLIF(BTRIM(sub_item_type_ai_designated), ''), '')) "
        "IN ('', 'unknown', 'none', 'null')"
    )
    branch = (
        "LOWER(COALESCE(NULLIF(BTRIM(user_confirmed_mil_branch), ''), "
        "NULLIF(BTRIM(mil_branch_ml_designated), ''), "
        "NULLIF(BTRIM(mil_branch_ai_designated), ''), '')) "
        "IN ('', 'unknown', 'none', 'null')"
    )
    if target == "subtype":
        return subtype
    if target == "branch":
        return branch
    if target == "both":
        return f"({subtype} AND {branch})"
    return f"({subtype} OR {branch})"


def _fetch_candidates(connection, args) -> list[dict[str, Any]]:
    where = [
        "LOWER(COALESCE(NULLIF(BTRIM(canonical_conflict), ''), "
        "NULLIF(BTRIM(conflict_ml_designated), ''), '')) = LOWER(%s)",
        "LOWER(COALESCE(NULLIF(BTRIM(canonical_nation), ''), "
        "NULLIF(BTRIM(nation_ml_designated), ''), '')) = LOWER(%s)",
        "LOWER(COALESCE(NULLIF(BTRIM(canonical_item_type), ''), "
        "NULLIF(BTRIM(item_type_ml_designated), ''), '')) = LOWER(%s)",
        _missing_clause(args.missing),
    ]
    params: list[Any] = [args.conflict, args.nation, args.item_type]
    if args.ids:
        where.append("id = ANY(%s)")
        params.append(args.ids)
    params.append(min(10_000, max(args.limit * 20, args.limit)))
    statement = sql.SQL(
        "SELECT {columns} FROM militaria WHERE {where} "
        "ORDER BY date_modified DESC NULLS LAST, id DESC LIMIT %s"
    ).format(
        columns=sql.SQL(", ").join(map(sql.Identifier, SELECT_COLUMNS)),
        where=sql.SQL(" AND ").join(sql.SQL(part) for part in where),
    )
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(statement, params)
        return [dict(row) for row in cursor.fetchall()]


def _store_updates(connection, product_id: int, updates: dict[str, str]) -> None:
    invalid = set(updates) - ALLOWED_UPDATE_COLUMNS
    if invalid:
        raise ValueError(f"Refusing unexpected update columns: {sorted(invalid)}")
    assignments = sql.SQL(", ").join(
        sql.SQL("{} = %s").format(sql.Identifier(column)) for column in updates
    )
    statement = sql.SQL("UPDATE militaria SET {assignments} WHERE id = %s").format(
        assignments=assignments
    )
    with connection.cursor() as cursor:
        cursor.execute(statement, [*updates.values(), product_id])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-credentials", required=True)
    parser.add_argument("--registry", required=True)
    parser.add_argument("--worker-python", required=True)
    parser.add_argument("--openai-credentials", default="")
    parser.add_argument("--audit-dir", default="milivault_logs/classification")
    parser.add_argument("--conflict", default="WW2")
    parser.add_argument("--nation", default="GERMANY")
    parser.add_argument("--item-type", default="helmets_accessories")
    parser.add_argument("--missing", choices=("either", "both", "subtype", "branch"), default="either")
    parser.add_argument("--ids", type=lambda value: [int(item) for item in value.split(",")], default=[])
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--allow-remote-embeddings", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.limit < 1 or args.limit > 1000:
        raise SystemExit("--limit must be between 1 and 1000")
    if args.allow_remote_embeddings and not args.openai_credentials:
        raise SystemExit("--openai-credentials is required with --allow-remote-embeddings")

    settings = {
        "enableClassificationPipeline": True,
        "classificationPipelineMode": "apply" if args.apply else "shadow",
        "classificationAllowRemoteEmbeddings": args.allow_remote_embeddings,
        "classificationRegistry": args.registry,
        "classificationPython": args.worker_python,
        "classificationAuditDir": args.audit_dir,
        "openaiCred": args.openai_credentials,
    }
    client = ClassificationPipelineClient(settings)
    connection = connect(**_load_db_credentials(args.db_credentials))
    connection.autocommit = True
    registry = ModelRegistry(args.registry)
    summary = {
        "selected": 0, "scope_filtered": 0, "updated": 0,
        "unchanged": 0, "errors": 0, "mode": client.mode,
    }
    try:
        fetched = _fetch_candidates(connection, args)
        candidates = []
        for row in fetched:
            context = _context(row)
            if any(stage.matches_scope(context) for stage in registry.enabled_stages()):
                candidates.append(row)
                if len(candidates) == args.limit:
                    break
            else:
                summary["scope_filtered"] += 1
        summary["selected"] = len(candidates)
        for row in candidates:
            try:
                updates = client.classify_product(_context(row))
                if updates and args.apply:
                    _store_updates(connection, int(row["id"]), updates)
                    summary["updated"] += 1
                    print(json.dumps({"id": row["id"], "updates": updates}, sort_keys=True))
                else:
                    summary["unchanged"] += 1
                    print(json.dumps({"id": row["id"], "updates": {}}, sort_keys=True))
            except Exception as exc:
                summary["errors"] += 1
                print(json.dumps({"id": row["id"], "error": f"{type(exc).__name__}: {exc}"}))
    finally:
        client.close()
        connection.close()
    print(json.dumps({"summary": summary}, sort_keys=True))
    return 1 if summary["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
