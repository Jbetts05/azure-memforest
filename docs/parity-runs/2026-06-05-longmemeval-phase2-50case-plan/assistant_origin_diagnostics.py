from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_WORD = re.compile(r"[a-z0-9]+", re.IGNORECASE)
_STOP_WORDS = {
    "a",
    "an",
    "and",
    "at",
    "by",
    "for",
    "from",
    "in",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
}
DEFAULT_NEEDLES = {
    "1de5cff2": ["veja", "wild rubber", "amazon"],
    "6ae235be": ["atmospheric distillation", "fluid catalytic", "alkylation", "hydrotreating"],
    "70b3e69b": ["manolo", "garcia"],
    "dc439ea3": ["hoop dance", "hoop"],
    "cc539528": ["ruby", "python", "php"],
    "8464fc84": ["roscioli"],
}


def diagnose_artifact_cases(results_dir: Path, *, fact_docs: dict[str, list[dict[str, Any]]] | None = None) -> dict[str, Any]:
    rows = []
    for path in sorted(results_dir.glob("[0-9][0-9]_*.json")):
        result = json.loads(path.read_text(encoding="utf-8"))
        case_id = str(result.get("case_id") or "")
        if case_id not in DEFAULT_NEEDLES:
            continue
        user_id = _case_user_id(result)
        topk_facts = _query_facts(result)
        store_facts = list((fact_docs or {}).get(user_id, []))
        rows.append(
            diagnose_case(
                result=result,
                needles=DEFAULT_NEEDLES[case_id],
                topk_facts=topk_facts,
                store_facts=store_facts,
                store_inspected=user_id in (fact_docs or {}),
            )
        )
    return {
        "case_count": len(rows),
        "store_inspected_count": sum(1 for row in rows if row["store_inspected"]),
        "classification_counts": _counts(row["classification"] for row in rows),
        "cases": rows,
    }


def diagnose_case(
    *,
    result: dict[str, Any],
    needles: list[str],
    topk_facts: list[dict[str, Any]],
    store_facts: list[dict[str, Any]],
    store_inspected: bool,
) -> dict[str, Any]:
    topk_hits = _needle_hits(needles, topk_facts)
    store_hits = _needle_hits(needles, store_facts) if store_inspected else {}
    relaxed_topk_hits = _relaxed_needle_hits(needles, topk_facts)
    relaxed_store_hits = _relaxed_needle_hits(needles, store_facts) if store_inspected else {}
    topk_hit_count = sum(1 for hit in topk_hits.values() if hit)
    store_hit_count = sum(1 for hit in store_hits.values() if hit) if store_inspected else 0
    relaxed_topk_hit_count = sum(1 for hit in relaxed_topk_hits.values() if hit)
    relaxed_store_hit_count = sum(1 for hit in relaxed_store_hits.values() if hit) if store_inspected else 0
    if topk_hits and topk_hit_count == len(topk_hits):
        classification = "gold_evidence_in_topk"
    elif topk_hit_count > 0:
        classification = "partial_gold_evidence_in_topk"
    elif store_inspected and store_hits and store_hit_count == len(store_hits):
        classification = "gold_evidence_in_store_not_topk"
    elif store_inspected and store_hit_count > 0:
        classification = "partial_gold_evidence_in_store"
    elif store_inspected and relaxed_store_hits and relaxed_store_hit_count == len(relaxed_store_hits):
        classification = "relaxed_gold_evidence_in_store"
    elif store_inspected and relaxed_store_hit_count > 0:
        classification = "partial_relaxed_gold_evidence_in_store"
    elif store_inspected:
        classification = "gold_evidence_not_lexically_found_in_store"
    else:
        classification = "store_not_inspected"
    input_shaping = [event.get("input_shaping") or {} for event in result.get("events") or [] if event.get("input_shaping")]
    return {
        "case_id": result.get("case_id"),
        "benchmark_user_id": result.get("benchmark_user_id") or _case_user_id(result),
        "primary_correct": bool(((result.get("judges") or {}).get("primary") or {}).get("label")),
        "answer": result.get("answer"),
        "needles": needles,
        "topk_fact_count": len(topk_facts),
        "store_fact_count": len(store_facts) if store_inspected else None,
        "topk_hits": topk_hits,
        "store_hits": store_hits if store_inspected else None,
        "relaxed_topk_hits": relaxed_topk_hits,
        "relaxed_store_hits": relaxed_store_hits if store_inspected else None,
        "topk_hit_count": topk_hit_count,
        "store_hit_count": store_hit_count if store_inspected else None,
        "relaxed_topk_hit_count": relaxed_topk_hit_count,
        "relaxed_store_hit_count": relaxed_store_hit_count if store_inspected else None,
        "store_inspected": store_inspected,
        "classification": classification,
        "assistant_turns_extractable": sum(int(item.get("assistant_turns_extractable") or 0) for item in input_shaping),
        "assistant_turns_marked_skip_extraction": sum(int(item.get("assistant_turns_marked_skip_extraction") or 0) for item in input_shaping),
    }


async def fetch_cosmos_fact_docs(results_dir: Path, *, endpoint: str, database_name: str) -> dict[str, list[dict[str, Any]]]:
    from azure.cosmos.aio import CosmosClient
    from azure.identity.aio import DefaultAzureCredential
    from src.cloud.schema import partition_key

    users = _users_by_tenant(results_dir)
    credential = DefaultAzureCredential()
    client = CosmosClient(endpoint, credential=credential)
    try:
        container = client.get_database_client(database_name).get_container_client("facts")
        docs: dict[str, list[dict[str, Any]]] = {}
        for tenant_id, user_ids in users.items():
            for user_id in user_ids:
                pk = partition_key(tenant_id=tenant_id, user_id=user_id)
                query = (
                    "SELECT c.fact_id, c.fact_text, c.time_start, c.time_end, c.metadata "
                    "FROM c WHERE c.tenant_user_key=@pk "
                    "AND (NOT IS_DEFINED(c.tombstoned) OR c.tombstoned=false)"
                )
                params = [{"name": "@pk", "value": pk}]
                rows = []
                async for item in container.query_items(query=query, parameters=params, partition_key=pk):
                    rows.append(item)
                docs[user_id] = rows
        return docs
    finally:
        await client.close()
        await credential.close()


def _users_by_tenant(results_dir: Path) -> dict[str, set[str]]:
    users: dict[str, set[str]] = {}
    for path in sorted(results_dir.glob("[0-9][0-9]_*.json")):
        result = json.loads(path.read_text(encoding="utf-8"))
        tenant_id = _case_tenant_id(result)
        user_id = _case_user_id(result)
        if tenant_id and user_id:
            users.setdefault(tenant_id, set()).add(user_id)
    return users


def _case_tenant_id(result: dict[str, Any]) -> str:
    for event in result.get("events") or []:
        status = event.get("status")
        if isinstance(status, dict) and status.get("tenant_id"):
            return str(status["tenant_id"])
    return ""


def _case_user_id(result: dict[str, Any]) -> str:
    if result.get("benchmark_user_id"):
        return str(result["benchmark_user_id"])
    for event in result.get("events") or []:
        status = event.get("status")
        if isinstance(status, dict) and status.get("user_id"):
            return str(status["user_id"])
    return ""


def _query_facts(result: dict[str, Any]) -> list[dict[str, Any]]:
    facts = []
    for event in result.get("events") or []:
        if event.get("step") == "query":
            facts.extend(list((event.get("response") or {}).get("facts") or []))
    return facts


def _needle_hits(needles: list[str], facts: list[dict[str, Any]]) -> dict[str, bool]:
    text = "\n".join(_fact_text(fact) for fact in facts)
    return {needle: _contains_phrase(text, needle) for needle in needles}


def _relaxed_needle_hits(needles: list[str], facts: list[dict[str, Any]]) -> dict[str, bool]:
    text_tokens = set(_normalize("\n".join(_fact_text(fact) for fact in facts)).split())
    return {needle: _contains_relaxed_tokens(text_tokens, needle) for needle in needles}


def _fact_text(fact: dict[str, Any]) -> str:
    parts = [str(fact.get(key) or "") for key in ("fact_text", "text", "content", "fact", "summary")]
    metadata = fact.get("metadata")
    if metadata:
        parts.append(json.dumps(metadata, ensure_ascii=False, sort_keys=True))
    return " ".join(parts)


def _contains_phrase(text: str, phrase: str) -> bool:
    return f" {_normalize(phrase)} " in f" {_normalize(text)} "


def _contains_relaxed_tokens(text_tokens: set[str], phrase: str) -> bool:
    phrase_tokens = {
        token
        for token in _normalize(phrase).split()
        if token not in _STOP_WORDS and len(token) >= 2
    }
    return bool(phrase_tokens) and phrase_tokens <= text_tokens


def _normalize(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return " ".join(match.group(0).lower() for match in _WORD.finditer(text))


def _counts(values) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


async def _main_async(args: argparse.Namespace) -> dict[str, Any]:
    fact_docs = None
    if args.cosmos:
        endpoint = args.cosmos_endpoint or os.environ.get("AZURE_COSMOS_ENDPOINT", "")
        database_name = args.cosmos_database or os.environ.get("AZURE_COSMOS_DATABASE_NAME", "")
        if not endpoint or not database_name:
            raise RuntimeError("Cosmos diagnostics require --cosmos-endpoint/--cosmos-database or matching env vars.")
        fact_docs = await fetch_cosmos_fact_docs(args.results_dir, endpoint=endpoint, database_name=database_name)
    return diagnose_artifact_cases(args.results_dir, fact_docs=fact_docs)


def main() -> None:
    parser = argparse.ArgumentParser(description="Classify assistant-origin misses using top-k artifacts and optional Cosmos facts.")
    parser.add_argument("results_dir", type=Path)
    parser.add_argument("--cosmos", action="store_true", help="Inspect the full Cosmos facts container for each benchmark user.")
    parser.add_argument("--cosmos-endpoint", default="")
    parser.add_argument("--cosmos-database", default="")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = asyncio.run(_main_async(args))
    rendered = json.dumps(result, indent=2, ensure_ascii=False)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)


if __name__ == "__main__":
    main()
