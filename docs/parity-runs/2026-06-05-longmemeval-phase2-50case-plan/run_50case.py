from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import re
import time
import urllib.request
from pathlib import Path
from typing import Any


RUN = Path(__file__).resolve().parent
PILOT_RUN = RUN.parent / "2026-06-03-longmemeval-phase2-intended-pilot" / "run_pilot.py"
SOURCE_URL = "https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/main/longmemeval_s_cleaned.json"
SOURCE_SHA256 = "d6f21ea9d60a0d56f34a05b609c79c88a451d2ae03597821ea3d5a9678c3a442"
CASE_INDEX = json.loads((RUN / "cases_index.json").read_text(encoding="utf-8"))
MANIFEST = json.loads((RUN / "manifest.json").read_text(encoding="utf-8"))

SHARD = os.getenv("PHASE2_50_SHARD", "").strip()
ALLOW_LIVE = os.getenv("PHASE2_50_ALLOW_LIVE", "").strip().lower() in {"1", "true", "yes", "on"}
PREFLIGHT_ONLY = os.getenv("PHASE2_50_PREFLIGHT_ONLY", "").strip().lower() in {"1", "true", "yes", "on"}
AGGREGATE_DIRS = [item.strip() for item in os.getenv("PHASE2_50_AGGREGATE_DIRS", "").split(";") if item.strip()]
RUN_SUFFIX = os.getenv("PHASE2_RUN_SUFFIX", "").strip()
RESULTS = Path(os.getenv("PHASE2_RESULTS_DIR", str(RUN / "case_results")))
PRO_JUDGE_MAX_OUTPUT_TOKENS = int(os.getenv("PHASE2_50_PRO_JUDGE_MAX_OUTPUT_TOKENS", "2048"))
PRO_JUDGE_SAMPLE_SIZE = int(os.getenv("PHASE2_50_PRO_JUDGE_SAMPLE_SIZE", "2"))
OFFICIAL_SHARD_SIZE = int(os.getenv("PHASE2_OFFICIAL_SHARD_SIZE", "10"))
SESSION_POLICY = os.getenv("PHASE2_SESSION_POLICY", "bounded").strip().lower()


def _load_pilot_module():
    spec = importlib.util.spec_from_file_location("phase2_pilot_harness", PILOT_RUN)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load pilot harness from {PILOT_RUN}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


pilot = _load_pilot_module()


def save(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2), encoding="utf-8")


def _source_cases() -> dict[str, dict[str, Any]]:
    raw = urllib.request.urlopen(SOURCE_URL, timeout=120).read()
    digest = hashlib.sha256(raw).hexdigest()
    if digest != SOURCE_SHA256:
        raise RuntimeError(f"source SHA mismatch: expected {SOURCE_SHA256}, got {digest}")
    return {str(item["question_id"]): item for item in json.loads(raw)}


def _normalize_case(item: dict[str, Any]) -> dict[str, Any]:
    answer_ids = set(item.get("answer_session_ids") or [])
    selected_indexes = _selected_session_indexes(item, answer_ids=answer_ids)

    sessions = []
    selected_turn_count = 0
    for source_index in selected_indexes:
        source_id = str(item["haystack_session_ids"][source_index])
        turns = []
        for turn_index, turn in enumerate(item["haystack_sessions"][source_index]):
            copied = dict(turn)
            copied["content_id"] = f"{source_id}:{turn_index}"
            copied["metadata"] = {"source": "longmemeval-phase2-50case-plan"}
            turns.append(copied)
        selected_turn_count += len(turns)
        sessions.append(
            {
                "session_id": f"lme-50case-{item['question_id']}-{source_id}",
                "source_session_id": source_id,
                "turns": turns,
                "metadata": {
                    "date": item["haystack_dates"][source_index],
                    "is_answer_session": source_id in answer_ids,
                },
            }
        )
    return {
        "case_id": str(item["question_id"]),
        "question_type": str(item["question_type"]),
        "question": item["question"],
        "question_date": item["question_date"],
        "gold_answer": item["answer"],
        "answer_session_ids": list(item.get("answer_session_ids") or []),
        "selected_session_count": len(sessions),
        "selected_turn_count": selected_turn_count,
        "total_haystack_sessions": len(item.get("haystack_sessions") or []),
        "total_haystack_turns": sum(len(session) for session in item.get("haystack_sessions") or []),
        "selection_strategy": _selection_strategy_text(),
        "session_policy": SESSION_POLICY,
        "sessions": sessions,
    }


def _selected_session_indexes(item: dict[str, Any], *, answer_ids: set[str]) -> list[int]:
    haystack_sessions = item.get("haystack_sessions") or []
    if SESSION_POLICY == "full":
        return list(range(len(haystack_sessions)))
    if SESSION_POLICY != "bounded":
        raise RuntimeError("PHASE2_SESSION_POLICY must be one of: bounded, full")
    selected_indexes = list(range(min(8, len(haystack_sessions))))
    selected_source_ids = {item["haystack_session_ids"][index] for index in selected_indexes}
    for index, source_id in enumerate(item.get("haystack_session_ids") or []):
        if source_id in answer_ids and source_id not in selected_source_ids:
            selected_indexes.append(index)
            selected_source_ids.add(source_id)
    index = 8
    while len(selected_indexes) < 9 and index < len(haystack_sessions):
        if index not in selected_indexes:
            selected_indexes.append(index)
        index += 1
    return sorted(selected_indexes)


def selected_case_ids(source: dict[str, dict[str, Any]] | None = None) -> list[str]:
    if _is_official_shard():
        _assert_official_session_policy()
    if SHARD == "regression":
        return list(CASE_INDEX["pilot_regression_case_ids"])
    if SHARD in CASE_INDEX["shards"]:
        return list(CASE_INDEX["shards"][SHARD])
    if SHARD == "all-heldout":
        return list(CASE_INDEX["heldout_case_ids"])
    if SHARD == "official-all":
        if source is None:
            raise RuntimeError("official-all selection requires loaded source cases")
        _assert_official_source_complete(source)
        return list(source.keys())
    if SHARD.startswith("official_"):
        if source is None:
            raise RuntimeError("official shard selection requires loaded source cases")
        _assert_official_source_complete(source)
        shard_index = _official_shard_index(SHARD)
        case_ids = list(source.keys())
        start = (shard_index - 1) * OFFICIAL_SHARD_SIZE
        selected = case_ids[start : start + OFFICIAL_SHARD_SIZE]
        if not selected:
            raise RuntimeError(f"official shard {SHARD} is empty for shard size {OFFICIAL_SHARD_SIZE}")
        return selected
    raise RuntimeError(
        "Set PHASE2_50_SHARD to one of regression, all-heldout, official-all, official_N, "
        + ", ".join(sorted(CASE_INDEX["shards"]))
    )


def load_cases() -> list[dict[str, Any]]:
    source = _source_cases()
    selected_ids = selected_case_ids(source)
    if pilot.CASE_FILTER:
        selected_ids = [case_id for case_id in selected_ids if case_id in pilot.CASE_FILTER]
    missing = [case_id for case_id in selected_ids if case_id not in source]
    if missing:
        raise RuntimeError(f"missing source cases: {missing}")
    return [_normalize_case(source[case_id]) for case_id in selected_ids]


def _official_shard_index(value: str) -> int:
    raw = value.removeprefix("official_").strip()
    if not raw.isdigit():
        raise RuntimeError(f"official shard must use form official_N, got {value!r}")
    shard_index = int(raw)
    if shard_index <= 0:
        raise RuntimeError(f"official shard index must be positive, got {value!r}")
    return shard_index


def _assert_official_source_complete(source: dict[str, dict[str, Any]]) -> None:
    expected = int(MANIFEST["source"]["total_cases"])
    if len(source) != expected:
        raise RuntimeError(f"official source expected {expected} unique cases, got {len(source)}")


def _assert_official_session_policy() -> None:
    if SESSION_POLICY != "full":
        raise RuntimeError("official LongMemEval-S runs require PHASE2_SESSION_POLICY=full")


def strict_preflight_snapshot() -> dict[str, Any]:
    snapshot = pilot.strict_preflight_snapshot()
    pro = MANIFEST["judge_policy"]["judge_control"]
    deployment = _deployment(pro["resource_group"], pro["resource"], pro["deployment"])
    if deployment["properties"]["provisioningState"] != "Succeeded":
        raise RuntimeError(f"pro judge deployment is not ready: {deployment['properties']['provisioningState']}")
    capabilities = deployment["properties"].get("capabilities") or {}
    if capabilities.get("responses") != "true":
        raise RuntimeError("pro judge deployment does not advertise Responses API support")
    snapshot["pro_judge"] = {
        "deployment": pro["deployment"],
        "resource": pro["resource"],
        "api_surface": pro["api_surface"],
        "api_version": pro["responses_api_version"],
        "capacity": deployment["sku"]["capacity"],
        "capabilities": capabilities,
    }
    return snapshot


def _deployment(resource_group: str, account_name: str, deployment_name: str) -> dict[str, Any]:
    raw = pilot.subprocess.check_output(
        [
            pilot.AZ,
            "cognitiveservices",
            "account",
            "deployment",
            "show",
            "--resource-group",
            resource_group,
            "--name",
            account_name,
            "--deployment-name",
            deployment_name,
            "-o",
            "json",
        ],
        text=True,
    )
    return json.loads(raw)


def pro_judge(prompt: str) -> dict[str, Any]:
    control = MANIFEST["judge_policy"]["judge_control"]
    body = {"model": control["deployment"], "input": prompt, "max_output_tokens": PRO_JUDGE_MAX_OUTPUT_TOKENS}
    response = pilot.requests.post(
        f"{control['endpoint'].rstrip('/')}/openai/responses?api-version={control['responses_api_version']}",
        headers=pilot.model_headers(),
        json=body,
        timeout=180,
    )
    response.raise_for_status()
    payload = response.json()
    texts: list[str] = []
    for item in payload.get("output") or []:
        for content in item.get("content") or []:
            if content.get("type") == "output_text":
                texts.append(str(content.get("text") or ""))
    content = "\n".join(texts).strip()
    if payload.get("status") != "completed":
        raise RuntimeError(f"pro judge response did not complete: {payload.get('status')}")
    if not content:
        raise RuntimeError("pro judge response did not include visible output_text content")
    return {
        "deployment": control["deployment"],
        "api_surface": "responses",
        "api_version": control["responses_api_version"],
        "response": content,
        "label": yes_no_label(content),
        "raw": payload,
    }


def run_residue_reprobe(results: list[dict[str, Any]]) -> dict[str, Any]:
    probes = []
    previous: dict[str, Any] | None = None
    for result in results:
        if previous is None:
            previous = result
            continue
        user_id = result["benchmark_user_id"]
        response = pilot.residue_probe(previous["question"], user_id=user_id)
        answer = str((response.get("answer") if isinstance(response, dict) else "") or "")
        facts = response.get("facts") if isinstance(response, dict) else []
        facts = facts or []
        raw_nonrefusal = bool(answer and not any(marker in answer.lower() for marker in (
            "don't know",
            "do not know",
            "don't have information",
            "not enough",
            "no information",
            "not have information",
            "sorry",
            "cannot",
            "unable",
            "no relevant",
            "no record",
        )))
        previous_versions = _forest_versions(previous)
        returned_versions = sorted(
            {
                str((fact.get("metadata") or {}).get("forest_version"))
                for fact in facts
                if (fact.get("metadata") or {}).get("forest_version")
            }
        )
        provenance_contaminated = bool(set(returned_versions) & set(previous_versions))
        probes.append(
            {
                "case_id": result["case_id"],
                "user_id": user_id,
                "question": previous["question"],
                "response": response,
                "raw_nonrefusal_event": raw_nonrefusal,
                "returned_forest_versions": returned_versions,
                "previous_forest_versions": previous_versions,
                "provenance_contaminated": provenance_contaminated,
            }
        )
        previous = result
    return {
        "created_at_unix": time.time(),
        "scope": "adjacent-pair post-materialization reprobes for cases in this shard/cohort",
        "probe_count": len(probes),
        "raw_nonrefusal_events": sum(1 for item in probes if item["raw_nonrefusal_event"]),
        "provenance_contamination_events": sum(1 for item in probes if item["provenance_contaminated"]),
        "probes": probes,
    }


def _forest_versions(result: dict[str, Any]) -> list[str]:
    versions: set[str] = set()
    for event in result.get("events") or []:
        response = event.get("response") or {}
        for fact in response.get("facts") or []:
            version = (fact.get("metadata") or {}).get("forest_version")
            if version:
                versions.add(str(version))
    return sorted(versions)


def aggregate(results: list[dict[str, Any]], before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    base = pilot.aggregate(results, before, after)
    completed = [result for result in results if result.get("status") == "succeeded"]
    base["shard"] = SHARD
    base["cohort"] = _cohort_name()
    if base["cohort"] == "official_500":
        base["not_official_score"] = True
        base["not_official_score_reason"] = _official_not_score_reason(SESSION_POLICY)
        base["session_policy"] = SESSION_POLICY
    base["status"] = "shard_completed" if base.get("completed_cases") == base.get("attempted_cases") else "shard_incomplete"
    base["pilot_gate_status_ignored"] = True
    base["pro_judge_policy"] = MANIFEST["judge_policy"]["control_usage"]
    base["pro_judge_disagreements"] = [
        {
            "case_id": result["case_id"],
            "pro_judge": result.get("judges", {}).get("pro_judge"),
        }
        for result in completed
        if result.get("judges", {}).get("agreement") is False and result.get("judges", {}).get("pro_judge")
    ]
    return base


def aggregate_shards(paths: list[Path]) -> dict[str, Any]:
    aggregates = [json.loads((path / "aggregate.json").read_text(encoding="utf-8")) for path in paths]
    cohorts = {str(item.get("cohort") or "") for item in aggregates}
    if len(cohorts) > 1:
        raise RuntimeError(f"cannot aggregate mixed cohorts: {sorted(cohorts)}")
    official = [item for item in aggregates if item.get("cohort") == "official_500"]
    if official:
        return _aggregate_official_shards(official, aggregates)
    heldout = [item for item in aggregates if item.get("cohort") == "heldout_50"]
    attempted = sum(int(item.get("attempted_cases") or 0) for item in heldout)
    completed = sum(int(item.get("completed_cases") or 0) for item in heldout)
    primary_correct = sum(int(item.get("primary_judge_correct") or 0) for item in heldout)
    disagreements = sum(int(item.get("judge_disagreements") or 0) for item in heldout)
    dlq_delta = sum(int(item.get("new_dlq_count_delta") or 0) for item in heldout)
    durable_failed = sum(int(item.get("durable_instance_failed_count") or 0) for item in heldout)
    answer_blocks = sum(int(item.get("blocked_by_content_filter") or 0) for item in heldout)
    residue_events = sum(_residue_event_count(item) for item in heldout)
    raw_nonrefusal_events = sum(_raw_nonrefusal_event_count(item) for item in heldout)
    skipped = sum(int(item.get("filtered_skip_count") or 0) for item in heldout)
    gate = MANIFEST["gates"]["heldout_50"]
    operational = MANIFEST["gates"]["operational"]
    return {
        "status": "succeeded"
        if (
            attempted == gate["attempted_cases"]
            and completed == gate["completed_cases"]
            and primary_correct >= gate["primary_correct_min"]
            and disagreements <= gate["judge_disagreements_max"]
            and skipped <= gate["non_answer_content_filter_skips_max"]
            and dlq_delta == operational["new_dlq_count_delta"]
            and durable_failed <= operational["durable_failed_instances_max"]
            and answer_blocks == operational["answer_bearing_content_filter_blocks"]
            and raw_nonrefusal_events == operational["residue_raw_nonrefusal_events"]
            and residue_events == operational["residue_provenance_contamination_events"]
        )
        else "gate_failed",
        "cohort": "heldout_50",
        "shard_count": len(heldout),
        "attempted_cases": attempted,
        "completed_cases": completed,
        "primary_judge_correct": primary_correct,
        "judge_disagreements": disagreements,
        "filtered_skip_count": skipped,
        "new_dlq_count_delta": dlq_delta,
        "durable_instance_failed_count": durable_failed,
        "blocked_by_content_filter": answer_blocks,
        "residue_raw_nonrefusal_events": raw_nonrefusal_events,
        "residue_provenance_contamination_events": residue_events,
        "shards": aggregates,
    }


def _aggregate_official_shards(official: list[dict[str, Any]], aggregates: list[dict[str, Any]]) -> dict[str, Any]:
    session_policies = {str(item.get("session_policy") or "") for item in official}
    if session_policies != {"full"}:
        raise RuntimeError(f"official shard aggregates must all use session_policy=full, got {sorted(session_policies)}")
    attempted = sum(int(item.get("attempted_cases") or 0) for item in official)
    completed = sum(int(item.get("completed_cases") or 0) for item in official)
    primary_correct = sum(int(item.get("primary_judge_correct") or 0) for item in official)
    disagreements = sum(int(item.get("judge_disagreements") or 0) for item in official)
    dlq_delta = sum(int(item.get("new_dlq_count_delta") or 0) for item in official)
    durable_failed = sum(int(item.get("durable_instance_failed_count") or 0) for item in official)
    answer_blocks = sum(int(item.get("blocked_by_content_filter") or 0) for item in official)
    raw_nonrefusal_events = sum(_raw_nonrefusal_event_count(item) for item in official)
    residue_events = sum(_residue_event_count(item) for item in official)
    skipped = sum(int(item.get("filtered_skip_count") or 0) for item in official)
    return {
        "status": "completed" if attempted == 500 and completed == 500 else "incomplete",
        "cohort": "official_500",
        "not_official_score": True,
        "not_official_score_reason": _official_not_score_reason("full"),
        "session_policy": "full",
        "expected_cases": int(MANIFEST["source"]["total_cases"]),
        "shard_count": len(official),
        "attempted_cases": attempted,
        "completed_cases": completed,
        "primary_judge_correct": primary_correct,
        "primary_correct_rate": (primary_correct / attempted) if attempted else 0.0,
        "judge_disagreements": disagreements,
        "filtered_skip_count": skipped,
        "new_dlq_count_delta": dlq_delta,
        "durable_instance_failed_count": durable_failed,
        "blocked_by_content_filter": answer_blocks,
        "residue_raw_nonrefusal_events": raw_nonrefusal_events,
        "residue_provenance_contamination_events": residue_events,
        "shards": aggregates,
    }


def _residue_event_count(item: dict[str, Any]) -> int:
    if "residue_postrun_provenance_events" in item:
        return int(item["residue_postrun_provenance_events"] or 0)
    return int(item.get("residue_contamination_events") or 0)


def _raw_nonrefusal_event_count(item: dict[str, Any]) -> int:
    if "residue_postrun_raw_nonrefusal_events" not in item:
        raise RuntimeError("held-out shard aggregate is missing residue_postrun_raw_nonrefusal_events")
    return int(item["residue_postrun_raw_nonrefusal_events"] or 0)


def yes_no_label(content: str) -> bool:
    match = re.match(r"^\s*(yes|no)\b", content.strip(), flags=re.IGNORECASE)
    return bool(match and match.group(1).lower() == "yes")


def pro_judge_sample_ids(cases: list[dict[str, Any]]) -> set[str]:
    if PRO_JUDGE_SAMPLE_SIZE <= 0:
        return set()
    ranked = sorted(
        (case["case_id"] for case in cases),
        key=lambda case_id: hashlib.sha256((f"{CASE_INDEX['selection_seed']}:{SHARD}:{case_id}").encode("utf-8")).hexdigest(),
    )
    return set(ranked[: min(PRO_JUDGE_SAMPLE_SIZE, len(ranked))])


def _cohort_name() -> str:
    if SHARD == "regression":
        return "regression_10"
    if SHARD == "official-all" or SHARD.startswith("official_"):
        return "official_500"
    return "heldout_50"


def _official_not_score_reason(session_policy: str) -> str:
    if session_policy == "full":
        return "Uses the official LongMemEval-S cleaned 500-case dataset and all haystack sessions, but keeps the Azure-MemForest GPT-5-family judge policy and internal harness gates."
    return "Uses the official LongMemEval-S cleaned 500-case dataset, but keeps the Azure-MemForest bounded session-slice policy and GPT-5-family judge policy."


def _is_official_shard() -> bool:
    return SHARD == "official-all" or SHARD.startswith("official_")


def _selection_strategy_text() -> str:
    if SESSION_POLICY == "full":
        return "all_haystack_sessions"
    return "first_8_haystack_sessions_plus_all_answer_sessions_then_fill_to_9"


def preflight_report(cases: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "shard": SHARD,
        "session_policy": SESSION_POLICY,
        "case_count": len(cases),
        "case_ids": [case["case_id"] for case in cases],
        "results_dir": str(RESULTS),
        "results_dir_exists": RESULTS.exists(),
        "strict_preflight": strict_preflight_snapshot(),
        "queue_before": pilot.queue_state(),
        "manifest_gates": MANIFEST["gates"],
    }


def main() -> None:
    if AGGREGATE_DIRS:
        final = aggregate_shards([Path(item) for item in AGGREGATE_DIRS])
        RESULTS.mkdir(parents=True, exist_ok=True)
        save(RESULTS / "aggregate_heldout_shards.json", final)
        print(json.dumps(final, indent=2), flush=True)
        return
    if not ALLOW_LIVE and not PREFLIGHT_ONLY:
        raise RuntimeError("Refusing live run. Set PHASE2_50_ALLOW_LIVE=true after final preflight approval.")
    cases = load_cases()
    if PREFLIGHT_ONLY:
        report = preflight_report(cases)
        RESULTS.mkdir(parents=True, exist_ok=True)
        save(RESULTS / "preflight.json", report)
        print(json.dumps(report, indent=2), flush=True)
        return
    if not RUN_SUFFIX:
        raise RuntimeError("PHASE2_RUN_SUFFIX is required for 50-case shards.")
    RESULTS.mkdir(parents=True, exist_ok=True)

    pilot.CASES = cases
    pilot.RESULTS = RESULTS
    pilot.OVERRIDES = {"overrides": []}
    pilot.OVERRIDE_BY_SESSION = {}

    preflight = strict_preflight_snapshot() if pilot.STRICT_PREFLIGHT else {"strict": False}
    metadata = {
        "api_version": pilot.api_get("/version", user_id="benchmark-phase2-50-metadata"),
        "queue_before": pilot.queue_state(),
        "decisions": MANIFEST,
        "auth_mode": pilot.AUTH_MODE,
        "dispatch_mode": pilot.DISPATCH_MODE,
        "durable_hub": pilot.DURABLE_HUB if pilot.DISPATCH_MODE == "durable" else None,
        "run_suffix": RUN_SUFFIX,
        "results_dir": str(RESULTS),
        "shard": SHARD,
        "session_policy": SESSION_POLICY,
        "case_ids": [case["case_id"] for case in cases],
        "preflight": preflight,
    }
    save(RESULTS / "run_metadata.json", metadata)

    previous_probe = None
    results = []
    pro_sample_ids = pro_judge_sample_ids(cases)
    for index, case in enumerate(cases, start=1):
        print(json.dumps({"case": case["case_id"], "index": index, "type": case["question_type"], "shard": SHARD}), flush=True)
        result = pilot.run_case(case, index, previous_probe)
        pro_reason = None
        if result.get("status") == "succeeded" and result.get("judges", {}).get("agreement") is False:
            pro_reason = "judge_disagreement"
        elif result.get("status") == "succeeded" and result["case_id"] in pro_sample_ids:
            pro_reason = "pre_registered_agreement_sample"
        if pro_reason:
            result["judges"]["pro_judge"] = pro_judge(result["judge_prompt"])
            result["judges"]["pro_judge_reason"] = pro_reason
            save(RESULTS / f"{index:02d}_{case['case_id']}.json", result)
        results.append(result)
        previous_probe = case["question"]
        save(RESULTS / "results_partial.json", results)
        if result["status"] != "succeeded":
            break
    queue_after = pilot.queue_state()
    save(RESULTS / "queue_after.json", queue_after)
    save(RESULTS / "results.json", results)
    residue = run_residue_reprobe(results)
    save(RESULTS / "postrun_residue_reprobe.json", residue)
    final = aggregate(results, metadata["queue_before"], queue_after)
    final["residue_postrun_provenance_events"] = residue["provenance_contamination_events"]
    final["residue_postrun_raw_nonrefusal_events"] = residue["raw_nonrefusal_events"]
    save(RESULTS / "aggregate.json", final)
    print(json.dumps(final, indent=2), flush=True)


if __name__ == "__main__":
    main()
