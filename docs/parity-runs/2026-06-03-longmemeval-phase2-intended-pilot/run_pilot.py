from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils.time import parse_timestamp_to_unix


AZ = r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"
API = "https://memforest-api.example.invalid"
MODEL_ENDPOINT = "https://memforest-example-foundry.cognitiveservices.azure.com"
TENANT = "11111111-1111-4111-8111-111111111111"
USER = "22222222-2222-4222-8222-222222222222"
API_SCOPE = "api://33333333-3333-4333-8333-333333333333/access_as_user"
MODEL_SCOPE = "https://cognitiveservices.azure.com/.default"
BENCHMARK_SECRET_PATH = Path.home() / "AppData" / "Local" / "Temp" / "azure_memforest_benchmark_secret.txt"
AUTH_MODE = os.getenv("PHASE2_AUTH_MODE", "benchmark").strip().lower()
DISPATCH_MODE = os.getenv("PHASE2_DISPATCH_MODE", "servicebus").strip().lower()
PRIMARY_JUDGE_DEPLOYMENT = os.getenv("PHASE2_PRIMARY_JUDGE_DEPLOYMENT", "gpt-5.4").strip() or "gpt-5.4"
CROSS_JUDGE_DEPLOYMENT = os.getenv("PHASE2_CROSS_JUDGE_DEPLOYMENT", "gpt-5.4-mini-memory").strip() or "gpt-5.4-mini-memory"
DURABLE_STATUS_KEY = os.getenv("PHASE2_DURABLE_STATUS_KEY", "").strip()
DURABLE_START_URL = os.getenv("PHASE2_DURABLE_START_URL", "").strip()
DURABLE_HUB = os.getenv("PHASE2_DURABLE_HUB", "MemForestWrites").strip()
DURABLE_STATUS_CONNECTION = os.getenv("PHASE2_DURABLE_STATUS_CONNECTION", "Storage").strip()
DURABLE_STATUS_REQUIRED = os.getenv("PHASE2_DURABLE_STATUS_REQUIRED", "").strip().lower() in {"1", "true", "yes", "on"}
DURABLE_STATUS_ATTEMPTS = int(os.getenv("PHASE2_DURABLE_STATUS_ATTEMPTS", "60").strip() or "60")
STRICT_PREFLIGHT = os.getenv("PHASE2_STRICT_PREFLIGHT", "true").strip().lower() not in {"0", "false", "no", "off"}
AZURE_RESOURCE_GROUP = os.getenv("PHASE2_AZURE_RESOURCE_GROUP", "azure-memforest").strip()
API_CONTAINERAPP = os.getenv("PHASE2_API_CONTAINERAPP", "memforest-example-api").strip()
DURABLE_FUNCTION_APP = os.getenv("PHASE2_DURABLE_FUNCTION_APP", "memforest-example-durable").strip()
RUN_SUFFIX = os.getenv("PHASE2_RUN_SUFFIX", "").strip()
CASE_FILTER = {item.strip() for item in os.getenv("PHASE2_CASE_IDS", "").split(",") if item.strip()}
API_IDEMPOTENT_POST_ATTEMPTS = int(os.getenv("PHASE2_API_IDEMPOTENT_POST_ATTEMPTS", "1").strip() or "1")
EXTRACT_ALL_ASSISTANT_TURNS = os.getenv("PHASE2_EXTRACT_ALL_ASSISTANT_TURNS", "").strip().lower() in {"1", "true", "yes", "on"}
SELECTIVE_ASSISTANT_EXTRACTION = os.getenv("PHASE2_SELECTIVE_ASSISTANT_EXTRACTION", "").strip().lower() in {"1", "true", "yes", "on"}

RUN = Path(__file__).resolve().parent
CASES = json.loads((RUN / "pilot_cases.json").read_text(encoding="utf-8"))
OVERRIDES = json.loads((RUN / "pilot_overrides.json").read_text(encoding="utf-8")) if (RUN / "pilot_overrides.json").exists() else {"overrides": []}
OVERRIDE_BY_SESSION = {
    (item["case_id"], item["source_session_id"]): item
    for item in OVERRIDES.get("overrides", [])
}
RESULTS = Path(os.getenv("PHASE2_RESULTS_DIR", str(RUN / "case_results")))
RESULTS.mkdir(exist_ok=True)

_tokens: dict[str, tuple[str, float]] = {}

FIXED_MODEL_CALL_BUDGET = os.getenv("PHASE2_MODEL_CALL_BUDGET_FIXED", "3000").strip() or "3000"

INTENDED_API_SETTINGS = {
    "AZURE_MEMFOREST_DISPATCH_MODE": "durable",
    "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MULTIPLIER": "20",
    "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MIN": FIXED_MODEL_CALL_BUDGET,
    "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MAX": FIXED_MODEL_CALL_BUDGET,
    "AZURE_MEMFOREST_MODEL_MAX_RETRIES": "2",
    "AZURE_MEMFOREST_MODEL_TIMEOUT_SECONDS": "240",
    "AZURE_OPENAI_CHAT_DEPLOYMENT": "gpt-5.4-mini-memory",
    "AZURE_OPENAI_ANSWER_DEPLOYMENT": "gpt-5.4",
    "AZURE_OPENAI_EMBEDDING_DEPLOYMENT": "text-embedding-3-large",
    "AZURE_OPENAI_EMBEDDING_DIMENSION": "3072",
    "AZURE_SEARCH_RECALL_INDEX_NAME": "memforest-recall-large",
}

INTENDED_DURABLE_SETTINGS = {
    "AzureFunctionsJobHost__extensions__durableTask__hubName": "MemForestWrites",
    "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MULTIPLIER": "20",
    "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MIN": FIXED_MODEL_CALL_BUDGET,
    "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MAX": FIXED_MODEL_CALL_BUDGET,
    "AZURE_MEMFOREST_MODEL_MAX_RETRIES": "2",
    "AZURE_MEMFOREST_MODEL_TIMEOUT_SECONDS": "240",
    "AZURE_OPENAI_CHAT_DEPLOYMENT": "gpt-5.4-mini-memory",
    "AZURE_OPENAI_ANSWER_DEPLOYMENT": "gpt-5.4",
    "AZURE_OPENAI_EMBEDDING_DEPLOYMENT": "text-embedding-3-large",
    "AZURE_OPENAI_EMBEDDING_DIMENSION": "3072",
    "AZURE_SEARCH_RECALL_INDEX_NAME": "memforest-recall-large",
}

PIPELINE_ELIGIBLE_CASES_MIN = 8
PIPELINE_PRIMARY_JUDGE_CORRECT_MIN = 7
PIPELINE_JUDGE_DISAGREEMENTS_MAX = 1
CALIBRATION_PRIMARY_JUDGE_CORRECT_MIN = 8
CALIBRATION_JUDGE_DISAGREEMENTS_MAX = 1


def save(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2), encoding="utf-8")


def token(scope: str) -> str:
    now = time.time()
    current = _tokens.get(scope)
    if current and current[1] - now > 300:
        return current[0]
    raw = subprocess.check_output(
        [AZ, "account", "get-access-token", "--tenant", TENANT, "--scope", scope, "--query", "accessToken", "-o", "tsv"],
        text=True,
    ).strip()
    payload = raw.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    exp = float(json.loads(base64.urlsafe_b64decode(payload.encode("utf-8"))).get("exp", now + 3000))
    _tokens[scope] = (raw, exp)
    return raw


def benchmark_secret() -> str:
    return BENCHMARK_SECRET_PATH.read_text(encoding="utf-8").strip()


def api_headers(*, user_id: str = USER, json_body: bool = False, idem: str | None = None) -> dict[str, str]:
    headers = {
        "x-tenant-id": TENANT,
        "x-actor-id": user_id,
    }
    if AUTH_MODE == "benchmark":
        headers["x-benchmark-auth"] = benchmark_secret()
    elif AUTH_MODE == "dev":
        headers["x-auth-roles"] = "memory.benchmark"
    elif AUTH_MODE == "entra":
        headers["Authorization"] = f"Bearer {token(API_SCOPE)}"
    else:
        raise RuntimeError(f"unsupported PHASE2_AUTH_MODE={AUTH_MODE!r}")
    if json_body:
        headers["Content-Type"] = "application/json"
    if idem:
        headers["Idempotency-Key"] = idem
    return headers


def model_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {token(MODEL_SCOPE)}", "Content-Type": "application/json"}


def api_get(path: str, *, user_id: str = USER) -> dict:
    response = requests.get(API + path, headers=api_headers(user_id=user_id), timeout=120)
    response.raise_for_status()
    return response.json()


def api_post(path: str, body: dict | None = None, *, user_id: str = USER, idem: str | None = None) -> dict:
    attempts = max(1, API_IDEMPOTENT_POST_ATTEMPTS if idem else 1)
    for attempt in range(1, attempts + 1):
        try:
            response = requests.post(
                API + path,
                headers=api_headers(user_id=user_id, json_body=True, idem=idem),
                json=body or {},
                timeout=180,
            )
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            if attempt == attempts:
                raise
            time.sleep(min(30, 2**attempt))
    raise RuntimeError("unreachable idempotent POST retry state")


def query_body_for_case(case: dict[str, Any], *, top_k: int = 8, max_facts: int = 8, trace_requested: bool = True) -> dict[str, Any]:
    body: dict[str, Any] = {
        "question": case["question"],
        "top_k": top_k,
        "max_facts": max_facts,
        "trace_requested": trace_requested,
    }
    question_date = str(case.get("question_date") or "").strip()
    if question_date:
        body["query_time"] = parse_timestamp_to_unix(question_date)
    return body


def progress_fingerprint(status: dict) -> tuple:
    telemetry = status.get("telemetry") or {}
    return (
        status.get("status"),
        status.get("stage"),
        len(status.get("stage_history") or []),
        telemetry.get("total_calls"),
        telemetry.get("total_started_calls"),
        telemetry.get("in_flight_calls"),
        telemetry.get("extract_build_heartbeat_count"),
        telemetry.get("total_latency_ms"),
    )


def wait_job_progress(job_id: str, *, user_id: str, hard_timeout_seconds: int, idle_timeout_seconds: int) -> tuple[dict, float, dict]:
    start = time.time()
    status = api_get(f"/v1/users/{user_id}/jobs/{job_id}", user_id=user_id)
    last_fingerprint = progress_fingerprint(status)
    last_progress_at = start
    progress_events = [{"elapsed_sec": 0.0, "fingerprint": list(last_fingerprint)}]
    while status["status"] not in {"succeeded", "failed", "cancelled"}:
        now = time.time()
        if now - start >= hard_timeout_seconds:
            return status, now - start, {"timeout_reason": "hard_timeout", "progress_events": progress_events}
        if now - last_progress_at >= idle_timeout_seconds:
            return status, now - start, {"timeout_reason": "idle_timeout", "progress_events": progress_events}
        time.sleep(10)
        status = api_get(f"/v1/users/{user_id}/jobs/{job_id}", user_id=user_id)
        fingerprint = progress_fingerprint(status)
        if fingerprint != last_fingerprint:
            last_fingerprint = fingerprint
            last_progress_at = time.time()
            progress_events.append({"elapsed_sec": last_progress_at - start, "fingerprint": list(fingerprint)})
    return status, time.time() - start, {"timeout_reason": None, "progress_events": progress_events}


def durable_instance_status(job_id: str) -> dict | None:
    if DISPATCH_MODE != "durable" or not (DURABLE_START_URL and DURABLE_STATUS_KEY):
        return None
    base_url = DURABLE_START_URL.split("/api/", 1)[0].rstrip("/")
    last_error = None
    for _ in range(max(1, DURABLE_STATUS_ATTEMPTS)):
        try:
            response = requests.get(
                f"{base_url}/runtime/webhooks/durabletask/instances/{job_id}",
                params={"taskHub": DURABLE_HUB, "connection": DURABLE_STATUS_CONNECTION},
                headers={"x-functions-key": DURABLE_STATUS_KEY},
                timeout=120,
            )
            response.raise_for_status()
            payload = response.json()
            redacted = {key: value for key, value in payload.items() if key != "input"}
            if redacted.get("runtimeStatus") == "Completed":
                redacted.pop("output", None)
            elif isinstance(redacted.get("output"), str) and len(redacted["output"]) > 4000:
                redacted["output"] = redacted["output"][:4000] + "...<truncated>"
            if redacted.get("runtimeStatus") in {"Completed", "Failed", "Terminated", "Canceled"}:
                return redacted
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else "unknown"
            last_error = RuntimeError(f"Durable status request failed with HTTP {status_code}")
        except Exception as exc:
            last_error = RuntimeError(f"Durable status request failed: {type(exc).__name__}")
        time.sleep(5)
    if last_error is not None:
        raise last_error
    return None


def reconcile_durable_completed_job(job_id: str, *, user_id: str, status: dict, wait_info: dict, durable_status: dict | None) -> tuple[dict, dict]:
    if status.get("status") == "succeeded" or (durable_status or {}).get("runtimeStatus") != "Completed":
        return status, wait_info
    refreshed = api_get(f"/v1/users/{user_id}/jobs/{job_id}", user_id=user_id)
    if refreshed.get("status") == "succeeded":
        updated_wait = dict(wait_info)
        updated_wait["durable_completed_reconciled"] = True
        updated_wait["original_timeout_reason"] = wait_info.get("timeout_reason")
        updated_wait["timeout_reason"] = None
        return refreshed, updated_wait
    return status, wait_info


def norm(text: object) -> str:
    value = str(text or "").lower()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def is_content_filter_failure(status: dict) -> bool:
    text = str(status.get("error") or "")
    return "content_filter" in text or "ResponsibleAIPolicyViolation" in text


def get_anscheck_prompt(task: str, question: str, answer: str, response: str, abstention: bool = False) -> str:
    if not abstention:
        if task in ["single-session-user", "single-session-assistant", "multi-session"]:
            template = "I will give you a question, a correct answer, and a response from a model. Please answer yes if the response contains the correct answer. Otherwise, answer no. If the response is equivalent to the correct answer or contains all the intermediate steps to get the correct answer, you should also answer yes. If the response only contains a subset of the information required by the answer, answer no. \n\nQuestion: {}\n\nCorrect Answer: {}\n\nModel Response: {}\n\nIs the model response correct? Answer yes or no only."
            return template.format(question, answer, response)
        if task == "temporal-reasoning":
            template = "I will give you a question, a correct answer, and a response from a model. Please answer yes if the response contains the correct answer. Otherwise, answer no. If the response is equivalent to the correct answer or contains all the intermediate steps to get the correct answer, you should also answer yes. If the response only contains a subset of the information required by the answer, answer no. In addition, do not penalize off-by-one errors for the number of days. If the question asks for the number of days/weeks/months, etc., and the model makes off-by-one errors (e.g., predicting 19 days when the answer is 18), the model's response is still correct. \n\nQuestion: {}\n\nCorrect Answer: {}\n\nModel Response: {}\n\nIs the model response correct? Answer yes or no only."
            return template.format(question, answer, response)
        if task == "knowledge-update":
            template = "I will give you a question, a correct answer, and a response from a model. Please answer yes if the response contains the correct answer. Otherwise, answer no. If the response contains some previous information along with an updated answer, the response should be considered as correct as long as the updated answer is the required answer.\n\nQuestion: {}\n\nCorrect Answer: {}\n\nModel Response: {}\n\nIs the model response correct? Answer yes or no only."
            return template.format(question, answer, response)
        if task == "single-session-preference":
            template = "I will give you a question, a rubric for desired personalized response, and a response from a model. Please answer yes if the response satisfies the desired response. Otherwise, answer no. The model does not need to reflect all the points in the rubric. The response is correct as long as it recalls and utilizes the user's personal information correctly.\n\nQuestion: {}\n\nRubric: {}\n\nModel Response: {}\n\nIs the model response correct? Answer yes or no only."
            return template.format(question, answer, response)
        raise NotImplementedError(task)
    template = "I will give you an unanswerable question, an explanation, and a response from a model. Please answer yes if the model correctly identifies the question as unanswerable. The model could say that the information is incomplete, or some other information is given but the asked information is not.\n\nQuestion: {}\n\nExplanation: {}\n\nModel Response: {}\n\nDoes the model correctly identify the question as unanswerable? Answer yes or no only."
    return template.format(question, answer, response)


def judge(deployment: str, prompt: str) -> dict:
    body = {
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0,
        "max_completion_tokens": 64,
    }
    response = requests.post(
        f"{MODEL_ENDPOINT}/openai/deployments/{deployment}/chat/completions?api-version=2024-10-21",
        headers=model_headers(),
        json=body,
        timeout=180,
    )
    response.raise_for_status()
    payload = response.json()
    content = payload["choices"][0]["message"].get("content", "").strip()
    return {"deployment": deployment, "response": content, "label": "yes" in content.lower(), "raw": payload}


def queue_state() -> dict:
    raw = subprocess.check_output(
        [
            AZ,
            "servicebus",
            "queue",
            "show",
            "--resource-group",
            "azure-memforest",
            "--namespace-name",
            "memforestexamplesb",
            "--name",
            "memforest-writes",
            "--query",
            "{active:countDetails.activeMessageCount,deadletter:countDetails.deadLetterMessageCount,scheduled:countDetails.scheduledMessageCount}",
            "-o",
            "json",
        ],
        text=True,
    )
    return json.loads(raw)


def _settings_dict(items: list[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in items:
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        value = item.get("value")
        if value is None and item.get("secretRef"):
            value = "<secret>"
        out[name] = str(value if value is not None else "")
    return out


def api_containerapp_settings() -> dict[str, str]:
    raw = subprocess.check_output(
        [
            AZ,
            "containerapp",
            "revision",
            "list",
            "--resource-group",
            AZURE_RESOURCE_GROUP,
            "--name",
            API_CONTAINERAPP,
            "--query",
            "[?properties.active && properties.trafficWeight>`0`].properties.template.containers[0].env",
            "-o",
            "json",
        ],
        text=True,
    )
    values = json.loads(raw)
    if not values:
        raise RuntimeError("api_containerapp strict preflight failed: no active traffic revision env found")
    revisions = [_settings_dict(env) for env in values]
    baseline = revisions[0]
    intended_keys = set(INTENDED_API_SETTINGS)
    mismatched_revisions = [
        index
        for index, settings in enumerate(revisions, start=1)
        if {key: settings.get(key) for key in intended_keys} != {key: baseline.get(key) for key in intended_keys}
    ]
    if mismatched_revisions:
        raise RuntimeError(f"api_containerapp strict preflight failed: active traffic revisions disagree on intended settings: {mismatched_revisions}")
    return baseline


def durable_function_settings() -> dict[str, str]:
    raw = subprocess.check_output(
        [
            AZ,
            "functionapp",
            "config",
            "appsettings",
            "list",
            "--resource-group",
            AZURE_RESOURCE_GROUP,
            "--name",
            DURABLE_FUNCTION_APP,
            "-o",
            "json",
        ],
        text=True,
    )
    return _settings_dict(json.loads(raw))


def _require_settings(label: str, actual: dict[str, str], expected: dict[str, str]) -> None:
    mismatches = {
        key: {"expected": expected_value, "actual": actual.get(key)}
        for key, expected_value in expected.items()
        if actual.get(key) != expected_value
    }
    if mismatches:
        raise RuntimeError(f"{label} strict preflight failed: {json.dumps(mismatches, sort_keys=True)}")


def _expected_stamped_budget() -> int:
    _assert_budget_settings_consistent(INTENDED_API_SETTINGS)
    return int(INTENDED_API_SETTINGS["AZURE_MEMFOREST_MODEL_CALL_BUDGET_MIN"])


def _assert_budget_settings_consistent(settings: dict[str, str]) -> None:
    floor = int(settings["AZURE_MEMFOREST_MODEL_CALL_BUDGET_MIN"])
    cap = int(settings["AZURE_MEMFOREST_MODEL_CALL_BUDGET_MAX"])
    if floor > cap:
        raise RuntimeError(f"budget strict preflight failed: min {floor} exceeds max {cap}")
    if floor != cap:
        raise RuntimeError(f"budget strict preflight failed: fixed stamped budget requires min {floor} to equal max {cap}")


def strict_preflight_snapshot() -> dict[str, Any]:
    api_settings = api_containerapp_settings()
    durable_settings = durable_function_settings() if DISPATCH_MODE == "durable" else {}
    _require_settings("api_containerapp", api_settings, INTENDED_API_SETTINGS)
    _assert_budget_settings_consistent(api_settings)
    if DISPATCH_MODE == "durable":
        expected_durable = dict(INTENDED_DURABLE_SETTINGS)
        expected_durable["AzureFunctionsJobHost__extensions__durableTask__hubName"] = DURABLE_HUB
        _require_settings("durable_function_app", durable_settings, expected_durable)
        _assert_budget_settings_consistent(durable_settings)
    keys = sorted(set(INTENDED_API_SETTINGS) | set(INTENDED_DURABLE_SETTINGS))
    return {
        "strict": True,
        "api_containerapp": {key: api_settings.get(key) for key in keys if key in api_settings},
        "durable_function_app": {key: durable_settings.get(key) for key in keys if key in durable_settings},
    }


def turns_for_ingest(session: dict, *, case_id: str = "") -> tuple[list[dict], dict]:
    """Store assistant transcript turns, but exclude them from extraction for Phase 1."""
    override = OVERRIDE_BY_SESSION.get((case_id, session.get("source_session_id")))
    retained_ids = set(override.get("retained_content_ids") or []) if override else set()
    extract_assistant_ids = set(override.get("extract_assistant_content_ids") or []) if override else set()
    assistant_advice_metadata = dict(override.get("assistant_advice_metadata") or {}) if override else {}
    session_date = str((session.get("metadata") or {}).get("date") or "").strip()
    shaped: list[dict] = []
    assistant_skip_count = 0
    assistant_extract_count = 0
    assistant_selective_count = 0
    user_extract_count = 0
    original_turns = list(session.get("turns") or [])
    source_turns = [
        turn for turn in original_turns
        if not retained_ids or turn.get("content_id") in retained_ids
    ]
    synthetic_turns = [dict(turn) for turn in (override.get("synthetic_turns") or [])] if override else []
    source_turns.extend(synthetic_turns)
    retained_hash = hashlib.sha256(
        json.dumps(source_turns, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    for turn in source_turns:
        copied = dict(turn)
        if session_date and not any(copied.get(key) for key in ("timestamp", "time_stamp", "session_time")):
            copied["timestamp"] = session_date
        metadata = dict(copied.get("metadata") or {})
        if copied.get("role") == "assistant":
            if EXTRACT_ALL_ASSISTANT_TURNS:
                metadata["assistant_extraction_context"] = True
                metadata["override_reason"] = "extract_all_assistant_turns"
                assistant_extract_count += 1
            elif copied.get("content_id") in extract_assistant_ids:
                metadata["assistant_advice_context"] = True
                metadata["override_reason"] = override.get("override_reason", "assistant_advice_context") if override else "assistant_advice_context"
                metadata.update(assistant_advice_metadata)
                assistant_extract_count += 1
            elif SELECTIVE_ASSISTANT_EXTRACTION and _is_selective_assistant_memory_turn(copied, shaped):
                metadata["assistant_extraction_context"] = True
                metadata["override_reason"] = "selective_assistant_extraction"
                assistant_extract_count += 1
                assistant_selective_count += 1
            else:
                metadata["skip_extraction"] = True
                assistant_skip_count += 1
        elif copied.get("role") == "user":
            user_extract_count += 1
        copied["metadata"] = metadata
        shaped.append(copied)
    return shaped, {
        "assistant_turns_marked_skip_extraction": assistant_skip_count,
        "assistant_turns_extractable": assistant_extract_count,
        "assistant_turns_selectively_extractable": assistant_selective_count,
        "user_turns_extractable": user_extract_count,
        "original_turn_count": len(original_turns),
        "retained_turn_count": len(source_turns),
        "evidence_minimized": bool(override),
        "override_type": override.get("override_type") if override else None,
        "retained_content_ids": sorted(retained_ids),
        "synthetic_turn_count": len(synthetic_turns),
        "synthetic_content_ids": [turn.get("content_id") for turn in synthetic_turns],
        "extract_assistant_content_ids": sorted(extract_assistant_ids),
        "extract_all_assistant_turns": EXTRACT_ALL_ASSISTANT_TURNS,
        "selective_assistant_extraction": SELECTIVE_ASSISTANT_EXTRACTION,
        "retained_turns_sha256": retained_hash,
        "exclude_from_pipeline_accuracy": bool(override and override.get("exclude_from_pipeline_accuracy")),
    }


_ASSISTANT_PHATIC_RE = re.compile(
    r"\b(?:happy to help|let me know|feel free|you'?re welcome|sounds good|great question|sure|okay|ok)\b",
    re.IGNORECASE,
)
_CONCRETE_VALUE_RE = re.compile(
    r"(?:[$€£]\s*\d|\b\d+(?:[.,:]\d+)?\s*(?:am|pm|a\.m\.|p\.m\.|%|percent|hours?|days?|weeks?|months?|years?|pages?|followers?|people|dollars?)\b)",
    re.IGNORECASE,
)
_LIST_MARKER_RE = re.compile(r"(?:^|\n)\s*(?:[-*•]|\d+[.)])\s+\S")
_COMMA_LIST_RE = re.compile(r"\b[^,\n]{3,},\s*[^,\n]{3,},\s*(?:and\s+)?[^,\n]{3,}\b", re.IGNORECASE)
_NAMED_ENTITY_RE = re.compile(
    r"(?:\b[A-Z][A-Za-zÀ-ÖØ-öø-ÿ'’-]{2,}\b(?:\s+\b[A-Z][A-Za-zÀ-ÖØ-öø-ÿ'’-]{2,}\b)+|\b[A-Z]{2,}\b)"
)
_INLINE_SINGLE_ENTITY_RE = re.compile(
    r"\b(?:is|was|called|named|choose|pick|recommend|suggest|try|visit|use|read|watch|play)\s+(?:the\s+)?([A-Z][A-Za-zÀ-ÖØ-öø-ÿ'’-]{2,})\b"
)
_SHORT_NAMED_ANSWER_RE = re.compile(r"^\s*(?:[A-Z][A-Za-zÀ-ÖØ-öø-ÿ'’-]{2,})(?:\s+[A-Z][A-Za-zÀ-ÖØ-öø-ÿ'’-]{2,}){0,3}\s*[.!?]?\s*$")
_QUOTED_TEXT_RE = re.compile(r"[\"'“”‘’][^\"'“”‘’]{2,}[\"'“”‘’]")
_USER_MEMORY_REQUEST_RE = re.compile(
    r"\b(?:remember|remind me|recall|list|name|enumerate|which|who|where|when|how many|how much|recommend|suggest|suggestion|option|brand|example|place|time|date|number)\b",
    re.IGNORECASE,
)
_EXPLICIT_LIST_REQUEST_RE = re.compile(r"\b(?:list|name|enumerate|recommend|suggest)\b", re.IGNORECASE)
_SPECIFIC_WHAT_REQUEST_RE = re.compile(
    r"\bwhat\b.{0,80}\b(?:process(?:es)?|language(?:s)?|brand|place|time|date|number|item(?:s)?|event(?:s)?|example)\b",
    re.IGNORECASE,
)
_ANSWER_SEEKING_REQUEST_RE = re.compile(
    r"\b(?:give me|what are|what is|which are|which is|any recommendations|recommendations for|"
    r"do you think there (?:are|is) any|can you (?:recommend|suggest)|tell me (?:about|which|what))\b",
    re.IGNORECASE,
)
_EXAMPLE_ANSWER_RE = re.compile(r"\b(?:for example|one example|popular option|include:|include)\b", re.IGNORECASE)
_GENERIC_ADVICE_RE = re.compile(
    r"\b(?:you should|try to|consider|it'?s generally|in general|for example|steps?|tips?|guide|how to)\b",
    re.IGNORECASE,
)


def _is_selective_assistant_memory_turn(turn: dict, prior_shaped_turns: list[dict]) -> bool:
    text = str(turn.get("content", turn.get("text", ""))).strip()
    if not text:
        return False
    if _ASSISTANT_PHATIC_RE.search(text) and len(text) < 180:
        return False
    previous_user_text = _previous_user_text(prior_shaped_turns)
    signals = {
        "concrete_value": bool(_CONCRETE_VALUE_RE.search(text)),
        "list_marker": bool(_LIST_MARKER_RE.search(text)),
        "comma_list": bool(_COMMA_LIST_RE.search(text)),
        "named_entity": bool(_NAMED_ENTITY_RE.search(text)),
        "inline_entity": bool(_INLINE_SINGLE_ENTITY_RE.search(text)),
        "short_named_answer": len(text) <= 80 and bool(_SHORT_NAMED_ANSWER_RE.search(text)),
        "quoted_text": bool(_QUOTED_TEXT_RE.search(text)),
    }
    non_comma_score = sum(1 for key, matched in signals.items() if key != "comma_list" and matched)
    has_memory_request = bool(_USER_MEMORY_REQUEST_RE.search(previous_user_text))
    has_specific_what_request = bool(_SPECIFIC_WHAT_REQUEST_RE.search(previous_user_text))
    has_answer_request = bool(_ANSWER_SEEKING_REQUEST_RE.search(previous_user_text))
    if _GENERIC_ADVICE_RE.search(text) and not has_memory_request and non_comma_score < 2:
        return False
    if signals["short_named_answer"] and has_memory_request:
        return True
    if signals["inline_entity"] and has_memory_request:
        return True
    if (signals["list_marker"] or signals["comma_list"]) and (
        _EXPLICIT_LIST_REQUEST_RE.search(previous_user_text) or has_specific_what_request
    ):
        return True
    if signals["list_marker"] and signals["named_entity"] and has_answer_request:
        return True
    if signals["inline_entity"] and has_answer_request:
        return True
    if signals["quoted_text"] and signals["comma_list"] and _EXAMPLE_ANSWER_RE.search(text) and has_answer_request:
        return True
    if signals["concrete_value"] and has_memory_request:
        return True
    if non_comma_score >= 3 and (has_memory_request or has_specific_what_request):
        return True
    return False


def _previous_user_text(prior_shaped_turns: list[dict]) -> str:
    for turn in reversed(prior_shaped_turns):
        if str(turn.get("role", "")).strip().lower() == "user":
            return str(turn.get("content", turn.get("text", "")))
    return ""


def residue_probe(question: str, *, user_id: str) -> dict:
    try:
        return api_post(f"/v1/users/{user_id}/query", {"question": question, "top_k": 3, "max_facts": 3, "trace_requested": False}, user_id=user_id)
    except requests.HTTPError as exc:
        return {"status": "http_error", "status_code": exc.response.status_code, "body": exc.response.text}
    except requests.RequestException as exc:
        return {"status": "request_error", "error": type(exc).__name__}


def run_case(case: dict, index: int, previous_probe: str | None) -> dict:
    case_id = case["case_id"]
    user_id = f"benchmark-phase2-{index:02d}-{case_id}{('-' + RUN_SUFFIX) if RUN_SUFFIX else ''}"
    out = RESULTS / f"{index:02d}_{case_id}.json"
    if out.exists():
        result = json.loads(out.read_text(encoding="utf-8"))
        if result.get("status") == "succeeded":
            print(json.dumps({"case": case_id, "status": "case_checkpoint_succeeded"}), flush=True)
            return result
        result["status"] = "running"
        result.setdefault("events", [])
        result.setdefault("filtered_skips", [])
        result.setdefault("modified_input", False)
        _restore_override_flags(result)
        result.setdefault("checkpoint_resume_count", 0)
        result["checkpoint_resume_count"] += 1
    else:
        result = {
            "case_id": case_id,
            "index": index,
            "question_type": case["question_type"],
            "question": case["question"],
            "question_date": case.get("question_date"),
            "gold_answer": case["gold_answer"],
            "selected_session_count": case["selected_session_count"],
            "selected_turn_count": case["selected_turn_count"],
            "events": [],
            "status": "running",
            "benchmark_user_id": user_id,
            "filtered_skips": [],
            "modified_input": False,
        }
    save(out, result)
    if previous_probe and "residue_probe" not in result:
        if result.get("checkpoint_resume_count", 0) > 0:
            result["residue_probe"] = {
                "question": previous_probe,
                "skipped": "checkpoint_resume_user_may_already_be_populated",
            }
        else:
            result["residue_probe"] = {"question": previous_probe, "response": residue_probe(previous_probe, user_id=user_id)}
        save(out, result)
    reconcile_existing_ingests(result, user_id=user_id)
    save(out, result)

    completed_indices = {
        int(event["index"])
        for event in result.get("events", [])
        if event.get("step") == "ingest" and (event.get("status") or {}).get("status") == "succeeded"
    }
    skipped_source_ids = {skip.get("source_session_id") for skip in result.get("filtered_skips", [])}
    result["events"].append(
        {
            "step": "resume",
            "completed_indices": sorted(completed_indices),
            "skipped_source_session_ids": sorted(x for x in skipped_source_ids if x),
            "timestamp_unix": time.time(),
        }
    )
    save(out, result)
    case_start = time.time()
    for session_index, session in enumerate(case["sessions"], start=1):
        if session_index in completed_indices or session["source_session_id"] in skipped_source_ids:
            print(json.dumps({"case": case_id, "ingest": session_index, "status": "checkpoint_skipped", "source_session_id": session["source_session_id"]}), flush=True)
            continue
        if time.time() - case_start > 90 * 60:
            result["status"] = "timeout_case"
            save(out, result)
            return result
        attempt = sum(1 for event in result.get("events", []) if event.get("step") == "ingest" and event.get("index") == session_index) + 1
        ingest_turns, input_shaping = turns_for_ingest(session, case_id=case_id)
        ingest = api_post(
            f"/v1/users/{user_id}/sessions/{session['session_id']}-{uuid.uuid4().hex}:ingest",
            {
                "turns": ingest_turns,
                "metadata": {
                    "source": "phase2-pilot",
                    "assistant_transcript_extraction_policy": "skip_assistant_turns",
                    **input_shaping,
                    **session.get("metadata", {}),
                },
            },
            user_id=user_id,
            idem=f"phase2-ingest-{case_id}-{session_index}-attempt-{attempt}",
        )
        if STRICT_PREFLIGHT:
            stamped_budget = dict(ingest.get("progress_budget") or {})
            if int(stamped_budget.get("max_completed_model_calls") or 0) != _expected_stamped_budget():
                raise RuntimeError(f"api stamped unexpected progress budget for {case_id}:{session_index}: {stamped_budget}")
        ingest_status, ingest_elapsed, wait_info = wait_job_progress(
            ingest["job_id"],
            user_id=user_id,
            hard_timeout_seconds=30 * 60,
            idle_timeout_seconds=10 * 60,
        )
        job_events = api_get(f"/v1/users/{user_id}/jobs/{ingest['job_id']}/events", user_id=user_id)
        durable_status = None
        durable_error = None
        try:
            durable_status = durable_instance_status(ingest["job_id"])
        except Exception as exc:
            durable_error = f"{type(exc).__name__}: {exc}"
        if durable_error is None:
            ingest_status, wait_info = reconcile_durable_completed_job(
                ingest["job_id"],
                user_id=user_id,
                status=ingest_status,
                wait_info=wait_info,
                durable_status=durable_status,
            )
        result["events"].append(
            {
                "step": "ingest",
                "index": session_index,
                "source_session_id": session["source_session_id"],
                "job_id": ingest["job_id"],
                "input_shaping": input_shaping,
                "status": ingest_status,
                "elapsed_sec": ingest_elapsed,
                "job_events": job_events,
                "wait_info": wait_info,
                "durable_instance": durable_status,
                "durable_status_error": durable_error,
            }
        )
        if input_shaping.get("evidence_minimized") or input_shaping.get("exclude_from_pipeline_accuracy"):
            result["modified_input"] = True
            result["exclude_from_pipeline_accuracy"] = True
        save(out, result)
        print(json.dumps({"case": case_id, "ingest": session_index, "status": ingest_status["status"], "elapsed": round(ingest_elapsed, 1)}), flush=True)
        if ingest_status["status"] != "succeeded":
            if is_content_filter_failure(ingest_status):
                skip = {
                    "source_session_id": session["source_session_id"],
                    "answer_bearing": bool((session.get("metadata") or {}).get("is_answer_session")),
                    "turn_count": len(session.get("turns") or []),
                    "reason": "content_filter",
                    "error": ingest_status.get("error"),
                }
                if skip["answer_bearing"]:
                    result["status"] = "blocked_by_content_filter"
                    result["filtered_block"] = skip
                    save(out, result)
                    return result
                result["filtered_skips"].append(skip)
                result["modified_input"] = True
                save(out, result)
                print(json.dumps({"case": case_id, "ingest": session_index, "status": "filtered_skipped", "source_session_id": session["source_session_id"]}), flush=True)
                continue
            result["status"] = "timeout_ingest" if wait_info.get("timeout_reason") else "failed_ingest"
            save(out, result)
            return result
        if DURABLE_STATUS_REQUIRED and (not durable_status or durable_status.get("runtimeStatus") != "Completed"):
            result["status"] = "failed_durable_instance"
            save(out, result)
            return result

    query_start = time.time()
    query_body = query_body_for_case(case)
    result["query_request"] = query_body
    query = api_post(f"/v1/users/{user_id}/query", query_body, user_id=user_id)
    query_elapsed = time.time() - query_start
    answer = str(query.get("answer") or "")
    prompt = get_anscheck_prompt(case["question_type"], case["question"], case["gold_answer"], answer, abstention="_abs" in case_id)
    primary = judge(PRIMARY_JUDGE_DEPLOYMENT, prompt)
    cross = judge(CROSS_JUDGE_DEPLOYMENT, prompt)
    n_answer = norm(answer)
    n_gold = norm(case["gold_answer"])
    proxy = {
        "normalized_exact": n_answer == n_gold,
        "contains_gold": bool(n_gold and n_gold in n_answer),
        "acceptable_answer_match": any(norm(a) and norm(a) in n_answer for a in case.get("phase1_expected", {}).get("acceptable_answers", [])),
    }
    result["events"].append({"step": "query", "elapsed_sec": query_elapsed, "response": query})
    result["judge_prompt"] = prompt
    result["judges"] = {"primary": primary, "cross_check": cross, "agreement": primary["label"] == cross["label"]}
    result["proxy_scores"] = proxy
    result["answer"] = answer
    result["status"] = "succeeded"
    save(out, result)
    return result


def reconcile_existing_ingests(result: dict, *, user_id: str) -> None:
    for event in result.get("events", []):
        if event.get("step") != "ingest" or not event.get("job_id"):
            continue
        status = event.get("status") or {}
        if status.get("status") == "succeeded":
            continue
        try:
            refreshed = api_get(f"/v1/users/{user_id}/jobs/{event['job_id']}", user_id=user_id)
        except Exception as exc:
            event["reconcile_error"] = f"{type(exc).__name__}: {exc}"
            continue
        if refreshed.get("status") == "succeeded":
            event["status"] = refreshed
            event["reconciled_from_api"] = True
            event["dlq_settlement_artifact"] = True
            try:
                event["durable_instance"] = durable_instance_status(event["job_id"])
            except Exception as exc:
                event["durable_status_error"] = f"{type(exc).__name__}: {exc}"
        elif refreshed.get("status") in {"failed", "cancelled"}:
            event["status"] = refreshed
            event["reconciled_from_api"] = True


def aggregate(results: list[dict], before: dict, after: dict) -> dict:
    completed = [r for r in results if r.get("status") == "succeeded"]
    pipeline_completed = [r for r in completed if not r.get("exclude_from_pipeline_accuracy")]
    new_dlq = int(after.get("deadletter", 0)) - int(before.get("deadletter", 0))
    durable_mode = DISPATCH_MODE == "durable"
    ingest_events = [
        event
        for result in results
        for event in _latest_ingest_events(result)
    ]
    durable_instances = [
        event.get("durable_instance")
        for event in ingest_events
        if event.get("durable_instance")
    ]
    successful_ingest_events = [
        event
        for event in ingest_events
        if (event.get("status") or {}).get("status") == "succeeded"
    ]
    skipped_non_answer_ingest_events = [
        event
        for result in results
        for event in _latest_ingest_events(result)
        if (event.get("status") or {}).get("status") == "failed"
        and _is_skipped_non_answer_ingest(result, event)
    ]
    durable_completed = sum(1 for item in durable_instances if item.get("runtimeStatus") == "Completed")
    durable_failed = sum(1 for item in durable_instances if item.get("runtimeStatus") in {"Failed", "Terminated", "Canceled"})
    dlq_gate_passed = new_dlq == 0
    durable_accounted_events = len(successful_ingest_events) + len(skipped_non_answer_ingest_events)
    durable_gate_passed = not durable_mode or durable_completed == durable_accounted_events
    calibration_primary_correct = sum(1 for r in completed if r["judges"]["primary"]["label"])
    calibration_judge_disagreements = sum(1 for r in completed if not r["judges"]["agreement"])
    pipeline_primary_correct = sum(1 for r in pipeline_completed if r["judges"]["primary"]["label"])
    pipeline_judge_disagreements = sum(1 for r in pipeline_completed if not r["judges"]["agreement"])
    filtered_skip_count = sum(len(r.get("filtered_skips") or []) for r in results)
    blocked_by_content_filter = sum(1 for r in results if r.get("status") == "blocked_by_content_filter")
    residue_contamination_events = sum(1 for r in results if _residue_contaminated(r.get("residue_probe")))
    all_gates_passed = (
        len(results) == len(CASES)
        and len(completed) == len(CASES)
        and len(pipeline_completed) >= PIPELINE_ELIGIBLE_CASES_MIN
        and pipeline_primary_correct >= PIPELINE_PRIMARY_JUDGE_CORRECT_MIN
        and pipeline_judge_disagreements <= PIPELINE_JUDGE_DISAGREEMENTS_MAX
        and calibration_primary_correct >= CALIBRATION_PRIMARY_JUDGE_CORRECT_MIN
        and calibration_judge_disagreements <= CALIBRATION_JUDGE_DISAGREEMENTS_MAX
        and dlq_gate_passed
        and durable_gate_passed
        and durable_failed == 0
        and blocked_by_content_filter == 0
        and residue_contamination_events == 0
    )
    return {
        "status": "succeeded" if all_gates_passed else "gate_failed",
        "dispatch_mode": DISPATCH_MODE,
        "auth_mode": AUTH_MODE,
        "attempted_cases": len(results),
        "completed_cases": len(completed),
        "strict_unmodified_completed_cases": sum(1 for r in completed if not r.get("modified_input")),
        "best_effort_completed_cases": len(completed),
        "filtered_skip_count": filtered_skip_count,
        "blocked_by_content_filter": blocked_by_content_filter,
        "primary_judge_correct": calibration_primary_correct,
        "judge_agreements": sum(1 for r in completed if r["judges"]["agreement"]),
        "judge_disagreements": calibration_judge_disagreements,
        "pipeline_eligible_cases": len(pipeline_completed),
        "pipeline_primary_judge_correct": pipeline_primary_correct,
        "pipeline_judge_disagreements": pipeline_judge_disagreements,
        "new_dlq_count_delta": new_dlq,
        "dlq_gate_enforced": True,
        "dlq_gate_passed": dlq_gate_passed,
        "durable_instance_count": len(durable_instances),
        "durable_instance_completed_count": durable_completed,
        "durable_instance_failed_count": durable_failed,
        "successful_ingest_event_count": len(successful_ingest_events),
        "skipped_non_answer_ingest_event_count": len(skipped_non_answer_ingest_events),
        "durable_accounted_event_count": durable_accounted_events,
        "durable_gate_passed": durable_gate_passed,
        "residue_contamination_events": residue_contamination_events,
        "advance_to_50_case_subset": all_gates_passed,
        "queue_before": before,
        "queue_after": after,
        "case_statuses": [
            {
                "case_id": r["case_id"],
                "status": r["status"],
                "answer": r.get("answer"),
                "primary_label": r.get("judges", {}).get("primary", {}).get("label"),
                "cross_label": r.get("judges", {}).get("cross_check", {}).get("label"),
            }
            for r in results
        ],
    }


def _latest_ingest_events(result: dict) -> list[dict]:
    latest: dict[int, dict] = {}
    for event in result.get("events", []):
        if event.get("step") != "ingest":
            continue
        try:
            index = int(event["index"])
        except (KeyError, TypeError, ValueError):
            continue
        latest[index] = event
    return [latest[index] for index in sorted(latest)]


def _residue_contaminated(probe: dict | None) -> bool:
    if not probe:
        return False
    response = probe.get("response") or {}
    if response.get("status") == "http_error":
        return False
    answer = str(response.get("answer") or "").strip()
    facts = response.get("facts") or []
    refusal_markers = (
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
    )
    refusal = any(marker in answer.lower() for marker in refusal_markers)
    if refusal:
        return False
    return bool(facts or answer)


def _is_skipped_non_answer_ingest(result: dict, event: dict) -> bool:
    status = event.get("status") or {}
    if not is_content_filter_failure(status):
        return False
    source_session_id = event.get("source_session_id")
    return any(
        skip.get("source_session_id") == source_session_id
        and skip.get("reason") == "content_filter"
        and skip.get("answer_bearing") is False
        for skip in result.get("filtered_skips", [])
    )


def _restore_override_flags(result: dict) -> None:
    if any(
        event.get("step") == "ingest"
        and (
            (event.get("input_shaping") or {}).get("evidence_minimized")
            or (event.get("input_shaping") or {}).get("exclude_from_pipeline_accuracy")
        )
        for event in result.get("events", [])
    ):
        result["modified_input"] = True
        result["exclude_from_pipeline_accuracy"] = True


def main() -> None:
    preflight = strict_preflight_snapshot() if STRICT_PREFLIGHT else {"strict": False}
    metadata = {
        "api_version": api_get("/version", user_id="benchmark-phase2-metadata"),
        "queue_before": queue_state(),
        "decisions": json.loads((RUN / "manifest.json").read_text(encoding="utf-8")),
        "isolation": f"fresh benchmark synthetic user per case using auth_mode={AUTH_MODE}; dispatch_mode={DISPATCH_MODE}; no clear-per-case",
        "auth_mode": AUTH_MODE,
        "dispatch_mode": DISPATCH_MODE,
        "extract_all_assistant_turns": EXTRACT_ALL_ASSISTANT_TURNS,
        "selective_assistant_extraction": SELECTIVE_ASSISTANT_EXTRACTION,
        "primary_judge_deployment": PRIMARY_JUDGE_DEPLOYMENT,
        "cross_judge_deployment": CROSS_JUDGE_DEPLOYMENT,
        "durable_hub": DURABLE_HUB if DISPATCH_MODE == "durable" else None,
        "run_suffix": RUN_SUFFIX,
        "results_dir": str(RESULTS),
        "case_filter": sorted(CASE_FILTER),
        "preflight": preflight,
    }
    save(RUN / "run_metadata.json", metadata)
    previous_probe = None
    results = []
    for index, case in enumerate(CASES, start=1):
        if CASE_FILTER and case["case_id"] not in CASE_FILTER:
            continue
        print(json.dumps({"case": case["case_id"], "index": index, "type": case["question_type"]}), flush=True)
        result = run_case(case, index, previous_probe)
        results.append(result)
        previous_probe = case["question"]
        save(RUN / "results_partial.json", results)
        if result["status"] != "succeeded":
            break
    queue_after = queue_state()
    save(RUN / "queue_after.json", queue_after)
    save(RUN / "results.json", results)
    save(RUN / "aggregate.json", aggregate(results, metadata["queue_before"], queue_after))
    print(json.dumps(json.loads((RUN / "aggregate.json").read_text(encoding="utf-8")), indent=2), flush=True)


if __name__ == "__main__":
    main()
