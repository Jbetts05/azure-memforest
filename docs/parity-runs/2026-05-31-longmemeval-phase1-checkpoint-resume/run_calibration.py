from __future__ import annotations

import base64
import json
import re
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

import requests


AZ = r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"
API = "https://memforest-api.example.invalid"
MODEL_ENDPOINT = "https://memforest-example-foundry.cognitiveservices.azure.com"
TENANT = "11111111-1111-4111-8111-111111111111"
USER = "22222222-2222-4222-8222-222222222222"
API_SCOPE = "api://33333333-3333-4333-8333-333333333333/access_as_user"
MODEL_SCOPE = "https://cognitiveservices.azure.com/.default"
BENCHMARK_SECRET_PATH = Path.home() / "AppData" / "Local" / "Temp" / "azure_memforest_benchmark_secret.txt"

RUN = Path(__file__).resolve().parent
CASES = json.loads((RUN / "calibration_cases.json").read_text(encoding="utf-8"))
RESULTS = RUN / "case_results"
RESULTS.mkdir(exist_ok=True)

_tokens: dict[str, tuple[str, float]] = {}


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
        "x-benchmark-auth": benchmark_secret(),
        "x-tenant-id": TENANT,
        "x-actor-id": user_id,
    }
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
    response = requests.post(API + path, headers=api_headers(user_id=user_id, json_body=True, idem=idem), json=body or {}, timeout=180)
    response.raise_for_status()
    return response.json()


def progress_fingerprint(status: dict) -> tuple:
    telemetry = status.get("telemetry") or {}
    return (
        status.get("status"),
        status.get("stage"),
        len(status.get("stage_history") or []),
        telemetry.get("total_calls"),
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
        "max_completion_tokens": 10,
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


def residue_probe(question: str, *, user_id: str) -> dict:
    try:
        return api_post(f"/v1/users/{user_id}/query", {"question": question, "top_k": 3, "max_facts": 3, "trace_requested": False}, user_id=user_id)
    except requests.HTTPError as exc:
        return {"status": "http_error", "status_code": exc.response.status_code, "body": exc.response.text}


def run_case(case: dict, index: int, previous_probe: str | None) -> dict:
    case_id = case["case_id"]
    user_id = f"benchmark-phase1-{index:02d}-{case_id}"
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
        result.setdefault("checkpoint_resume_count", 0)
        result["checkpoint_resume_count"] += 1
    else:
        result = {
            "case_id": case_id,
            "index": index,
            "question_type": case["question_type"],
            "question": case["question"],
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
    reconcile_existing_ingests(result, user_id=user_id)
    save(out, result)

    if previous_probe:
        result["residue_probe"] = {"question": previous_probe, "response": residue_probe(previous_probe, user_id=user_id)}
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
        ingest = api_post(
            f"/v1/users/{user_id}/sessions/{session['session_id']}-{uuid.uuid4().hex}:ingest",
            {"turns": session["turns"], "metadata": {"source": "phase1-calibration", **session.get("metadata", {})}},
            user_id=user_id,
            idem=f"phase1-ingest-{case_id}-{session_index}-attempt-{attempt}",
        )
        ingest_status, ingest_elapsed, wait_info = wait_job_progress(
            ingest["job_id"],
            user_id=user_id,
            hard_timeout_seconds=30 * 60,
            idle_timeout_seconds=10 * 60,
        )
        job_events = api_get(f"/v1/users/{user_id}/jobs/{ingest['job_id']}/events", user_id=user_id)
        result["events"].append(
            {
                "step": "ingest",
                "index": session_index,
                "source_session_id": session["source_session_id"],
                "job_id": ingest["job_id"],
                "status": ingest_status,
                "elapsed_sec": ingest_elapsed,
                "job_events": job_events,
                "wait_info": wait_info,
            }
        )
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

    query_start = time.time()
    query = api_post(f"/v1/users/{user_id}/query", {"question": case["question"], "top_k": 8, "max_facts": 8, "trace_requested": True}, user_id=user_id)
    query_elapsed = time.time() - query_start
    answer = str(query.get("answer") or "")
    prompt = get_anscheck_prompt(case["question_type"], case["question"], case["gold_answer"], answer, abstention="_abs" in case_id)
    primary = judge("gpt-5.4", prompt)
    cross = judge("gpt-5.4-mini-memory", prompt)
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
        elif refreshed.get("status") in {"failed", "cancelled"}:
            event["status"] = refreshed
            event["reconciled_from_api"] = True


def aggregate(results: list[dict], before: dict, after: dict) -> dict:
    completed = [r for r in results if r.get("status") == "succeeded"]
    new_dlq = int(after.get("deadletter", 0)) - int(before.get("deadletter", 0))
    return {
        "status": "succeeded" if len(completed) == len(results) and new_dlq == 0 and all(r["judges"]["primary"]["label"] for r in completed) else "gate_failed",
        "attempted_cases": len(results),
        "completed_cases": len(completed),
        "strict_unmodified_completed_cases": sum(1 for r in completed if not r.get("modified_input")),
        "best_effort_completed_cases": len(completed),
        "filtered_skip_count": sum(len(r.get("filtered_skips") or []) for r in results),
        "blocked_by_content_filter": sum(1 for r in results if r.get("status") == "blocked_by_content_filter"),
        "primary_judge_correct": sum(1 for r in completed if r["judges"]["primary"]["label"]),
        "judge_agreements": sum(1 for r in completed if r["judges"]["agreement"]),
        "new_dlq_count_delta": new_dlq,
        "queue_before": before,
        "queue_after": after,
        "case_statuses": [{"case_id": r["case_id"], "status": r["status"], "answer": r.get("answer"), "primary_label": r.get("judges", {}).get("primary", {}).get("label"), "cross_label": r.get("judges", {}).get("cross_check", {}).get("label")} for r in results],
    }


def main() -> None:
    metadata = {
        "api_version": api_get("/version", user_id="benchmark-phase1-metadata"),
        "queue_before": queue_state(),
        "decisions": json.loads((RUN / "calibration_manifest.json").read_text(encoding="utf-8")),
        "isolation": "fresh benchmark synthetic user per case using x-benchmark-auth; no clear-per-case",
    }
    save(RUN / "run_metadata.json", metadata)
    previous_probe = None
    results = []
    for index, case in enumerate(CASES, start=1):
        print(json.dumps({"case": case["case_id"], "index": index, "type": case["question_type"]}), flush=True)
        result = run_case(case, index, previous_probe)
        results.append(result)
        previous_probe = case["question"]
        save(RUN / "results_partial.json", results)
        if result["status"] in {"failed_clear", "timeout_case"}:
            break
    queue_after = queue_state()
    save(RUN / "queue_after.json", queue_after)
    save(RUN / "results.json", results)
    save(RUN / "aggregate.json", aggregate(results, metadata["queue_before"], queue_after))
    print(json.dumps(json.loads((RUN / "aggregate.json").read_text(encoding="utf-8")), indent=2), flush=True)


if __name__ == "__main__":
    main()
