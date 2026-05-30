from __future__ import annotations

import base64
import json
import re
import statistics
import subprocess
import time
import uuid
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import quote

import requests


AZ = r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"
API = "https://memforest-api.example.invalid"
TENANT = "11111111-1111-4111-8111-111111111111"
USER = "22222222-2222-4222-8222-222222222222"
SCOPE = "api://33333333-3333-4333-8333-333333333333/access_as_user"

RUN = Path(__file__).resolve().parent
CASES = json.loads((RUN / "batch_cases.json").read_text(encoding="utf-8"))
RESULTS_DIR = RUN / "case_results"
RESULTS_DIR.mkdir(exist_ok=True)

_token: str | None = None
_token_exp = 0.0


def save_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2), encoding="utf-8")


def token() -> str:
    global _token, _token_exp
    now = time.time()
    if _token and _token_exp - now > 300:
        return _token
    raw = subprocess.check_output(
        [
            AZ,
            "account",
            "get-access-token",
            "--tenant",
            TENANT,
            "--scope",
            SCOPE,
            "--query",
            "accessToken",
            "-o",
            "tsv",
        ],
        text=True,
    ).strip()
    payload = raw.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    claims = json.loads(base64.urlsafe_b64decode(payload.encode("utf-8")))
    _token = raw
    _token_exp = float(claims.get("exp", now + 3000))
    return raw


def headers(*, json_body: bool = False, idem: str | None = None) -> dict[str, str]:
    result = {"Authorization": f"Bearer {token()}"}
    if json_body:
        result["Content-Type"] = "application/json"
    if idem:
        result["Idempotency-Key"] = idem
    return result


def get(path: str) -> dict:
    response = requests.get(API + path, headers=headers(), timeout=120)
    response.raise_for_status()
    return response.json()


def post(path: str, body: dict | None = None, *, idem: str | None = None) -> dict:
    response = requests.post(API + path, headers=headers(json_body=True, idem=idem), json=body or {}, timeout=180)
    response.raise_for_status()
    return response.json()


def wait_job(job_id: str, timeout_seconds: int) -> tuple[dict, float]:
    start = time.time()
    status = get(f"/v1/users/{USER}/jobs/{job_id}")
    while time.time() - start < timeout_seconds and status["status"] not in {"succeeded", "failed", "cancelled"}:
        time.sleep(5)
        status = get(f"/v1/users/{USER}/jobs/{job_id}")
    return status, time.time() - start


def norm(text: object) -> str:
    value = str(text or "").lower()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def token_f1(answer: str, gold: str) -> float:
    answer_tokens = norm(answer).split()
    gold_tokens = norm(gold).split()
    if not answer_tokens or not gold_tokens:
        return 0.0
    answer_counts = Counter(answer_tokens)
    gold_counts = Counter(gold_tokens)
    overlap = sum((answer_counts & gold_counts).values())
    if overlap == 0:
        return 0.0
    precision = overlap / len(answer_tokens)
    recall = overlap / len(gold_tokens)
    return 2 * precision * recall / (precision + recall)


def score_answer(answer: str, gold: str, facts: list[dict]) -> dict:
    n_answer = norm(answer)
    n_gold = norm(gold)
    facts_text = "\n".join(str(f.get("text", "")) for f in facts)
    return {
        "normalized_exact": bool(n_answer and n_answer == n_gold),
        "contains_gold": bool(n_gold and n_gold in n_answer),
        "gold_contains_answer": bool(n_answer and n_answer in n_gold),
        "token_f1": token_f1(answer, gold),
        "support_fact_contains_gold": bool(n_gold and n_gold in norm(facts_text)),
    }


def run_case(case: dict, run_index: int) -> dict:
    case_id = case["case_id"]
    result = {
        "case_id": case_id,
        "run_index": run_index,
        "question_type": case["question_type"],
        "question": case["question"],
        "gold_answer": case["gold_answer"],
        "selected_session_count": case["selected_session_count"],
        "selected_turn_count": case["selected_turn_count"],
        "events": [],
        "status": "running",
    }
    out_path = RESULTS_DIR / f"{run_index:02d}_{case_id}.json"

    clear = post(f"/v1/users/{USER}/memory:clear", idem=f"lme-batch-clear-{case_id}-{uuid.uuid4().hex}")
    clear_status, elapsed = wait_job(clear["job_id"], 360)
    result["events"].append({"step": "clear", "job_id": clear["job_id"], "status": clear_status, "elapsed_sec": elapsed})
    save_json(out_path, result)
    if clear_status["status"] != "succeeded":
        result["status"] = "failed_clear"
        save_json(out_path, result)
        return result

    for idx, session in enumerate(case["sessions"], start=1):
        session_id = quote(session["session_id"], safe="")
        ingest = post(
            f"/v1/users/{USER}/sessions/{session_id}:ingest",
            body={"turns": session["turns"], "metadata": session["metadata"]},
            idem=f"lme-batch-ingest-{case_id}-{idx}-{uuid.uuid4().hex}",
        )
        ingest_status, elapsed = wait_job(ingest["job_id"], 1200)
        result["events"].append(
            {
                "step": "ingest",
                "index": idx,
                "session_id": session["session_id"],
                "source_session_id": session["source_session_id"],
                "job_id": ingest["job_id"],
                "status": ingest_status,
                "elapsed_sec": elapsed,
            }
        )
        save_json(out_path, result)
        print(json.dumps({"case": case_id, "step": "ingest", "index": idx, "status": ingest_status["status"], "elapsed_sec": round(elapsed, 1)}), flush=True)
        if ingest_status["status"] != "succeeded":
            result["status"] = "failed_ingest"
            result["failed_session_id"] = session["session_id"]
            save_json(out_path, result)
            return result

    query_start = time.time()
    query = post(
        f"/v1/users/{USER}/query",
        body={"question": case["question"], "top_k": 8, "max_facts": 8, "trace_requested": True},
    )
    query_elapsed = time.time() - query_start
    answer = str(query.get("answer") or "")
    scores = score_answer(answer, case["gold_answer"], query.get("facts") or [])
    result["events"].append({"step": "query", "elapsed_sec": query_elapsed, "response": query, "scores": scores})
    result["answer"] = answer
    result["scores"] = scores
    result["status"] = "succeeded"
    save_json(out_path, result)
    return result


def aggregate(results: list[dict], metadata: dict) -> dict:
    attempted = len(results)
    completed = [r for r in results if r.get("status") == "succeeded"]
    failed = [r for r in results if r.get("status") != "succeeded"]
    ingests = [event["elapsed_sec"] for r in results for event in r.get("events", []) if event.get("step") == "ingest"]
    clears = [event["elapsed_sec"] for r in results for event in r.get("events", []) if event.get("step") == "clear"]
    queries = [event["elapsed_sec"] for r in results for event in r.get("events", []) if event.get("step") == "query"]
    by_type: dict[str, dict] = {}
    for question_type in sorted({r["question_type"] for r in results}):
        rows = [r for r in results if r["question_type"] == question_type]
        scored = [r for r in rows if r.get("status") == "succeeded"]
        by_type[question_type] = {
            "attempted": len(rows),
            "succeeded": len(scored),
            "contains_gold": sum(1 for r in scored if r.get("scores", {}).get("contains_gold")),
            "support_fact_contains_gold": sum(1 for r in scored if r.get("scores", {}).get("support_fact_contains_gold")),
            "avg_token_f1": statistics.mean([r.get("scores", {}).get("token_f1", 0.0) for r in scored]) if scored else 0.0,
        }
    return {
        "metadata": metadata,
        "attempted_cases": attempted,
        "completed_cases": len(completed),
        "failed_cases": len(failed),
        "proxy_contains_gold_success": sum(1 for r in completed if r.get("scores", {}).get("contains_gold")),
        "proxy_exact_success": sum(1 for r in completed if r.get("scores", {}).get("normalized_exact")),
        "support_fact_contains_gold": sum(1 for r in completed if r.get("scores", {}).get("support_fact_contains_gold")),
        "avg_token_f1": statistics.mean([r.get("scores", {}).get("token_f1", 0.0) for r in completed]) if completed else 0.0,
        "latency_sec": {
            "clear_avg": statistics.mean(clears) if clears else None,
            "ingest_avg": statistics.mean(ingests) if ingests else None,
            "ingest_p50": statistics.median(ingests) if ingests else None,
            "query_avg": statistics.mean(queries) if queries else None,
        },
        "by_question_type": by_type,
        "failures": [{"case_id": r["case_id"], "status": r.get("status"), "events": r.get("events", [])[-2:]} for r in failed],
    }


def main() -> None:
    metadata = {
        "api_version": get("/version"),
        "user_id": USER,
        "case_count": len(CASES),
        "model_stack": {
            "chat_deployment": "gpt-5.4",
            "chat_model_version": "2026-03-05",
            "embedding_deployment": "text-embedding-3-large",
            "embedding_dimension": 3072,
            "embedding_model_version": "text-embedding-3-large-1",
            "search_index": "memforest-recall-large",
        },
        "scoring": "proxy: normalized exact, normalized contains-gold, token F1, and supporting-fact contains-gold; not official LongMemEval LLM-judge scoring",
    }
    save_json(RUN / "run_metadata.json", metadata)

    results: list[dict] = []
    for idx, case in enumerate(CASES, start=1):
        print(json.dumps({"case": case["case_id"], "index": idx, "question_type": case["question_type"], "turns": case["selected_turn_count"]}), flush=True)
        result = run_case(case, idx)
        results.append(result)
        print(json.dumps({"case": case["case_id"], "status": result["status"], "answer": result.get("answer"), "scores": result.get("scores")}), flush=True)
        save_json(RUN / "batch_results_partial.json", results)
        save_json(RUN / "aggregate_partial.json", aggregate(results, metadata))

    save_json(RUN / "batch_results.json", results)
    save_json(RUN / "aggregate.json", aggregate(results, metadata))


if __name__ == "__main__":
    main()
