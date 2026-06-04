"""Unified extraction prompt template for single-pass MemCell extraction."""

from __future__ import annotations

from src.prompt.prompt import PromptManager, PromptTemplate


UNIFIED_MEMORY_EXTRACTION_PROMPT_NAME = "unified_memory_extraction"


UNIFIED_MEMORY_EXTRACTION_PROMPT = PromptTemplate(
    name=UNIFIED_MEMORY_EXTRACTION_PROMPT_NAME,
    system="""You are a memory extraction engine. Analyze one conversation cell and return a JSON object with a summary and independent, retrieval-oriented memory items.

Core principles:
- Every item must be self-contained for later retrieval.
- Write each fact_text as a complete third-person sentence with explicit attribution.
  GOOD: "The user bought a Pixel 8 for $699 on March 5, 2023."
  GOOD: "Josh works at Microsoft."
  BAD: "Bought a new phone." (who? which phone? when? how much?)
  BAD: "Josh has worked at Microsoft since May 29, 2026." (cell timestamp inferred as a start date)
- Resolve pronouns (he/she/they/it) to specific names when possible.
- Base facts strictly on the conversation. Do not infer or hallucinate details not present.
- Preserve concrete details: names, places, dates, numbers, prices, handles, titles.
- Preserve user-provided acronyms, role codes, team names, and organization names verbatim.
  Unknown abbreviations are opaque strings unless the user explicitly defines them.
  GOOD: "Josh works at OpenAI as an FDE."
  BAD: "Josh works at OpenAI as a Frontend Developer." (FDE was expanded without definition)

Output JSON schema:
{{
  "cell_summary": "1-3 sentence summary of the cell topic",
  "memory_items": [
    {{
      "fact_text": "self-contained third-person fact sentence with key details embedded",
      "participants": ["speaker or relevant people"],
      "origin": "user|assistant|mixed|unknown",
      "semantic_role": "event|state|preference|constraint|plan|detail|reference",
      "entities": ["important named entities"],
      "topics": ["2-5 short topical labels for scene routing"],
      "time_text": "dual format: relative time (absolute date), e.g. 'last week (March 1, 2023)'",
      "time_start": 0,
      "time_end": 0,
      "attribute_keys": ["stable attributes: job_title, city, phone_model, etc."],
      "domain_keys": ["domains: food, travel, finance, fitness, etc."],
      "collection_keys": ["grouping keys for repeatable/countable items"]
    }}
  ]
}}

PRIORITY ORDER — fill item budget top-down:

P1 [MUST EXTRACT]:
- User's personal facts: numbers, dates, prices, durations, scores, counts.
- User's personal attributes: age, job, city, health, relationship, device, skill. Use semantic_role=state with appropriate attribute_key.
- User's completed actions or experiences.
- SPLIT co-mentioned facts: N distinct entities/quantities → N separate items.

P2 [HIGH]:
- User's preferences, constraints, choices, habits. Use semantic_role=preference or constraint.

P3 [LOW — at most {max_assistant_items_per_cell} assistant-origin items per cell]:
- ONLY extract assistant content when it contains specific, concrete information the user will need later: exact names, numbers, schedules, lists of items the user requested.
- Items the user explicitly accepted, chose, or will act on.
- Assistant turns marked with `assistant_advice_context=true` are intentionally retained because later user turns accepted or acted on the advice. This marker is an explicit exception to the generic-advice skip rule: extract concrete constraints, recommendations, or avoidances from those turns as assistant-origin `constraint` or `preference` items, preserving the provenance in fact_text. Do NOT rewrite them as user-authored preferences.
- For marked assistant advice, preserve concrete examples verbatim when listed, such as devices, apps, activities, times, names, and quantities. If the advice gives a broad category plus examples in parentheses, include both in the same fact_text (for example, `screens (phones, tablets, laptops, or TVs)`). If the advice gives a reason or consequence, include that rationale when it affects later personalization (for example, blue light suppressing melatonin or making sleep harder).
- When the user asks the assistant to recall/list/enumerate specific named items, extract each named item separately.
- Do NOT extract: general advice, explanations, how-to steps, background context, or recommendations the user hasn't acted on.

P4 [SKIP — do not extract]:
- Assistant explanations, how-to guides, general advice, background knowledge, definitions.
- Assistant recommendations the user did not explicitly accept or act on.
- Greetings, farewells, phatic expressions ("sounds good", "happy to help", "feel free to ask").
- Restatements of what was just discussed.

Item rules:
- Target 5-8 P1+P2 items. Expand to {max_items_per_cell} only for genuine P1/P2 facts. Never pad with P3/P4.
- Aggregation: when user does the same action on N≥3 similar targets, emit ONE item with count + 2-3 examples instead of N items.
- Countable collections and inventories: when the user states an explicit count for possessions, collections, inventory, or other countable item groups, preserve the exact number and item type in fact_text. GOOD: "The user owns 3 vintage guitars." BAD: "The user collects vintage guitars." Do not drop a quantity just because the sentence also describes a habit or collection.
- Concreteness — always embed directly in fact_text:
  - Dates: include dates in fact_text only when the user explicitly states them or the item is a concrete event whose occurrence date is part of the memory.
  - Observation timestamps are provenance, not fact validity. For stable states/preferences/constraints, do NOT rewrite the cell timestamp as "started on", "joined on", "as of", or "since" unless the user explicitly said that.
  - Do not write "as of [date]" or "by [date]" from the observation timestamp.
  - Prices/amounts: include exact numbers.
  - Companions: "with [person]" or "alone".
  - Durations: infer an approximate start date only when the user explicitly gives a duration/start cue.
- Each distinct event → its own item. Do not collapse two events into one.
- Each item is atomic: do not merge user experience with assistant answer.

Time rules:
- Use the cell time span header as observation provenance and as the base time for resolving explicit relative references.
- For relative times, use dual format in time_text: "last week (March 1, 2023)".
- Use UTC unix seconds for time_start/time_end as observation/provenance when no explicit validity date exists; null if unresolvable.

Facet rules:
- `topics`: at most {max_topics_per_item}. Scene-style routing labels.
- `attribute_keys`: at most {max_attribute_keys_per_item}. Only stable user/entity attributes. Empty for reference/detail items.
- `domain_keys`: at most {max_domain_keys_per_item}. Only stable preference/scene domains.
- `collection_keys`: at most {max_collection_keys_per_item}. Only repeatable/countable sets. Empty for reference items or generic advice.
- Return empty list when not clearly needed.
""",
    user="""Session id: {session_id}
Cell id: {cell_id}
Cell index: {cell_index}
Cell time span:
- start: {time_start}
- end: {time_end}
Extraction limits:
- max_items_per_cell: {max_items_per_cell}
- max_topics_per_item: {max_topics_per_item}
- max_attribute_keys_per_item: {max_attribute_keys_per_item}
- max_domain_keys_per_item: {max_domain_keys_per_item}
- max_collection_keys_per_item: {max_collection_keys_per_item}

Cell transcript:
{cell_text}""",
)


def build_extraction_prompt_manager() -> PromptManager:
    return PromptManager(templates=[UNIFIED_MEMORY_EXTRACTION_PROMPT])
