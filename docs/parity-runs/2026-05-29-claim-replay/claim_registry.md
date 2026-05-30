# Claim registry

This registry is seeded from extracted PDF text plus in-repo code/docs evidence. Claims are not marked human-confirmed paper claims unless a source quote is included in the report.

| ID | Category | Claim | Evidence | Confidence | Verifiability |
| --- | --- | --- | --- | --- | --- |
| EX-01 | extraction | Conversations are chunked into MemCells before extraction; each MemCell is a contiguous local memory unit within one session. | src/utils/types.py:41-58<br>src/extraction/chunker.py:49-55 | high | code-testable |
| EX-02 | extraction | Single-pass extraction produces MemoryItems with fact text, origin, semantic role, entities, topics, time anchors, and key metadata. | src/utils/types.py:62-85<br>src/extraction/pipeline.py:170-230 | high | code-testable |
| FM-01 | facts | MemForest maintains a canonical fact store backed by exact match plus FAISS vector deduplication. | src/extraction/fact_manager.py:36<br>src/extraction/fact_manager.py:76 | high | code-testable |
| FM-02 | facts | Deduplication proceeds through exact match, vector candidate retrieval, and LLM equivalence judging. | src/extraction/fact_manager.py:180-250 | high | code-testable |
| TS-01 | trees | MemForest stores extracted facts through three complementary tree views: Session, Entity, and Scene. | README.md:27-33<br>src/build/tree_types.py:41-58 | high | code-testable |
| TS-02 | trees | Session trees preserve source order and provide fallback access to original interaction context. | src/build/tree_types.py:26-37<br>paper extracted text lines around 910-918 | high | code-testable |
| TS-03 | trees | Entity trees group evidence about recurring subjects; the user-origin tree covers user-origin facts. | src/build/entity_router.py:37-52<br>src/config/default.yaml:60-61 | high | code-testable |
| TS-04 | trees | Scene trees group semantically coherent situations/topics via online clustering. | src/build/scene_router.py:36-80<br>src/build/tree_types.py:64-79 | high | code-testable |
| TP-01 | temporal | Tree nodes carry time_start/time_end spanning descendants for temporal organization. | src/build/tree_types.py:15-16 | high | code-testable |
| UP-01 | updates | Incremental ingest inserts new evidence and marks affected nodes dirty instead of rebuilding all memory conceptually. | src/build/tree_builder.py:1-10<br>src/build/tree_types.py:59-61 | high | code-testable |
| UP-02 | updates | Summary reuse via content-hash cache avoids re-summarizing unchanged nodes. | src/forest/memforest.py:170-202<br>src/forest/memforest.py:164-167 | high | code-testable |
| RE-01 | retrieval | Query pipeline follows Recall, Plan, Browse, Rerank, and Answer stages. | src/query/pipeline.py:7-9<br>src/config/query_config.py:89-95 | high | code-testable |
| RE-02 | retrieval | Local recall supports top-down, bottom-up, and union directions. | src/query/retriever.py:1-11<br>src/config/query_config.py:22-26 | high | code-testable |
| RE-07 | retrieval | Browsing uses global beam search from root to L0 leaves. | src/query/browser.py:1-18<br>src/query/browser.py:119-158 | high | code-testable |
| RE-10 | retrieval | Fact reranking is embedding cosine similarity over stored fact embeddings, with no LLM call in local reranker. | src/query/reranker.py:1-8<br>src/query/reranker.py:62-97 | high | code-testable |
| EV-01 | evaluation | Evaluation artifacts cover LoCoMo and LongMemEval variants with frozen per-question CSVs and baseline summaries. | benchmark/*.csv<br>benchmark/baseline_summary.json | medium | artifact-testable |
| SY-01 | system | MemForest isolates users via per-user UserForest objects and supports parallel querying across users. | src/forest/user_forest.py:83-90<br>src/forest/memforest.py:363-404 | high | code-testable |