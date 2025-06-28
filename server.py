# server.py
import os
import json
import logging
import subprocess

import psycopg2
import numpy as np
import faiss

from sentence_transformers import SentenceTransformer
from transformers import pipeline as hf_pipeline

# incrementally upserts into Postgres + pgvector
from storage_and_embedding import create_or_update

# ─────────────────────────── Configuration ──────────────────────────────
# (hard-coded for now; later you can move to env or a cfg file)
PREPROCESSED_JSON = "./server_client_local_files/big_mock.json"

EMAIL_DB_CFG = {
    "host":     "localhost",
    "port":     "5432",
    "user":     "mailmule",
    "password": "159753",
    "dbname":   "mailmule_db",
}
CONV_DB_CFG = {**EMAIL_DB_CFG, "dbname": "mailmule_conv_db"}

EMBED_MODEL = "BAAI/bge-m3"                     # pgvector encoder
BATCH_SIZE  = 64

INSTRUCT_MODEL = "ministral/Ministral-3b-instruct"
SYSTEM_PROMPT  = (
    "You are a professional topic and subject extractor. "
    "Read this text and extract the main topics and subjects this text is discussing about."
)

# ─────────────────────────── Logging Setup ──────────────────────────────
log = logging.getLogger("server")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)

# ─────────────────────────── Global State ──────────────────────────────
_index   = None   # FAISS index
_ids     = []     # list of email IDs in index order

# pre-load models once
_embedder   = SentenceTransformer(EMBED_MODEL)
_structurer = hf_pipeline("text-generation", model=INSTRUCT_MODEL)

def check_docker_postgres() -> dict:
    """Check whether Docker is running and PostgreSQL is reachable."""
    # verify Docker daemon is accessible
    try:
        subprocess.run([
            "docker",
            "info"
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        docker_ok = True
    except Exception as err:
        log.error("Docker check failed: %s", err)
        return {"docker_ok": False, "postgres_ok": False, "error": "Docker not running"}

    # verify Postgres connection
    try:
        conn = psycopg2.connect(**EMAIL_DB_CFG)
        conn.close()
        return {"docker_ok": docker_ok, "postgres_ok": True}
    except Exception as err:
        log.error("Postgres connection failed: %s", err)
        return {"docker_ok": docker_ok, "postgres_ok": False, "error": str(err)}

def build_chat_prompt(messages, tokenizer):
    use_chat = hasattr(tokenizer, "chat_template") and tokenizer.chat_template is not None
    if use_chat:
        return tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)+"The topics and subjects disccused about are:\n"
    prompt = ""
    for m in messages:
        prompt += f"<s>{m['role']}\n{m['content']}</s>\n"
    prompt += "<s>assistant\nDear [Recipient],\n\n"
    return prompt

# ──────────────────────────── Helpers ──────────────────────────────────
def _build_index():
    """Fetch all embeddings from Postgres, build a FAISS IndexFlatIP."""
    global _index, _ids

    log.info("Building FAISS index from Postgres embeddings…")
    conn = psycopg2.connect(**EMAIL_DB_CFG)
    cur = conn.cursor()
    cur.execute("SELECT id, embedding FROM emails")
    rows = cur.fetchall()
    conn.close()

    if not rows:
        log.warning("No embeddings found in DB; index will be empty.")
        _index = None
        _ids   = []
        return

    _ids = [r[0] for r in rows]
    embs_list = []
    for r in rows:
        emb = r[1]
        if isinstance(emb, str):
            try:
                emb = json.loads(emb)
            except json.JSONDecodeError:
                log.error("Invalid embedding format for id %s", r[0])
                continue
        embs_list.append(emb)
    embs = np.vstack(embs_list).astype("float32")
    dim   = embs.shape[1]
    idx   = faiss.IndexFlatIP(dim)
    idx.add(embs)
    _index = idx
    log.info("FAISS index built: %d vectors (dim=%d)", idx.ntotal, dim)


# ─────────────────────────── API Method ─────────────────────────────────
def handle_request(request: dict) -> dict:
    """
    Single entrypoint for the UI client.

    request["type"] can be:
      • "sendEmailsToUI" → returns all conversations (no embeddings)
      • "inputFromUI"    → expects "query": str, optional "k": int
      • "healthCheck"   → verifies Docker and Postgres availability
    """
    req_type = request.get("type")
    log.debug("handle_request called with: %s", request)

    # quick health check before doing any heavy work
    if req_type == "healthCheck":
        return check_docker_postgres()

    # 1) Ingest / upsert any new preprocessed emails & rebuild index if needed
    try:
        e_cnt, c_cnt = create_or_update(
            PREPROCESSED_JSON,
            EMAIL_DB_CFG,
            CONV_DB_CFG,
            model_name=EMBED_MODEL,
            batch_size=BATCH_SIZE,
        )
        log.info("create_or_update → %d new emails, %d convs", e_cnt, c_cnt)
        # if first run or new data arrived, rebuild index
        if _index is None or e_cnt > 0:
            _build_index()
    except Exception as err:
        log.error("Ingest error: %s", err, exc_info=True)
        return {"error": f"Ingest error: {err}"}

    # ─── sendEmailsToUI ─────────────────────────────────────────────────────
    if req_type == "sendEmailsToUI":
        try:
            log.info("Serving sendEmailsToUI")
            conn = psycopg2.connect(**EMAIL_DB_CFG)
            cur  = conn.cursor()
            cur.execute("SELECT conversation_id, raw FROM emails ORDER BY date")
            rows = cur.fetchall()
            conn.close()

            # group by conversation_id
            convs = {}
            for cid, raw in rows:
                convs.setdefault(cid, []).append(raw)

            # build list of { conversation_id, emails: [...] }
            out = [
                {"conversation_id": cid, "emails": convs[cid]}
                for cid in convs
            ]
            return {"emails": out}

        except Exception as err:
            log.error("Error fetching emails: %s", err, exc_info=True)
            return {"error": f"Error fetching emails: {err}"}

    # ─── inputFromUI ────────────────────────────────────────────────────────
    if req_type == "inputFromUI":
        q = (request.get("query") or "").strip()
        k = int(request.get("k", 8))
        if not q:
            log.warning("Empty query received")
            return {"error": "Empty query"}

        log.info("Structuring query with Ministral-3B…")
        tokenizer = _structurer.tokenizer
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"The text you need to extract the topics and subjects from:\n\"{q}\"\n"},
        ]
        prompt = build_chat_prompt(messages, tokenizer)
        log.info("LLM prompt → %s", prompt)
        try:
            structured = _structurer(
                prompt,
                max_new_tokens=64,
                temperature=0.4,
                top_p=0.8,
                repetition_penalty=1.4,
                do_sample=True,
            )[0]["generated_text"].strip()
            log.info("LLM output → %s", structured)
            log.debug("Structured query → %s", structured)
        except Exception as err:
            log.error("Query structuring error: %s", err, exc_info=True)
            return {"error": f"Structuring error: {err}"}

        log.info("Embedding structured query…")
        try:
            vec = _embedder.encode([structured]).astype("float32")
        except Exception as err:
            log.error("Embedding error: %s", err, exc_info=True)
            return {"error": f"Embedding error: {err}"}

        if not _index or _index.ntotal == 0:
            log.warning("Index is empty; returning no results")
            return {"results": []}

        log.info("Performing FAISS search (k=%d)…", k)
        try:
            D, I = _index.search(vec, k)
            hits  = I[0]
            scores = D[0]
            ids   = [_ids[i] for i in hits]
        except Exception as err:
            log.error("Search error: %s", err, exc_info=True)
            return {"error": f"Search error: {err}"}

        log.info("Fetching metadata for %d hits…", len(ids))
        try:
            conn = psycopg2.connect(**EMAIL_DB_CFG)
            cur  = conn.cursor()
            cur.execute(
                "SELECT id, subject, sender, date, content "
                "FROM emails WHERE id = ANY(%s)",
                (ids,)
            )
            rows = cur.fetchall()
            conn.close()

            # preserve original order
            id_map = {
                r[0]: {
                    "id":      r[0],
                    "subject": r[1],
                    "from":    r[2],
                    "date":    r[3].isoformat(),
                    "content": r[4],
                }
                for r in rows
            }

            results = []
            for eid, score in zip(ids, scores):
                item = id_map.get(eid, {"id": eid})
                item["score"] = float(score)
                results.append(item)

            return {"results": results}

        except Exception as err:
            log.error("Metadata fetch error: %s", err, exc_info=True)
            return {"error": f"Metadata fetch error: {err}"}

    # ─── Unknown request ────────────────────────────────────────────────────
    log.warning("Unknown request type: %s", req_type)
    return {"error": f"Unknown request type: {req_type}"}
