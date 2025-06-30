#!/usr/bin/env python3
# mailmule_store.py – unified pgvector email/conversation loader
#
# Public API
#  * create_all(json_path, email_db_cfg, conv_db_cfg, …)
#  * update_all(json_path, email_db_cfg, conv_db_cfg, …)
#  * create_or_update(json_path, email_db_cfg, conv_db_cfg, …)
#
# each returns (emails_written:int, conversations_written:int).
#
# running this file directly chooses create_or_update() automatically.

from __future__ import annotations

import json
import os
import time
import logging
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import psycopg2
from psycopg2.extras import Json, execute_values
from sentence_transformers import SentenceTransformer

# ────────────────────────────── logging ───────────────────────────────────────
log = logging.getLogger("mailmule")
if not log.handlers:
    level = logging.DEBUG if os.getenv("MAILMULE_DEBUG") else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )

# ────────────────────────────── DDL / SQL ─────────────────────────────────────
EMAIL_DDL = """
create extension if not exists vector;
create table if not exists emails (
    id              text primary key,
    conversation_id text,
    subject         text,
    sender          text,
    date            timestamptz,
    order_in_conv   int,
    content         text,
    raw             jsonb,
    embedding       vector({dim})
);
"""

CONV_DDL = """
create extension if not exists vector;
create table if not exists conversations (
    conversation_id text primary key,
    email_count     int,
    embedding       vector({dim})
);
"""

EMAIL_UPSERT = """
insert into emails
(id, conversation_id, subject, sender, date,
 order_in_conv, content, raw, embedding)
values %s
on conflict (id) do nothing;
"""

CONV_UPSERT = """
insert into conversations
(conversation_id, email_count, embedding)
values %s
on conflict (conversation_id) do update
set email_count = excluded.email_count,
    embedding    = excluded.embedding;
"""

# ────────────────────────────── helpers ───────────────────────────────────────
def db_connect(cfg: dict, retries: int = 3, backoff: float = 2.0):
    """
    Connect to Postgres with exponential-backoff retry.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            return psycopg2.connect(**cfg)
        except psycopg2.OperationalError as err:
            if attempt >= retries:
                log.error("DB connection failed after %d attempts: %s", attempt, err)
                raise
            sleep = backoff ** attempt
            log.warning("DB connection failed (attempt %d/%d) – retrying in %.1fs",
                        attempt, retries, sleep)
            time.sleep(sleep)


def table_exists(conn, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("select to_regclass(%s)", (name,))
        return cur.fetchone()[0] is not None


def ensure_schema(conn, ddl: str, dim: int) -> None:
    """
    Always run the CREATE IF NOT EXISTS DDL. Cheap, idempotent, and covers upgrades.
    """
    with conn, conn.cursor() as cur:
        cur.execute(ddl.format(dim=dim))


def strip_label(text: Optional[str]) -> Optional[str]:
    """
    Remove an optional 'Label: value' prefix produced by preprocess_emails_for_embeddings.
    """
    if not text:
        return text
    return text.split(":", 1)[-1].strip() if ":" in text[:15] else text


def safe_int(val):
    try:
        return int(val)
    except Exception:
        return None

# ────────────────────────────── JSON loader ──────────────────────────────────
def load_flat_emails(path: Path) -> List[dict]:
    """
    Flatten the conversation-centric JSON into a list of email dicts.
    """
    try:
        data = json.loads(path.read_text("utf-8"))
    except FileNotFoundError:
        log.error("JSON file %s not found – run the extractor first?", path)
        return []
    except json.JSONDecodeError as err:
        log.error("JSON file %s is malformed: %s", path, err)
        return []

    flat: List[dict] = []
    for conv in data:
        cid = strip_label(conv.get("conversation_id"))
        for em in conv.get("emails", []):
            em["conversation_id"] = cid
            flat.append(em)
    return flat

# ────────────────────────────── batching / enc ───────────────────────────────
def encode_batches(
    model: SentenceTransformer,
    texts: List[str],
    batch_size: int,
) -> List[List[float]]:
    """
    Robust batch-encoder: if a batch fails, fall back to per-item encode
    (copying pattern from both original scripts).
    """
    vecs: List[List[float]] = []
    for start in range(0, len(texts), batch_size):
        chunk = texts[start : start + batch_size]
        try:
            vecs.extend(model.encode(chunk, normalize_embeddings=True).tolist())
        except Exception as err:
            log.error("batch encode failed → reverting to single items: %s", err)
            for txt in chunk:
                try:
                    vecs.append(model.encode(txt, normalize_embeddings=True).tolist())
                except Exception:
                    vecs.append([0.0] * model.get_sentence_embedding_dimension())
    return vecs

def build_rows(
    model: SentenceTransformer,
    emails: List[dict],
    batch_size: int,
) -> Tuple[List[tuple], Dict[str, List[List[float]]]]:
    """
    Transform email dicts into:
        - email_rows  (list of tuples suitable for execute_values)
        - conv_vectors {conversation_id: [embedding, …]}
    """
    texts = []
    for em in emails:
        prompt = (
            f"You are an AI tasked with turning this email into a context-based vector, represent it for searching relevant documents and retrieval.\n "
            f"{em.get("from") if em.get("from") else "unknown sender"}.\n"
            f"{em.get("subject") if em.get("subject") else "No Title"}.\n"
            f"{em.get("content") if em.get("content") else "no content"}.\n"
        )
        texts.append(prompt)
    print (len(texts),"\n", texts[:5])  # Debug: print first 5 prompts
    vectors = encode_batches(model, texts, batch_size)

    email_rows: List[tuple] = []
    conv_vecs: Dict[str, List[List[float]]] = {}

    for em, vec in zip(emails, vectors):
        eid = strip_label(em.get("id"))
        cid = strip_label(em.get("conversation_id"))
        email_rows.append(
            (
                eid,
                cid,
                strip_label(em.get("subject")),
                strip_label(em.get("from") or em.get("sender")),
                parsedate_to_datetime(strip_label(em.get("date") or "")) if em.get("date") else None,
                safe_int(em.get("order")),
                em.get("content"),
                Json(em),
                vec,
            )
        )
        conv_vecs.setdefault(cid, []).append(vec)
    return email_rows, conv_vecs

def merge_conv_vectors(
    existing: Dict[str, Tuple[List[float], int]],
    incoming: Dict[str, List[List[float]]],
) -> List[tuple]:
    """
    Merge old + new vectors by weighted average.
    existing  = {conversation_id: (vector, email_count)}
    incoming  = {conversation_id: [vec, vec, …]}
    Returns rows ready for CONV_UPSERT.
    """
    rows: List[tuple] = []
    for cid, vecs in incoming.items():
        new_cnt = len(vecs)
        new_sum = [sum(col) for col in zip(*vecs)]

        if cid in existing:
            old_vec, old_cnt = existing[cid]
            tot = old_cnt + new_cnt
            merged = [(o * old_cnt + s) / tot for o, s in zip(old_vec, new_sum)]
            rows.append((cid, tot, merged))
        else:
            avg = [v / new_cnt for v in new_sum]
            rows.append((cid, new_cnt, avg))
    return rows

# ────────────────────────────── API functions ────────────────────────────────
def create_all(
    json_path: Path | str,
    email_db_cfg: dict,
    conv_db_cfg: dict,
    model_name: str = "BAAI/bge-m3",
    batch_size: int = 64,
) -> Tuple[int, int]:
    """
    First-time ingestion. If tables already exist, this automatically
    falls back to update_all() so callers don’t have to think about it.
    """
    # quick existence check so we can skip redundant work
    conn_check = db_connect(email_db_cfg, retries=1)
    if table_exists(conn_check, "emails"):
        log.info("Tables already exist – create_all() will run update_all() instead.")
        conn_check.close()
        return update_all(json_path, email_db_cfg, conv_db_cfg,
                          model_name=model_name, batch_size=batch_size)
    conn_check.close()

    emails = load_flat_emails(Path(json_path))
    if not emails:
        log.warning("create_all: 0 emails found in JSON – nothing to do.")
        return 0, 0

    model = SentenceTransformer(model_name, trust_remote_code=True, device="cpu")
    dim = model.get_sentence_embedding_dimension()

    email_db = db_connect(email_db_cfg)
    conv_db  = db_connect(conv_db_cfg)
    ensure_schema(email_db, EMAIL_DDL, dim)
    ensure_schema(conv_db,  CONV_DDL,  dim)

    email_rows, conv_vecs = build_rows(model, emails, batch_size)

    with email_db, email_db.cursor() as cur:
        execute_values(cur, EMAIL_UPSERT, email_rows, page_size=500)

    conv_rows = merge_conv_vectors({}, conv_vecs)
    with conv_db, conv_db.cursor() as cur:
        execute_values(cur, CONV_UPSERT, conv_rows, page_size=200)

    log.info("create_all: inserted %d emails, %d conversations",
             len(email_rows), len(conv_rows))
    return len(email_rows), len(conv_rows)


def update_all(
    json_path: Path | str,
    email_db_cfg: dict,
    conv_db_cfg: dict,
    model_name: str = "BAAI/bge-m3",
    batch_size: int = 64,
) -> Tuple[int, int]:
    """
    Incremental ingest: only new email IDs are embedded and stored.
    """
    emails = load_flat_emails(Path(json_path))
    if not emails:
        log.warning("update_all: 0 emails found in JSON – nothing to do.")
        return 0, 0

    model = SentenceTransformer(model_name, trust_remote_code=True, device="cpu")
    dim = model.get_sentence_embedding_dimension()

    email_db = db_connect(email_db_cfg)
    conv_db  = db_connect(conv_db_cfg)
    ensure_schema(email_db, EMAIL_DDL, dim)   # self-heal even on updates
    ensure_schema(conv_db,  CONV_DDL,  dim)

    # fetch existing email IDs
    with email_db.cursor() as cur:
        cur.execute("select id from emails")
        existing_ids = {row[0] for row in cur.fetchall()}

    new_emails = [e for e in emails if strip_label(e.get("id")) not in existing_ids]
    if not new_emails:
        log.info("update_all: database already up-to-date – no new emails.")
        return 0, 0

    email_rows, conv_vecs = build_rows(model, new_emails, batch_size)

    with email_db, email_db.cursor() as cur:
        execute_values(cur, EMAIL_UPSERT, email_rows, page_size=500)

    # merge + upsert conversation vectors
    if conv_vecs:
        cid_list = list(conv_vecs.keys())
        existing_conv: Dict[str, Tuple[List[float], int]] = {}
        with conv_db.cursor() as cur:
            cur.execute(
                "select conversation_id, embedding, email_count "
                "from conversations where conversation_id = any(%s)",
                (cid_list,),
            )
            existing_conv = {cid: (vec, cnt) for cid, vec, cnt in cur.fetchall()}

        conv_rows = merge_conv_vectors(existing_conv, conv_vecs)
        with conv_db, conv_db.cursor() as cur:
            execute_values(cur, CONV_UPSERT, conv_rows, page_size=200)
    else:
        conv_rows = []

    log.info("update_all: inserted %d new emails, updated %d conversations",
             len(email_rows), len(conv_rows))
    return len(email_rows), len(conv_rows)


def create_or_update(
    json_path: Path | str,
    email_db_cfg: dict,
    conv_db_cfg: dict,
    model_name: str = "BAAI/bge-m3",
    batch_size: int = 64,
) -> Tuple[int, int]:
    """
    Smart helper: if the tables exist → update, otherwise → create.
    """
    conn = db_connect(email_db_cfg, retries=1)
    is_init = not table_exists(conn, "emails")
    conn.close()
    if is_init:
        return create_all(json_path, email_db_cfg, conv_db_cfg,
                          model_name=model_name, batch_size=batch_size)
    return update_all(json_path, email_db_cfg, conv_db_cfg,
                      model_name=model_name, batch_size=batch_size)

# ────────────────────────────── CLI entrypoint ───────────────────────────────
if __name__ == "__main__":
    import argparse

    JSON_PATH = Path("server_client_local_files/mock_preprocessed_emails.json")
    EMAIL_DB_CFG = dict(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        user=os.getenv("PGUSER", "mailmule"),
        password=os.getenv("PGPASSWORD", "159753"),
        dbname=os.getenv("PGDATABASE", "mailmule_db"),
    )
    CONV_DB_CFG = dict(EMAIL_DB_CFG, dbname=os.getenv("PGCONV_DB", "mailmule_conv_db"))

    parser = argparse.ArgumentParser(description="MailMule pgvector loader")
    parser.add_argument("--create", action="store_true", help="force full create")
    parser.add_argument("--update", action="store_true", help="force incremental update")
    parser.add_argument("--json",   help="path to preprocessed_emails.json", default=JSON_PATH)

    args = parser.parse_args()
    json_path = Path(args.json)

    start = time.time()
    try:
        if args.create:
            e, c = create_all(json_path, EMAIL_DB_CFG, CONV_DB_CFG)
        elif args.update:
            e, c = update_all(json_path, EMAIL_DB_CFG, CONV_DB_CFG)
        else:
            e, c = create_or_update(json_path, EMAIL_DB_CFG, CONV_DB_CFG)

        log.info("done – %d emails, %d conversations (%.1fs)",
                 e, c, time.time() - start)
    except KeyboardInterrupt:
        log.warning("cancelled by user")
