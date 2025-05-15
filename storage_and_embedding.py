#!/usr/bin/env python3
# store_and_embed.py
# batch-embed emails and conversations, store vectors in pgvector

import os
import sys
import json
import time
import logging
from pathlib import Path
from email.utils import parsedate_to_datetime
from typing import List, Dict, Tuple

import psycopg2
from psycopg2.extras import Json, execute_values
from sentence_transformers import SentenceTransformer

# --- logging setup ----------------------------------------------------------
log_level = logging.DEBUG if os.getenv("MAILMULE_DEBUG") else logging.INFO
logging.basicConfig(
    level=log_level,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("mailmule")

# --- constant config --------------------------------------------------------
batch_size: int = 64
model_name: str = "BAAI/bge-m3"
json_path: Path = Path("server_client_local_files/preprocessed_emails.json")

email_db_cfg = dict(
    host=os.getenv("PGHOST", "localhost"),
    port=os.getenv("PGPORT", "5432"),
    user=os.getenv("PGUSER", "mailmule"),
    password=os.getenv("PGPASSWORD", "159753"),
    dbname=os.getenv("PGDATABASE", "mailmule_db"),
)

conversation_db_cfg = dict(email_db_cfg, dbname=os.getenv("PGCONV_DB", "mailmule_conv_db"))

# --- ddl strings ------------------------------------------------------------
email_ddl = """
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

conversation_ddl = """
create extension if not exists vector;
create table if not exists conversations (
    conversation_id text primary key,
    email_count     int,
    embedding       vector({dim})
);
"""

email_upsert_sql = """
insert into emails
(id, conversation_id, subject, sender, date,
 order_in_conv, content, raw, embedding)
values %s
on conflict (id) do nothing;
"""

conversation_upsert_sql = """
insert into conversations
(conversation_id, email_count, embedding)
values %s
on conflict (conversation_id) do update
set email_count = excluded.email_count,
    embedding    = excluded.embedding;
"""

# --- helpers ----------------------------------------------------------------
def open_db(cfg: dict) -> psycopg2.extensions.connection:
    # open postgres connection
    return psycopg2.connect(**cfg)

def ensure_schema(conn, ddl: str, dim: int) -> None:
    # create extension and tables
    with conn, conn.cursor() as cur:
        cur.execute(ddl.format(dim=dim))

def fetch_existing_email_ids(conn) -> set[str]:
    # ids already stored
    with conn.cursor() as cur:
        cur.execute("select id from emails")
        return {row[0] for row in cur.fetchall()}

def fetch_existing_conversations(conn, conv_ids: List[str]) -> Dict[str, Tuple[List[float], int]]:
    # existing conversations vectors and counts
    if not conv_ids:
        return {}
    with conn.cursor() as cur:
        execute_values(
            cur,
            "select conversation_id, embedding, email_count from conversations where conversation_id in %s",
            [tuple(conv_ids)],
            fetch=True,
        )
        return {cid: (vec, cnt) for cid, vec, cnt in cur.fetchall()}  # type: ignore

def strip_label(text: str | None) -> str | None:
    # remove "Label: " from beginning
    if not text:
        return text
    return text.split(":", 1)[-1].strip() if ":" in text[:15] else text

def safe_int(value) -> int | None:
    # parse int or none
    try:
        return int(value)
    except Exception:
        return None

def batch_encode(model: SentenceTransformer, texts: List[str]) -> List[List[float]]:
    # encode in batches
    vectors: List[List[float]] = []
    total = len(texts)
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        chunk = texts[start:end]
        log.debug("encoding %d‒%d / %d", start + 1, end, total)
        try:
            vectors.extend(model.encode(chunk, normalize_embeddings=True).tolist())
        except Exception as err:
            log.error("batch encode failed: %s", err)
            # fallback to single item encode
            for txt in chunk:
                try:
                    vectors.append(model.encode(txt, normalize_embeddings=True).tolist())
                except Exception as sub_err:
                    log.error("single encode failed, substituting zeros: %s", sub_err)
                    vectors.append([0.0] * model.get_sentence_embedding_dimension())
    return vectors

# --- json load --------------------------------------------------------------
def load_emails(path: Path) -> List[dict]:
    # load json and flatten
    log.info("loading %s", path)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as err:
        log.error("failed reading json: %s", err)
        return []
    emails: List[dict] = []
    for conv in data:
        conv_id = strip_label(conv.get("conversation_id"))
        for em in conv.get("emails", []):
            em["conversation_id"] = conv_id
            emails.append(em)
    return emails

# --- main -------------------------------------------------------------------
def main() -> None:
    start_time = time.time()

    # load json
    emails = load_emails(json_path)
    if not emails:
        log.warning("no emails loaded, exiting")
        return
    log.info("emails in file: %d", len(emails))

    # init model
    log.info("loading model %s", model_name)
    model = SentenceTransformer(model_name, trust_remote_code=True, device="cpu")
    embed_dim = model.get_sentence_embedding_dimension()

    # db setup
    log.info("connecting databases")
    try:
        email_db = open_db(email_db_cfg)
        conversation_db = open_db(conversation_db_cfg)
    except Exception as err:
        log.error("db connect failed: %s", err)
        return

    ensure_schema(email_db, email_ddl, embed_dim)
    ensure_schema(conversation_db, conversation_ddl, embed_dim)

    # filter new emails
    stored_ids = fetch_existing_email_ids(email_db)
    new_emails = [e for e in emails if strip_label(e.get("id")) not in stored_ids]
    log.info("new emails: %d", len(new_emails))
    if not new_emails:
        log.info("nothing to embed, done")
        return

    # build texts
    email_texts: List[str] = []
    for em in new_emails:
        subject = em.get("subject") or ""
        body = em.get("content") or ""
        email_texts.append(f"{subject} {body}".strip())

    # encode
    vectors = batch_encode(model, email_texts)

    # prep rows and convo aggregates
    email_rows = []
    convo_vectors: Dict[str, List[List[float]]] = {}
    for em, vec in zip(new_emails, vectors):
        email_id = strip_label(em.get("id"))
        conv_id = strip_label(em.get("conversation_id"))
        subject = strip_label(em.get("subject"))
        sender = strip_label(em.get("from"))
        date_val = parsedate_to_datetime(strip_label(em.get("date") or "")) if em.get("date") else None
        order_val = safe_int(em.get("order"))

        email_rows.append(
            (
                email_id,
                conv_id,
                subject,
                sender,
                date_val,
                order_val,
                em.get("content"),
                Json(em),
                vec,
            )
        )
        convo_vectors.setdefault(conv_id, []).append(vec)

    # upsert emails
    try:
        with email_db, email_db.cursor() as cur:
            execute_values(cur, email_upsert_sql, email_rows, page_size=500)
            log.info("email rows upserted: %d", len(email_rows))
    except Exception as err:
        log.error("email upsert failed: %s", err)

    # update conversations
    existing_conv = fetch_existing_conversations(conversation_db, list(convo_vectors.keys()))
    convo_rows = []
    for cid, new_vec_list in convo_vectors.items():
        new_count = len(new_vec_list)
        new_sum = [sum(col) for col in zip(*new_vec_list)]

        if cid in existing_conv:
            old_vec, old_count = existing_conv[cid]
            total = old_count + new_count
            merged_vec = [
                (old_val * old_count + add_val) / total
                for old_val, add_val in zip(old_vec, new_sum)
            ]
            convo_rows.append((cid, total, merged_vec))
        else:
            avg_vec = [val / new_count for val in new_sum]
            convo_rows.append((cid, new_count, avg_vec))

    try:
        with conversation_db, conversation_db.cursor() as cur:
            execute_values(cur, conversation_upsert_sql, convo_rows, page_size=200)
            log.info("conversation rows upserted: %d", len(convo_rows))
    except Exception as err:
        log.error("conversation upsert failed: %s", err)

    elapsed = time.time() - start_time
    log.info("done in %.1f seconds", elapsed)

# entry
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("interrupted by user")
