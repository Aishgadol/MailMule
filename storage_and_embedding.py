#!/usr/bin/env python3
# store_and_embed.py
# modular toolkit for pgvector email-and-conversation stores

"""
public api
----------
create_or_update(json_path, email_db_cfg, conv_db_cfg, …)  # auto
create_all(json_path, email_db_cfg, conv_db_cfg, …)        # full ingest
update_all(json_path, email_db_cfg, conv_db_cfg, …)        # incremental

each returns (emails_written, conversations_written)
running this file directly calls create_or_update() with env-derived cfg
"""

from __future__ import annotations

import json
import os
import time
import logging
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import List, Dict, Tuple

import psycopg2
from psycopg2.extras import Json, execute_values
from sentence_transformers import SentenceTransformer

# --- logging -----------------------------------------------------------------
log = logging.getLogger("mailmule")
if not log.handlers:
    _lvl = logging.DEBUG if os.getenv("MAILMULE_DEBUG") else logging.INFO
    logging.basicConfig(
        level=_lvl,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )

# --- ddl strings -------------------------------------------------------------
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

# --- helpers -----------------------------------------------------------------
def open_db(cfg: dict):
    return psycopg2.connect(**cfg)

def table_exists(conn, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("select to_regclass(%s)", (name,))
        return cur.fetchone()[0] is not None

def ensure_schema(conn, ddl: str, dim: int) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(ddl.format(dim=dim))

def strip_label(text: str | None) -> str | None:
    if not text:
        return text
    return text.split(":", 1)[-1].strip() if ":" in text[:15] else text

def safe_int(val):
    try:
        return int(val)
    except Exception:
        return None

# --- json --------------------------------------------------------------------
def load_flat_emails(path: Path) -> List[dict]:
    data = json.loads(path.read_text("utf-8"))
    flat: List[dict] = []
    for conv in data:
        cid = strip_label(conv.get("conversation_id"))
        for em in conv.get("emails", []):
            em["conversation_id"] = cid
            flat.append(em)
    return flat

# --- batching ----------------------------------------------------------------
def encode_batches(
    model: SentenceTransformer,
    texts: List[str],
    batch_size: int,
) -> List[List[float]]:
    vecs: List[List[float]] = []
    for start in range(0, len(texts), batch_size):
        chunk = texts[start : start + batch_size]
        try:
            vecs.extend(model.encode(chunk, normalize_embeddings=True).tolist())
        except Exception as err:
            log.error("batch encode failed, fallback to singles: %s", err)
            for txt in chunk:
                try:
                    vecs.append(model.encode(txt, normalize_embeddings=True).tolist())
                except Exception:
                    vecs.append([0.0] * model.get_sentence_embedding_dimension())
    return vecs

# --- row builders ------------------------------------------------------------
def build_rows(
    model: SentenceTransformer,
    emails: List[dict],
    batch_size: int,
) -> tuple[List[tuple], Dict[str, List[List[float]]]]:
    texts = [
        " ".join(filter(None, (em.get("subject"), em.get("content")))) for em in emails
    ]
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
                strip_label(em.get("from")),
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
    out = []
    for cid, vecs in incoming.items():
        new_cnt = len(vecs)
        new_sum = [sum(col) for col in zip(*vecs)]
        if cid in existing:
            old_vec, old_cnt = existing[cid]
            tot = old_cnt + new_cnt
            merged = [
                (o * old_cnt + s) / tot for o, s in zip(old_vec, new_sum)
            ]
            out.append((cid, tot, merged))
        else:
            avg = [v / new_cnt for v in new_sum]
            out.append((cid, new_cnt, avg))
    return out

# --- api ---------------------------------------------------------------------
def create_all(
    json_path: Path | str,
    email_db_cfg: dict,
    conv_db_cfg: dict,
    model_name: str = "BAAI/bge-m3",
    batch_size: int = 64,
) -> tuple[int, int]:
    emails = load_flat_emails(Path(json_path))
    if not emails:
        log.warning("no emails found")
        return 0, 0

    model = SentenceTransformer(model_name, trust_remote_code=True, device="cpu")
    dim = model.get_sentence_embedding_dimension()

    email_db = open_db(email_db_cfg)
    conv_db = open_db(conv_db_cfg)
    ensure_schema(email_db, EMAIL_DDL, dim)
    ensure_schema(conv_db,  CONV_DDL,  dim)

    email_rows, conv_vecs = build_rows(model, emails, batch_size)

    with email_db, email_db.cursor() as cur:
        execute_values(cur, EMAIL_UPSERT, email_rows, page_size=500)

    conv_rows = merge_conv_vectors({}, conv_vecs)
    with conv_db, conv_db.cursor() as cur:
        execute_values(cur, CONV_UPSERT, conv_rows, page_size=200)

    log.info("create_all: %d emails, %d conversations", len(email_rows), len(conv_rows))
    return len(email_rows), len(conv_rows)

def update_all(
    json_path: Path | str,
    email_db_cfg: dict,
    conv_db_cfg: dict,
    model_name: str = "BAAI/bge-m3",
    batch_size: int = 64,
) -> tuple[int, int]:
    emails = load_flat_emails(Path(json_path))
    if not emails:
        return 0, 0

    model = SentenceTransformer(model_name, trust_remote_code=True, device="cpu")
    dim = model.get_sentence_embedding_dimension()

    email_db = open_db(email_db_cfg)
    conv_db = open_db(conv_db_cfg)
    ensure_schema(email_db, EMAIL_DDL, dim)
    ensure_schema(conv_db,  CONV_DDL,  dim)

    with email_db.cursor() as cur:
        cur.execute("select id from emails")
        existing_ids = {row[0] for row in cur.fetchall()}

    new_emails = [e for e in emails if strip_label(e.get("id")) not in existing_ids]
    if not new_emails:
        log.info("update_all: no new emails")
        return 0, 0

    email_rows, conv_vecs = build_rows(model, new_emails, batch_size)

    with email_db, email_db.cursor() as cur:
        execute_values(cur, EMAIL_UPSERT, email_rows, page_size=500)

    if conv_vecs:
        cid_list = list(conv_vecs.keys())
        existing_conv = {}
        if cid_list:
            with conv_db.cursor() as cur:
                cur.execute(
                    "select conversation_id, embedding, email_count "
                    "from conversations where conversation_id = any(%s)",
                    (cid_list,),
                )
                existing_conv = {
                    cid: (vec, cnt) for cid, vec, cnt in cur.fetchall()
                }
        conv_rows = merge_conv_vectors(existing_conv, conv_vecs)
        with conv_db, conv_db.cursor() as cur:
            execute_values(cur, CONV_UPSERT, conv_rows, page_size=200)
    else:
        conv_rows = []

    log.info("update_all: %d emails, %d conversations", len(email_rows), len(conv_rows))
    return len(email_rows), len(conv_rows)

def create_or_update(
    json_path: Path | str,
    email_db_cfg: dict,
    conv_db_cfg: dict,
    model_name: str = "BAAI/bge-m3",
    batch_size: int = 64,
) -> tuple[int, int]:
    try:
        conn = open_db(email_db_cfg)
    except Exception as err:
        log.error("cannot connect to email db: %s", err)
        return 0, 0

    if table_exists(conn, "emails"):
        return update_all(json_path, email_db_cfg, conv_db_cfg, model_name, batch_size)
    return create_all(json_path, email_db_cfg, conv_db_cfg, model_name, batch_size)

# --- cli entry ---------------------------------------------------------------
if __name__ == "__main__":
    JSON_PATH = Path("server_client_local_files/preprocessed_emails.json")
    EMAIL_DB_CFG = dict(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        user=os.getenv("PGUSER", "mailmule"),
        password=os.getenv("PGPASSWORD", "159753"),
        dbname=os.getenv("PGDATABASE", "mailmule_db"),
    )
    CONV_DB_CFG = dict(EMAIL_DB_CFG, dbname=os.getenv("PGCONV_DB", "mailmule_conv_db"))

    start = time.time()
    try:
        emails_written, conv_written = create_or_update(
            JSON_PATH,
            EMAIL_DB_CFG,
            CONV_DB_CFG,
            model_name="BAAI/bge-m3",
            batch_size=64,
        )
        log.info(
            "finished: %d emails, %d conversations in %.1f s",
            emails_written,
            conv_written,
            time.time() - start,
        )
    except KeyboardInterrupt:
        log.warning("interrupted by user")
