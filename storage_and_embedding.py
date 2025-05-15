#!/usr/bin/env python3
# store_and_embed.py
# read preprocessed_emails.json, embed with bge-m3 (onnx), and upsert into postgres+pgvector

import os
import sys
import json
import logging
from pathlib import Path
from email.utils import parsedate_to_datetime

import psycopg2
from psycopg2.extras import Json, execute_values
from sentence_transformers import SentenceTransformer

# configure basic logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ─── config from env vars or defaults ─────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("PGHOST", "localhost"),
    "port":     os.getenv("PGPORT", "5432"),
    "user":     os.getenv("PGUSER", "mailmule"),
    "password": os.getenv("PGPASSWORD", "mailmule"),
    "dbname":   os.getenv("PGDATABASE", "mailmule"),
}
INPUT_PATH = Path(
    sys.argv[1] if len(sys.argv) > 1 else
    "server_client_local_files/preprocessed_emails.json"
)
EMBED_MODEL_NAME = "BAAI/bge-m3"

# ─── create table with pgvector column ────────────────────────────────────────
DDL_SQL = """
create extension if not exists pgvector;
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
UPSERT_SQL = """
insert into emails
(id, conversation_id, subject, sender, date,
 order_in_conv, content, raw, embedding)
values %s
on conflict (id) do nothing;
"""

def open_db_connection():
    # connect to postgres using psycopg2
    return psycopg2.connect(**DB_CONFIG)

def ensure_table_schema(conn, dim):
    # create extension and table if needed
    with conn, conn.cursor() as cur:
        cur.execute(DDL_SQL.format(dim=dim))

def load_preprocessed_json(path):
    # load the json from disk or exit on error
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        logging.error(f"file not found: {path}")
        sys.exit(1)
    except json.JSONDecodeError:
        logging.error(f"invalid json in file: {path}")
        sys.exit(1)
    logging.info(f"loaded {len(data)} conversations from {path}")
    return data

def flatten_conversations(conversations):
    # produce a list of individual email dicts
    flat = []
    for conv in conversations:
        conv_id = conv.get("conversation_id")
        for em in conv.get("emails", []):
            em["conversation_id"] = conv_id
            flat.append(em)
    return flat

def fetch_existing_ids(conn):
    # get set of email ids already in db
    with conn.cursor() as cur:
        cur.execute("select id from emails")
        return {row[0] for row in cur.fetchall()}

def parse_email_date(date_str):
    # convert rfc-2822 date to datetime or return None
    if not date_str:
        return None
    try:
        return parsedate_to_datetime(date_str)
    except Exception:
        return None

def prepare_upsert_rows(model, emails):
    # build list of tuples for bulk upsert
    rows = []
    for em in emails:
        text = " ".join(
            part for part in (em.get("subject"), em.get("content")) if part
        )
        if not text.strip():
            logging.warning(f"skipping email {em.get('id')}: empty subject/content")
            continue
        try:
            vector = model.encode(text, normalize_embeddings=True).tolist()
        except Exception as e:
            logging.error(f"embedding failed for {em.get('id')}: {e}")
            continue
        rows.append((
            em["id"],
            em.get("conversation_id"),
            em.get("subject"),
            em.get("from"),             # map json 'from' → column 'sender'
            parse_email_date(em.get("date")),
            em.get("order"),
            em.get("content"),
            Json(em),
            vector,
        ))
    return rows

def load_embedding_model():
    # load sentence-transformers onnx model or exit on error
    try:
        model = SentenceTransformer(EMBED_MODEL_NAME, trust_remote_code=True)
    except Exception as e:
        logging.error(f"failed to load embedding model: {e}")
        sys.exit(1)
    dim = model.get_sentence_embedding_dimension()
    if not isinstance(dim, int) or dim <= 0:
        logging.error("model returned invalid embedding dimension")
        sys.exit(1)
    return model

def main():
    # 1. load and flatten emails
    conversations = load_preprocessed_json(INPUT_PATH)
    email_list = flatten_conversations(conversations)

    # 2. load embedding model
    model = load_embedding_model()

    # 3. connect to db and ensure table exists
    try:
        conn = open_db_connection()
    except Exception as e:
        logging.error(f"db connection failed: {e}")
        sys.exit(1)
    ensure_table_schema(conn, model.get_sentence_embedding_dimension())

    # 4. find new emails and prepare rows
    existing_ids = fetch_existing_ids(conn)
    new_emails = [e for e in email_list if e["id"] not in existing_ids]
    if not new_emails:
        logging.info("database already up-to-date")
        return
    logging.info(f"processing {len(new_emails)} new emails")

    rows = prepare_upsert_rows(model, new_emails)
    if not rows:
        logging.info("no valid emails to upsert")
        return

    # 5. bulk upsert into postgres
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, rows, page_size=500)
    except Exception as e:
        logging.error(f"db upsert failed: {e}")
        sys.exit(1)

    logging.info("successfully stored new emails and embeddings")

if __name__ == "__main__":
    main()
