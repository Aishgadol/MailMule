#!/usr/bin/env python3
# store_and_embed.py
# read a preprocessed_emails.json file, embed each email with bge-m3 (onnx),
# and upsert both raw json + embedding into a postgres table with pgvector

import os
import sys
import json
from pathlib import Path
from email.utils import parsedate_to_datetime

import psycopg2
from psycopg2.extras import Json, execute_values
from sentence_transformers import SentenceTransformer

# ─── configuration pulled from env vars with sensible defaults ────────────────
PG_CFG = {
    "host":     os.getenv("PGHOST", "localhost"),
    "port":     os.getenv("PGPORT", "5432"),
    "user":     os.getenv("PGUSER", "mailmule"),
    "password": os.getenv("PGPASSWORD", "mailmule"),
    "dbname":   os.getenv("PGDATABASE", "mailmule"),
}

JSON_FILE = Path(
    sys.argv[1] if len(sys.argv) > 1
    else "server_client_local_files/preprocessed_emails.json"
)

MODEL_NAME = "BAAI/bge-m3"          # sentence-transformers pulls onnx version

# ─── ddl for the single table we need ─────────────────────────────────────────
DDL_TEMPLATE = """
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

# upsert with do-nothing on conflict keeps existing rows unchanged
UPSERT_SQL = """
insert into emails
(id, conversation_id, subject, sender, date,
 order_in_conv, content, raw, embedding)
values %s
on conflict (id) do nothing;
"""

# ─── helper: open a db connection ─────────────────────────────────────────────
def open_connection():
    # psycopg2 connects using the cfg dict
    return psycopg2.connect(**PG_CFG)

# ─── helper: create table if first run ────────────────────────────────────────
def ensure_schema(conn, dim):
    # executes ddl with correct embedding dimension
    ddl = DDL_TEMPLATE.format(dim=dim)
    with conn, conn.cursor() as cur:
        cur.execute(ddl)

# ─── helper: flatten conversations to individual emails ──────────────────────
def flatten_conversations(conversations):
    # each object in json has conversation_id and list "emails"
    flat = []
    for conv in conversations:
        conv_id = conv.get("conversation_id")
        for email in conv.get("emails", []):
            email["conversation_id"] = conv_id
            flat.append(email)
    return flat

# ─── helper: read local json file ─────────────────────────────────────────────
def load_json(path):
    data = json.loads(path.read_text(encoding="utf-8"))
    print(f"loaded {len(data)} conversations from {path}")
    return data

# ─── helper: get set of ids already stored ───────────────────────────────────
def fetch_existing_ids(conn):
    with conn.cursor() as cur:
        cur.execute("select id from emails")
        return {row[0] for row in cur.fetchall()}

# ─── helper: safe date parse ─────────────────────────────────────────────────
def parse_date(date_str):
    # returns datetime or None if parse fails
    try:
        return parsedate_to_datetime(date_str) if date_str else None
    except Exception:
        return None

# ─── build list of rows for bulk insert ──────────────────────────────────────
def prepare_rows(model, emails):
    rows = []
    for email in emails:
        # embed subject + body for semantic meaning
        text = " ".join(
            piece for piece in (email.get("subject"), email.get("content")) if piece
        )
        vector = model.encode(text, normalize_embeddings=True).tolist()
        rows.append(
            (
                email["id"],
                email.get("conversation_id"),
                email.get("subject"),
                email.get("from"),                 # sender address
                parse_date(email.get("date")),
                email.get("order"),
                email.get("content"),
                Json(email),                       # raw jsonb payload
                vector,
            )
        )
    return rows

# ─── main pipeline ───────────────────────────────────────────────────────────
def main():
    # load preprocessed json
    conversations = load_json(JSON_FILE)
    all_emails = flatten_conversations(conversations)

    # load bge-m3 onnx model once
    model = SentenceTransformer(MODEL_NAME, trust_remote_code=True)

    # connect to postgres
    conn = open_connection()
    ensure_schema(conn, model.get_sentence_embedding_dimension())

    # filter out ids we already stored
    existing_ids = fetch_existing_ids(conn)
    new_emails = [e for e in all_emails if e["id"] not in existing_ids]
    if not new_emails:
        print("database already up-to-date")
        return

    print(f"embedding and storing {len(new_emails)} new emails")
    batch = prepare_rows(model, new_emails)

    # bulk insert using execute_values for speed
    with conn, conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, batch, page_size=500)

    print("done – new emails saved with embeddings")

if __name__ == "__main__":
    main()
