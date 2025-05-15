#!/usr/bin/env python3
"""
store_and_embed.py

▪ Input : path to preprocessed_emails.json (MailMule format)
▪ Task  : upsert every email → Postgres (raw JSON) and pgvector embedding
▪ Notes : first run seeds the DB; later runs only process new IDs
"""

import json, os, sys, datetime as dt
from pathlib import Path

import psycopg2
from psycopg2.extras import Json, execute_values
from sentence_transformers import SentenceTransformer


# ─── configuration ─────────────────────────────────────────────────────────────
PG_CONN = {
    "host"    : os.getenv("PGHOST", "localhost"),
    "port"    : os.getenv("PGPORT", "5432"),
    "user"    : os.getenv("PGUSER", "mailmule"),
    "password": os.getenv("PGPASSWORD", "mailmule"),
    "dbname"  : os.getenv("PGDATABASE", "mailmule"),
}
JSON_PATH = Path(sys.argv[1] if len(sys.argv) > 1 else
                 "server_client_local_files/preprocessed_emails.json")
MODEL_NAME = "BAAI/bge-m3"

# ─── database helpers ──────────────────────────────────────────────────────────
DDL = """
CREATE EXTENSION IF NOT EXISTS pgvector;
CREATE TABLE IF NOT EXISTS emails (
    id             TEXT PRIMARY KEY,
    conversation_id TEXT,
    subject        TEXT,
    sender         TEXT,
    date           TIMESTAMPTZ,
    order_in_conv  INT,
    content        TEXT,
    raw            JSONB,
    embedding      VECTOR(%d)
);
"""

UPSERT_SQL = """
INSERT INTO emails
(id, conversation_id, subject, sender, date,
 order_in_conv, content, raw, embedding)
VALUES %s
ON CONFLICT (id) DO NOTHING;
"""


def open_db():
    return psycopg2.connect(**PG_CONN)


def ensure_schema(conn, dim: int):
    with conn, conn.cursor() as cur:
        cur.execute(DDL % dim)


# ─── ingestion logic ───────────────────────────────────────────────────────────
def load_json() -> list[dict]:
    try:
        data = json.loads(JSON_PATH.read_text(encoding="utf-8"))
        print(f"[INFO] loaded {JSON_PATH} ({len(data)} conversations)")
        return data
    except Exception as e:
        sys.exit(f"[FATAL] cannot read {JSON_PATH}: {e}")


def flatten(conversations: list[dict]) -> list[dict]:
    """Return a flat list with one dict per *email* (preserves raw)."""
    flat = []
    for conv in conversations:
        cid = conv.get("conversation_id")
        for em in conv.get("emails", []):
            em["conversation_id"] = cid
            flat.append(em)
    return flat


def fetch_existing_ids(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM emails")
        return {r[0] for r in cur.fetchall()}


def build_embedding(model, email: dict) -> list[float]:
    text = " ".join(
        x for x in (email.get("subject", ""), email.get("content", "")) if x
    )
    return model.encode(text, normalize_embeddings=True).tolist()


def rows_for_upsert(model, emails: list[dict]) -> list[tuple]:
    rows = []
    for em in emails:
        emb = build_embedding(model, em)
        rows.append(
            (
                em["id"],
                em.get("conversation_id"),
                em.get("subject"),
                em.get("from"),
                # convert RFC-2822 date to timestamptz when possible
                parse_ts(em.get("date")),
                em.get("order"),
                em.get("content"),
                Json(em),  # raw jsonb
                emb,
            )
        )
    return rows


def parse_ts(date_str: str | None):
    from email.utils import parsedate_to_datetime

    if not date_str:
        return None
    try:
        return parsedate_to_datetime(date_str)
    except Exception:
        return None


# ─── main ──────────────────────────────────────────────────────────────────────
def main():
    conversations = load_json()
    flat_emails   = flatten(conversations)

    model = SentenceTransformer(MODEL_NAME)
    conn  = open_db()
    ensure_schema(conn, model.get_sentence_embedding_dimension())

    existing = fetch_existing_ids(conn)
    fresh    = [e for e in flat_emails if e["id"] not in existing]

    if not fresh:
        print("[INFO] database already up-to-date.")
        return

    print(f"[INFO] embedding & inserting {len(fresh)} new emails …")
    rows = rows_for_upsert(model, fresh)

    with conn, conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, page_size=500)

    print("[DONE] all new emails stored and indexed.")


if __name__ == "__main__":
    main()
