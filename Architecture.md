# Architecture

MailMule is organised as a set of standalone scripts that together implement a local email search engine. After the initial Gmail export every step operates offline using local files and PostgreSQL.

## Components
- **gmail_json_extractor_to_json_best.py** – authenticates with Gmail and writes conversations to `server_client_local_files/emails.json`.
  - Uses the Gmail API and `BeautifulSoup` to extract plain text from messages and attachments.
- **preprocess_emails_for_embeddings.py** – cleans HTML, normalises whitespace and writes `preprocessed_emails.json`.
- **storage_and_embedding.py** – embeds messages and stores them in Postgres.
  - Functions `create_all`, `update_all` and `create_or_update` manage ingestion.
  - Email embeddings are generated with `SentenceTransformer` (`BAAI/bge-m3`).
  - Two tables are created:
    - `emails` (id, conversation_id, subject, sender, date, order_in_conv, content, raw, embedding)
    - `conversations` (conversation_id, email_count, embedding)
- **server.py** – interface used by the GUI.
  - Keeps a FAISS `IndexFlatIP` in memory.
  - Provides `handle_request()` which supports `sendEmailsToUI`, `inputFromUI` and `healthCheck` requests.
  - On startup or when new data arrives it rebuilds the FAISS index from the embeddings stored in Postgres.
  - Queries are rephrased with the `transformers` text-generation pipeline (`ministral/Ministral-3b-instruct`) before embedding.
- **client.py** – Tkinter GUI that calls `handle_request` directly and displays results.
- **email_json_database_updater.py** – optional updater that fetches only new Gmail messages and re-runs preprocessing.

## Pipeline Diagram
```
            +-------------+
            |  Gmail API  |
            +-------------+
                    |
                    v
  +---------------------------------------+
  | gmail_json_extractor_to_json_best.py  |
  +---------------------------------------+
                    |
                    v
  server_client_local_files/emails.json
                    |
                    v
 +-----------------------------------------+
 | preprocess_emails_for_embeddings.py     |
 +-----------------------------------------+
                    |
                    v
  server_client_local_files/preprocessed_emails.json
                    |
                    v
    +---------------------------+
    | storage_and_embedding.py  |
    +---------------------------+
                    |
                    v
     PostgreSQL (pgvector)
                    |
                    v
    +-----------+      +---------+
    | server.py |<---->| client.py |
    +-----------+      +---------+
```

## Data Flow
1. `gmail_json_extractor_to_json_best.py` pulls messages from Gmail and produces `emails.json`.
2. `preprocess_emails_for_embeddings.py` converts this to `preprocessed_emails.json`.
3. `storage_and_embedding.py` embeds the cleaned emails and populates the `emails` and `conversations` tables.
4. `server.py` loads all embeddings into FAISS and exposes `handle_request`.
5. `client.py` sends search queries to the server and renders the results.
6. `email_json_database_updater.py` can append new messages to `emails.json` so steps 2–4 can be repeated.

The scripts communicate through JSON files and the local PostgreSQL instance. Models are loaded once per run to minimise memory usage and startup cost.

## Search Process
When the user submits a query from the GUI:
1. `client.py` calls `handle_request({'type': 'inputFromUI', 'query': q, 'k': 8})`.
2. The server rewrites the query with the LLM, embeds it using the same SentenceTransformer model and performs a FAISS similarity search.
3. Metadata for the top results is fetched from Postgres and returned to the client for display.

```
client.py
    |
    | query
    v
server.py
    |-- check Docker & Postgres
    |-- ingest new JSON via storage_and_embedding.py
    |-- build/update FAISS index
    |-- rewrite query with Ministral-3b
    |-- embed with BGE-m3
    |-- search FAISS
    |-- fetch metadata from PostgreSQL
    v
client.py (results)
```

This design keeps all data on the user's machine while providing semantic search over the local email archive.
