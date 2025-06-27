# Architecture

MailMule is a collection of small Python utilities that together provide a local email search engine. The project avoids any cloud services after the initial Gmail download and keeps every component lightweight so it can run on a modest machine.

## Pipeline Overview
1. **gmail_json_extractor_to_json_best.py** – performs Gmail OAuth and writes conversations to `server_client_local_files/emails.json`.
2. **preprocess_emails_for_embeddings.py** – strips HTML and normalises text into `preprocessed_emails.json`.
3. **storage_and_embedding.py** – encodes messages with the `BAAI/bge-m3` SentenceTransformer and stores both emails and conversation vectors in PostgreSQL (using `pgvector`).
4. **server.py** – builds a FAISS index from the database and answers search requests. Queries are first rewritten with `ministral/Ministral-3b-instruct` via Hugging Face `pipeline` before being embedded.
5. **client.py** – a Tkinter GUI that interacts with `server.py` through its `handle_request` function.
6. **email_json_database_updater.py** – incremental fetcher that merges new Gmail messages into the JSON database and re-runs preprocessing.

Below is a simplified diagram of how the components connect:

```
Gmail API
   |
[gmail_json_extractor_to_json_best.py]
   |
emails.json
   |
[preprocess_emails_for_embeddings.py]
   |
preprocessed_emails.json
   |
[storage_and_embedding.py] --(BAAI/bge-m3)--> PostgreSQL + pgvector
   |
        +--------------+
        |  server.py   |
        +--------------+
          ^       |
          |       | rephrase (Ministral-3b) -> embed -> FAISS
        +--------------+
        |  client.py   |
        +--------------+
```

The optional `email_json_database_updater.py` feeds new data back into `emails.json` so preprocessing can be rerun when new messages arrive.

The scripts communicate via simple JSON files and a local PostgreSQL instance. Models are loaded once per run and kept in memory to minimise latency.

## Models
- **BAAI/bge-m3** (SentenceTransformer) for email embeddings.
- **ministral/Ministral-3b-instruct** via `transformers.pipeline` to structure user queries.

FAISS is used for similarity search while PostgreSQL + pgvector store metadata and vectors.
