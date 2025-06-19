# Target MVP Architecture

This project will be streamlined for local‑only operation on a modest machine (2 CPU cores, ~4GB RAM). The goal is a minimal but complete email search workflow with no external cloud dependencies after the initial Gmail export.

## Proposed Diagram
```mermaid
flowchart TD
    subgraph Pipeline
        A["pipeline.py\n(Gmail fetch + preprocess)"] --> B["emails.db (SQLite)"]
        B --> C["embed_and_index.py"]
        C --> D["FAISS index + metadata"]
    end

    subgraph API
        E["app.py (FastAPI)"] --> F[(FAISS index)]
        F <-- E
    end

    G[Browser UI] -->|/search| E
```

## Component Responsibilities

**pipeline.py** – Single entrypoint for OAuth, Gmail download, and text cleaning. It writes structured email records directly into a lightweight SQLite database (`emails.db`).

**embed_and_index.py** – Reads new rows from SQLite, generates embeddings with a small SentenceTransformer model (e.g. `all-MiniLM-L6-v2`) and updates a local FAISS index file on disk. The embeddings are also stored back in SQLite for reference.

**app.py** – Minimal FastAPI service exposing one `POST /search` endpoint. On startup it loads the FAISS index and associated metadata from SQLite. Queries are encoded and looked up in FAISS; matching subjects/senders/snippets are returned as JSON.

**Browser UI** – A static HTML page (similar to the existing `index.html`) that submits search queries and renders results.

## Data Flow & Interfaces
1. `pipeline.py` performs OAuth once, downloads all Gmail messages and stores them in SQLite table `emails(id, subject, sender, date, content)`.
2. `embed_and_index.py` reads any emails without an embedding, computes vectors, appends them to a FAISS index file (`emails.faiss`) and updates the `embedding` column in SQLite.
3. `app.py` loads `emails.faiss` and metadata at startup. A search request POSTs `{query: "..."}` and returns JSON `[ {subject, sender, date, snippet}, ... ]`.

### API Contract
```json
POST /search
{ "query": "example" }
-->
{ "results": [ {"subject": "...", "sender": "...", "date": "...", "snippet": "..." } ] }
```

## Technology Choices & Rationale
- **Python stdlib + `sqlite3`** for storage; avoids running a PostgreSQL server.
- **SentenceTransformers** with a small CPU‑friendly model (`all-MiniLM-L6-v2`).
- **FAISS** for vector search because it is efficient and works entirely offline.
- **FastAPI** kept for convenience but can be replaced with Flask if dependencies need to be trimmed further.
- Everything runs locally after Gmail export; all models are downloaded once and reused.

## Local‑Only Constraints
- No runtime calls to external services after the Gmail fetch step.
- Embedding models and FAISS index files reside under `data/` inside the project directory.
- SQLite database (`emails.db`) is also stored locally; no network database connections.

## Proposed Directory Layout
```
.
├── data/
│   ├── emails.db          # SQLite database with raw and embedded emails
│   ├── emails.faiss       # FAISS index built by embed_and_index.py
│   └── models/            # downloaded SentenceTransformer model
├── pipeline.py            # fetch + preprocess script
├── embed_and_index.py     # embedding generation and FAISS upkeep
├── app.py                 # FastAPI search API
└── static/
    └── index.html         # browser UI
```
