# MailMule

MailMule is a local retrieval system for Gmail messages. It extracts your conversations, cleans the text and indexes every email so you can perform semantic searches without sending data to any cloud services. The pipeline uses light‑weight models and runs completely on the host machine.

## Pipeline
1. **gmail_json_extractor_to_json_best.py** – authenticates with Gmail and stores conversations in `server_client_local_files/emails.json`.
2. **preprocess_emails_for_embeddings.py** – removes HTML and normalises text into `preprocessed_emails.json`.
3. **storage_and_embedding.py** – generates embeddings with `BAAI/bge-m3` and inserts them into PostgreSQL tables (`emails`, `conversations`).
4. **server.py** – builds a FAISS index from the stored vectors. Queries are rewritten with `ministral/Ministral-3b-instruct` before embedding and search.
5. **client.py** – Tkinter GUI that calls `server.handle_request` directly.
6. **email_json_database_updater.py** – optional incremental updater that fetches new messages and re‑runs preprocessing.

Each step is independent so you can inspect or modify any stage. Data moves between scripts via JSON files or the local PostgreSQL instance.

## Usage
### Setup
1. Install Python 3.10 and run `pip install -r requirements.txt`.
2. Start PostgreSQL (e.g. `docker-compose up -d db`). Two databases are required: `mailmule_db` and `mailmule_conv_db`.
   The scripts read database credentials from the standard `PG*` environment
   variables. If your PostgreSQL instance uses different values, set `PGHOST`,
   `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE` and `PGCONV_DB` accordingly.
3. Obtain Gmail OAuth credentials and place `credentials.json` in the repository root.

### Building the database
1. Run `python gmail_json_extractor_to_json_best.py` to create `server_client_local_files/emails.json`.
2. Run `python preprocess_emails_for_embeddings.py` to produce `server_client_local_files/preprocessed_emails.json`.
3. Execute `python storage_and_embedding.py --create --json server_client_local_files/preprocessed_emails.json` to populate PostgreSQL.

### Searching
Launch the interface with:
```bash
python client.py
```
The client performs a health check to ensure Docker and PostgreSQL are reachable. When searching, it sends the query to `server.handle_request`, which structures the text with the language model, embeds it and performs a FAISS similarity search. Results are displayed in the GUI.

## Startup Health Check
The GUI calls `handle_request({'type': 'healthCheck'})` on startup. The server verifies Docker accessibility and PostgreSQL connectivity. If either check fails the GUI exits with an error dialog.

## Tech Stack
- Python 3.10
- PostgreSQL with pgvector extension
- SentenceTransformers (`BAAI/bge-m3`)
- Transformers text-generation pipeline (`ministral/Ministral-3b-instruct`)
- FAISS for vector search
- Tkinter for the client interface

The project demonstrates a complete but lightweight retrieval workflow that runs entirely offline.
