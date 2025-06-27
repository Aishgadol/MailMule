# MailMule

MailMule is a small research project exploring local email search using only open‑source tools. Every component runs on the user's machine so no data ever leaves the local environment. The code purposely sticks to light‑weight models to keep the requirements modest, therefore accuracy is limited but the pipeline is easy to inspect and extend.

## Pipeline
1. **gmail_json_extractor_to_json_best.py** – logs into Gmail via OAuth and stores conversations in `server_client_local_files/emails.json`.
2. **preprocess_emails_for_embeddings.py** – cleans HTML and normalises text into `preprocessed_emails.json`.
3. **storage_and_embedding.py** – embeds the messages with `BAAI/bge-m3` and saves them alongside their metadata into PostgreSQL tables (`emails`, `conversations`).
4. **server.py** – loads all embeddings into a FAISS index. Incoming queries are first rephrased with `ministral/Ministral-3b-instruct`, then embedded and searched.
5. **client.py** – a Tkinter interface that calls `server.py` through the `handle_request` API.
6. **email_json_database_updater.py** – optional helper for incrementally fetching new messages and re‑running preprocessing.

Scripts are intentionally decoupled so each stage can be run on its own or swapped out. Data moves between stages via JSON files or the local PostgreSQL instance. Models are loaded once per run to keep memory use predictable.

## Running the Demo
1. Install the dependencies from `requirements.txt` in a Python 3.10 environment.
2. Start a local PostgreSQL server and create two databases: `mailmule_db` and `mailmule_conv_db`.
3. Run `gmail_json_extractor_to_json_best.py` to export your mailbox.
4. Execute `preprocess_emails_for_embeddings.py` followed by `storage_and_embedding.py --create` to build the initial database.
5. Launch the GUI with `python client.py` and search through your emails.

The models are small so results might not be perfect, but the system demonstrates how to wire together data extraction, preprocessing, embedding generation and similarity search entirely offline.

## Tech Stack
- Python 3.10
- PostgreSQL + pgvector
- SentenceTransformers (`BAAI/bge-m3`)
- Transformers pipeline (`ministral/Ministral-3b-instruct`)
- FAISS for vector search
- Tkinter for the GUI

The code aims to be clear, modular and easy to experiment with. It serves as a starting point for more advanced retrieval work once stronger hardware or models are available.
