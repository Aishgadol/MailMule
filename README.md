# MailMule

This project provides a local email search system powered by semantic embeddings and similarity search.

## What it does

1. Connects to Gmail and downloads messages as JSON.
2. Cleans and flattens each email into a simple text format (From | Subject | Body).
3. Generates vector embeddings for each email using two sentence-transformer models.
4. Builds a FAISS index of all embeddings for fast similarity lookup.
5. Allows a user to enter a text query and returns the most similar emails based on embedding distance.

## Goals

* Create a fast, local-only email search prototype.
* Experiment with embedding models and indexing methods.
* Serve as a foundation before moving to a client-server or web-based version.
* Build toward a full‑stack client‑server architecture that implements the described pipeline.

## Current Tech Stack

* **Python 3.8+**
* **Gmail API** (via `google-api-python-client`) for email extraction
* **BeautifulSoup** for HTML cleaning
* **sentence-transformers**:

  * `distiluse-base-multilingual-cased-v2`
  * `sentence-transformers-alephbert`
* **FAISS** for vector indexing and similarity search
* **PostgreSQL** for storing metadata and raw email JSON
