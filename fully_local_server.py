#!/usr/bin/env python3
"""
Fully local offline pipeline for email embedding, storage, and query retrieval.

Components:
  - Embedding Generation: Uses two SentenceTransformer models (distiluse-base-multilingual-cased-v2 and sentence-transformers-alephbert)
    and concatenates their outputs.
  - FAISS: Builds an index to store high-dimensional concatenated embeddings.
  - PostgreSQL: Stores full email JSON records with metadata, linked via a unique vector_id.
  - Query Processing: Converts a user query to an embedding, performs similarity search in FAISS, and fetches the corresponding emails.
"""

import json
import numpy as np
import faiss
import psycopg2
from psycopg2.extras import Json
from sentence_transformers import SentenceTransformer

# --------------------------
# Configuration Parameters
# --------------------------

# PostgreSQL connection parameters for a containerized instance.
# Update the uppercase fields with your specific credentials.
PG_HOST = "localhost"
PG_PORT = 5432
PG_USER = "POSTGRES_USER"  # <-- update with your PostgreSQL username
PG_PASSWORD = "POSTGRES_PASSWORD"  # <-- update with your PostgreSQL password
PG_DATABASE = "POSTGRES_DATABASE"  # <-- update with your PostgreSQL database name

# Filenames for preprocessed emails and FAISS index
PREPROCESSED_FILE = "preprocessed_emails.json"
FAISS_INDEX_FILE = "faiss_index.bin"

# Embedding dimensions (assumed: 512 for distiluse + 768 for alephbert)
EMBEDDING_DIMENSION = 512 + 768  # 1280


# --------------------------
# Utility and Helper Functions
# --------------------------

def load_emails():
    """Load and flatten emails from the preprocessed conversations JSON."""
    with open(PREPROCESSED_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    emails = []
    for conv in data:
        if 'emails' in conv:
            emails.extend(conv['emails'])
    return emails


def compute_embedding(text, model1, model2):
    """
    Compute embeddings for the given text using two models,
    and return their concatenation as a single numpy vector.
    """
    emb1 = model1.encode(text, convert_to_numpy=True)
    emb2 = model2.encode(text, convert_to_numpy=True)
    return np.concatenate([emb1, emb2])


def build_faiss_index(emails, model1, model2):
    """
    For each email, compute the concatenated embedding from subject and content,
    assign a unique integer id (vector_id), and add it to a FAISS index.
    """
    # Create a flat (L2) index and wrap it with an ID map to store custom ids.
    index = faiss.IndexFlatL2(EMBEDDING_DIMENSION)
    index = faiss.IndexIDMap(index)

    email_vectors = []
    vector_ids = []
    for i, email in enumerate(emails):
        # Use both subject and content as the embedding input text
        text_parts = []
        if 'subject' in email:
            text_parts.append(email['subject'])
        if 'content' in email:
            text_parts.append(email['content'])
        text = " ".join(text_parts)

        # Compute the embedding and store it
        emb = compute_embedding(text, model1, model2)
        email_vectors.append(emb)
        vector_ids.append(i)
        # Store the vector id in the email record for linking with PostgreSQL
        email['vector_id'] = i

    if email_vectors:
        vectors = np.vstack(email_vectors).astype('float32')
        index.add_with_ids(vectors, np.array(vector_ids))
    return index


def save_faiss_index(index, filename):
    """Save the FAISS index to disk."""
    faiss.write_index(index, filename)


def load_faiss_index(filename):
    """Load the FAISS index from disk."""
    index = faiss.read_index(filename)
    return index


def connect_postgres():
    """Connect to PostgreSQL and return the connection object."""
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DATABASE
    )
    return conn


def create_table(conn):
    """
    Create the emails table (if it does not exist) with a schema that includes:
      - vector_id: unique id linking to FAISS,
      - various email metadata fields,
      - raw_json: the full email record.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS emails (
        vector_id BIGINT PRIMARY KEY,
        email_id TEXT,
        subject TEXT,
        sender TEXT,
        date TEXT,
        conversation_id TEXT,
        content TEXT,
        order_val TEXT,
        raw_json JSONB
    );
    """
    cur = conn.cursor()
    cur.execute(create_table_query)
    conn.commit()
    cur.close()


def store_emails_in_postgres(emails, conn):
    """
    Insert each email record along with its metadata into PostgreSQL.
    The 'vector_id' field is used as the primary key for linking with FAISS.
    """
    cur = conn.cursor()
    insert_query = """
    INSERT INTO emails (vector_id, email_id, subject, sender, date, conversation_id, content, order_val, raw_json)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (vector_id) DO UPDATE SET
        email_id = EXCLUDED.email_id,
        subject = EXCLUDED.subject,
        sender = EXCLUDED.sender,
        date = EXCLUDED.date,
        conversation_id = EXCLUDED.conversation_id,
        content = EXCLUDED.content,
        order_val = EXCLUDED.order_val,
        raw_json = EXCLUDED.raw_json;
    """
    for email in emails:
        vector_id = email.get('vector_id')
        email_id = email.get('id', '')
        subject = email.get('subject', '')
        sender = email.get('from', '')
        date = email.get('date', '')
        conversation_id = email.get('conversation_id', '')
        content = email.get('content', '')
        order_val = email.get('order', '')
        raw_json = json.dumps(email)
        cur.execute(insert_query,
                    (vector_id, email_id, subject, sender, date, conversation_id, content, order_val, raw_json))
    conn.commit()
    cur.close()


def query_emails(query, index, model1, model2, conn, top_k=5):
    """
    Given a natural language query, compute its embedding,
    perform a similarity search in the FAISS index,
    then retrieve and return the matching email records from PostgreSQL.
    """
    # Compute the concatenated embedding for the query
    emb = compute_embedding(query, model1, model2).astype('float32')
    emb = np.expand_dims(emb, axis=0)  # shape (1, dimension)
    distances, ids = index.search(emb, top_k)
    # Retrieve the email records from PostgreSQL using the found vector_ids
    vector_ids = ids[0].tolist()
    cur = conn.cursor()
    select_query = "SELECT raw_json FROM emails WHERE vector_id = ANY(%s);"
    cur.execute(select_query, (vector_ids,))
    rows = cur.fetchall()
    results = [row[0] for row in rows]
    cur.close()
    return results, distances[0].tolist()


# --------------------------
# Main Pipeline Execution
# --------------------------

def main():
    # Step 1: Load preprocessed emails.
    print("Loading preprocessed emails...")
    emails = load_emails()
    print(f"Loaded {len(emails)} emails.")

    # Step 2: Load local embedding models.
    print("Loading embedding models...")
    model1 = SentenceTransformer("distiluse-base-multilingual-cased-v2")
    model2 = SentenceTransformer("sentence-transformers-alephbert")
    print("Embedding models loaded.")

    # Step 3: Build FAISS index with concatenated embeddings.
    print("Building FAISS index...")
    index = build_faiss_index(emails, model1, model2)
    print("FAISS index built.")

    # Step 4: Save the FAISS index to disk (optional but recommended).
    print("Saving FAISS index to disk...")
    save_faiss_index(index, FAISS_INDEX_FILE)
    print("FAISS index saved as", FAISS_INDEX_FILE)

    # Step 5: Connect to PostgreSQL, create the table, and store email metadata.
    print("Connecting to PostgreSQL...")
    conn = connect_postgres()
    create_table(conn)
    print("Storing emails in PostgreSQL...")
    store_emails_in_postgres(emails, conn)
    print("Emails stored in PostgreSQL.")

    # Step 6: Query processing loop.
    print("Entering query loop. Type 'exit' to quit.")
    while True:
        user_query = input("Enter your query: ")
        if user_query.lower() == 'exit':
            break
        results, distances = query_emails(user_query, index, model1, model2, conn, top_k=5)
        print("Top matching emails:")
        for res, dist in zip(results, distances):
            print(f"Distance: {dist}")
            print(json.dumps(res, indent=2))
            print("-" * 40)

    conn.close()
    print("Query loop exited. Pipeline complete.")


if __name__ == '__main__':
    main()
