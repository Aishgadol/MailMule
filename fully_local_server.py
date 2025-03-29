#!/usr/bin/env python3
"""
offline email search pipeline.
embedding: concat distiluse and alephbert vectors.
faiss: in-memory index for fast search.
postgres: stores email metadata.
query: compute query embedding, then fetch similar emails.
"""

import json
import numpy as np
import faiss
import psycopg2
from psycopg2.extras import Json
from sentence_transformers import SentenceTransformer

# config - update these uppercase fields with your info
pg_host = "localhost"  # postgres container host (port mapped)
pg_port = 5432  # postgres port (default 5432)
pg_user = "POSTGRES_USER"  # update with your postgres username
pg_password = "POSTGRES_PASSWORD"  # update with your postgres password
pg_database = "POSTGRES_DATABASE"  # update with your postgres db name

preprocessed_file = "preprocessed_emails.json"  # file output from preprocessing
faiss_index_file = "faiss_index.bin"  # file to save/load faiss index
embed_dim = 512 + 768  # total dims for concat embeddings (1280)


def load_emails():
    # load emails from json and flatten conversations into one list
    with open(preprocessed_file, 'r', encoding='utf-8') as f:
        conv_data = json.load(f)
    all_emails = []
    for conv in conv_data:
        if 'emails' in conv:
            all_emails.extend(conv['emails'])
    return all_emails


def compute_embedding(text, model_a, model_b):
    # get embedding from model_a and model_b and concat them into one vector
    emb_a = model_a.encode(text, convert_to_numpy=True)
    emb_b = model_b.encode(text, convert_to_numpy=True)
    return np.concatenate([emb_a, emb_b])


def build_faiss_index(emails, model_a, model_b):
    # create a faiss index using l2 distance, wrapped in an id map so we can assign custom ids
    index = faiss.IndexFlatL2(embed_dim)
    index = faiss.IndexIDMap(index)
    emb_list = []  # list to hold embedding vectors
    id_list = []  # list to hold vector ids corresponding to emails
    # loop over emails, compute embedding from subject and content, assign vector_id
    for i, email in enumerate(emails):
        text_parts = []
        if 'subject' in email:
            text_parts.append(email['subject'])
        if 'content' in email:
            text_parts.append(email['content'])
        combined_text = " ".join(text_parts)  # combine subject and content
        emb = compute_embedding(combined_text, model_a, model_b)
        emb_list.append(emb)
        id_list.append(i)
        email['vector_id'] = i  # add vector id to email record for later lookup in postgres
    if emb_list:
        emb_array = np.vstack(emb_list).astype('float32')  # stack into a numpy array
        index.add_with_ids(emb_array, np.array(id_list))
    return index


def save_index(index, file_path):
    # save the faiss index to disk so we can reload it later without rebuilding
    faiss.write_index(index, file_path)


def connect_pg():
    # establish a connection to the postgres database
    return psycopg2.connect(
        host=pg_host,
        port=pg_port,
        user=pg_user,
        password=pg_password,
        dbname=pg_database
    )


def create_pg_table(conn):
    # create a table to store email metadata and the full json record, if it doesn't exist
    create_sql = """
    create table if not exists emails (
        vector_id bigint primary key,
        email_id text,
        subject text,
        sender text,
        date text,
        conversation_id text,
        content text,
        order_val text,
        raw_json jsonb
    );
    """
    cur = conn.cursor()
    cur.execute(create_sql)
    conn.commit()
    cur.close()


def store_emails_pg(emails, conn):
    # store each email record in postgres using vector_id as the unique key
    cur = conn.cursor()
    insert_sql = """
    insert into emails (vector_id, email_id, subject, sender, date, conversation_id, content, order_val, raw_json)
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    on conflict (vector_id) do update set
        email_id = excluded.email_id,
        subject = excluded.subject,
        sender = excluded.sender,
        date = excluded.date,
        conversation_id = excluded.conversation_id,
        content = excluded.content,
        order_val = excluded.order_val,
        raw_json = excluded.raw_json;
    """
    # loop over each email and insert its data
    for email in emails:
        vec_id = email.get('vector_id')
        email_id_val = email.get('id', '')
        subject_val = email.get('subject', '')
        sender_val = email.get('from', '')
        date_val = email.get('date', '')
        conv_id_val = email.get('conversation_id', '')
        content_val = email.get('content', '')
        order_val = email.get('order', '')
        raw_json = json.dumps(email)
        cur.execute(insert_sql, (
        vec_id, email_id_val, subject_val, sender_val, date_val, conv_id_val, content_val, order_val, raw_json))
    conn.commit()
    cur.close()


def query_emails(query_str, index, model_a, model_b, conn, top_k=5):
    # compute the embedding for the query string and search the faiss index
    q_emb = compute_embedding(query_str, model_a, model_b).astype('float32')
    q_emb = np.expand_dims(q_emb, axis=0)  # reshape to (1, embed_dim)
    dists, ids = index.search(q_emb, top_k)  # get distances and vector ids of top matches
    id_list = ids[0].tolist()
    # fetch the corresponding email records from postgres using the vector ids
    cur = conn.cursor()
    cur.execute("select raw_json from emails where vector_id = any(%s);", (id_list,))
    rows = cur.fetchall()
    cur.close()
    return [row[0] for row in rows], dists[0].tolist()


def main():
    # main pipeline: load emails, build index, store in postgres, then query loop
    print("loading emails...")
    emails = load_emails()
    print("loaded", len(emails), "emails")

    print("loading embedding models...")
    # load both models; they must be cached locally to avoid external calls
    model_a = SentenceTransformer("distiluse-base-multilingual-cased-v2")
    model_b = SentenceTransformer("sentence-transformers-alephbert")
    print("models loaded")

    print("building faiss index...")
    faiss_index = build_faiss_index(emails, model_a, model_b)
    print("index built with", faiss_index.ntotal, "vectors")

    print("saving index to disk...")
    save_index(faiss_index, faiss_index_file)
    print("index saved as", faiss_index_file)

    print("connecting to postgres...")
    pg_conn = connect_pg()
    create_pg_table(pg_conn)  # ensure table exists before inserting records
    print("storing emails in postgres...")
    store_emails_pg(emails, pg_conn)
    print("emails stored in postgres")

    # start a simple query loop to test the pipeline
    print("entering query loop (type 'exit' to quit):")
    while True:
        user_input = input("query> ")
        if user_input.strip().lower() == "exit":
            break
        # get matching emails and distances
        results, distances = query_emails(user_input, faiss_index, model_a, model_b, pg_conn, top_k=5)
        print("top matches:")
        for res, dist in zip(results, distances):
            print("distance:", dist)
            print(json.dumps(res, indent=2))
            print("-" * 40)

    pg_conn.close()  # close postgres connection
    print("done.")


if __name__ == '__main__':
    main()
