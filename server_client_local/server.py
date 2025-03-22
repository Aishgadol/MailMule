# import standard libraries
import os  # for file operations
import json  # for json parsing
import uuid  # for generating unique ids
import email.utils  # for parsing email dates
import numpy as np  # for numerical operations

# import fastapi related modules
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# import google oauth and gmail api libraries
from google_auth_oauthlib.flow import InstalledAppFlow  # for oauth flow
from google.auth.transport.requests import Request as GoogleRequest  # for refreshing tokens
from googleapiclient.discovery import build  # for building gmail service
from google.oauth2.credentials import Credentials  # for handling credentials

# import semantic search libraries
from sentence_transformers import SentenceTransformer  # for generating embeddings

# create fastapi app
app = FastAPI()

# setup cors to allow our local client (for testing)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # in production, restrict this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# set the gmail api scope (read only)
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# global variables for email data and vector index
email_data = []  # list to store emails
embedding_matrix = None  # numpy array for embeddings
faiss_index = None  # faiss index object
embedding_model = SentenceTransformer('all-MiniLM-L6-v2')  # load embedding model

# file to store user credentials (each line is a json record)
credentials_file = "info.jsonl"

# pydantic model for search query
class SearchQuery(BaseModel):
    query: str  # the search query string

# helper function to save credentials in json lines file
def save_user_credentials(user_id: str, creds: Credentials):
    creds_dict = json.loads(creds.to_json())  # convert creds to dict
    record = {"user_id": user_id, "creds": creds_dict}
    with open(credentials_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
    print(  f"saved credentials for user {user_id}")

# helper function to load credentials for a given user id
def load_user_credentials(user_id: str) -> Credentials:
    if not os.path.exists(credentials_file):
        raise HTTPException(status_code=404, detail="credentials file not found")
    with open(credentials_file, "r", encoding="utf-8") as f:
        for line in f:
            try:
                record = json.loads(line)
                if record.get("user_id") == user_id:
                    print(  f"loaded credentials for user {user_id}")
                    return Credentials.from_authorized_user_info(record["creds"], SCOPES)
            except Exception as err:
                print("error parsing record: ", err)
    raise HTTPException(status_code=404, detail="user credentials not found")

# helper function to remove credentials for a user
def remove_user_credentials(user_id: str):
    if not os.path.exists(credentials_file):
        return
    lines = []
    with open(credentials_file, "r", encoding="utf-8") as f:
        lines = f.readlines()
    with open(credentials_file, "w", encoding="utf-8") as f:
        for line in lines:
            try:
                record = json.loads(line)
                if record.get("user_id") != user_id:
                    f.write(line)
            except Exception as err:
                print("error processing line: ", err)
    print(  f"removed credentials for user {user_id}")

# endpoint: /login - performs oauth flow and stores credentials with a unique id
@app.get("/login")
async def login():
    try:
        print("starting oauth flow...")
        flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
        user_creds = flow.run_local_server(port=0)
        print("oauth flow completed")
        user_id = str(uuid.uuid4())
        save_user_credentials(user_id, user_creds)
        print("login successful, user id: ", user_id)
        return {"status": "logged in successfully", "user_id": user_id}
    except Exception as e:
        print("error during login: ", str(e))
        raise HTTPException(status_code=500, detail=str(e))

# endpoint: /fetch_emails - fetches gmail emails and creates a vector index
@app.get("/fetch_emails")
async def fetch_emails(user_id: str = Query(...)):
    try:
        user_creds = load_user_credentials(user_id)
        print("building gmail api service...")
        service = build('gmail', 'v1', credentials=user_creds)
        results = service.users().messages().list(userId='me', maxResults=100).execute()
        messages = results.get('messages', [])
        print(  f"fetched {len(messages)} messages for user {user_id}")
        global email_data, embedding_matrix, faiss_index
        email_data = []
        texts = []
        for msg in messages:
            msg_details = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
            snippet = msg_details.get('snippet', '')
            headers = msg_details['payload'].get('headers', [])
            subject = "n/a"
            sender = "n/a"
            date_str = "n/a"
            time_str = "n/a"
            for header in headers:
                name = header.get("name", "").lower()
                if name == 'subject':
                    subject = header.get("value", "n/a")
                elif name == 'from':
                    sender = header.get("value", "n/a")
                elif name == 'date':
                    try:
                        parsed_date = email.utils.parsedate_to_datetime(header.get("value", ""))
                        date_str = parsed_date.strftime("%Y-%m-%d")
                        time_str = parsed_date.strftime("%H:%M:%S")
                    except Exception:
                        date_str = header.get("value", "n/a")
            email_record = {
                "subject": subject,
                "sender": sender,
                "date": date_str,
                "time": time_str,
                "snippet": snippet
            }
            email_data.append(email_record)
            texts.append(subject + ". " + snippet)
        print("generating embeddings for emails...")
        embedding_matrix = embedding_model.encode(texts, convert_to_numpy=True)
        dim = embedding_matrix.shape[1]
        print( f"embedding dimension: {dim}")
        faiss_index = faiss.IndexFlatL2(dim)
        faiss_index.add(embedding_matrix)
        print( f"faiss index built with {faiss_index.ntotal} vectors")
        return {"status": "emails fetched and indexed", "count": len(email_data)}
    except Exception as e:
        print("error fetching emails: ", str(e))
        raise HTTPException(status_code=500, detail=str(e))

# endpoint: /search - searches for emails based on query using the faiss index
@app.post("/search")
async def search(search_query: SearchQuery, user_id: str = Query(...)):
    try:
        _ = load_user_credentials(user_id)  # verify user exists
        if faiss_index is None:
            raise HTTPException(status_code=400, detail="emails not indexed, fetch emails first")
        print( f"searching for query '{search_query.query}' for user {user_id}")
        query_vec = embedding_model.encode([search_query.query], convert_to_numpy=True)
        k = 10  # number of neighbors
        distances, indices = faiss_index.search(query_vec, k)
        print("search distances: ", distances)
        results = []
        for idx in indices[0]:
            if idx < len(email_data):
                results.append(email_data[idx])
        results.sort(key=lambda x: x['date'])
        print("search results sorted.")
        return {"results": results}
    except Exception as e:
        print("error during search: ", str(e))
        raise HTTPException(status_code=500, detail=str(e))

# endpoint: /disconnect - removes user's credentials from the file
@app.get("/disconnect")
async def disconnect(user_id: str = Query(...)):
    try:
        remove_user_credentials(user_id)
        print(  f"user {user_id} disconnected")
        return {"status": "disconnected", "user_id": user_id}
    except Exception as e:
        print("error during disconnect: ", str(e))
        raise HTTPException(status_code=500, detail=str(e))

# main block to run the server directly from the ide
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)
