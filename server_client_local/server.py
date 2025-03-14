# Import standard libraries
import os  # For file and path operations
import pickle  # For saving and loading OAuth tokens
import email.utils  # For parsing email date strings
import numpy as np  # For numerical operations

# Import FastAPI and related modules for creating our REST API
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Import Google OAuth and Gmail API libraries
from google_auth_oauthlib.flow import InstalledAppFlow  # For OAuth flow
from google.auth.transport.requests import Request as GoogleRequest  # For refreshing tokens
from googleapiclient.discovery import build  # To build Gmail API service

# Import semantic search libraries (SentenceTransformers and FAISS)
from sentence_transformers import SentenceTransformer  # For text embeddings
import faiss  # For vector indexing and similarity search

# Create FastAPI app instance
app = FastAPI()  # Initialize FastAPI app

# Set up CORS to allow our local client to call our API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this to specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Define the Gmail API scope for read-only access
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# Global variables for Gmail credentials and email data
creds = None  # Will hold Google OAuth credentials
emails_data = []  # List to store fetched emails (dictionaries)
email_embeddings = None  # Numpy array for email embeddings
index = None  # FAISS index for vector search
model = SentenceTransformer('all-MiniLM-L6-v2')  # Load embedding model (efficient and small)

# Pydantic model to validate search query request body
class SearchQuery(BaseModel):
    query: str  # The search query string

# GET endpoint to perform Gmail OAuth login
@app.get("/login")
async def login():
    global creds
    try:
        # Check for stored credentials
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
            print("Loaded credentials from token.pickle")
        # If credentials missing or invalid, run the OAuth flow
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                print("Credentials expired, refreshing...")
                creds.refresh(GoogleRequest())
                print("Credentials refreshed")
            else:
                print("Starting OAuth flow...")
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
                print("OAuth flow completed")
            # Save the new credentials for future use
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
            print("Credentials saved to token.pickle")
        print("Login successful")
        return {"status": "Logged in successfully"}
    except Exception as e:
        print("Error during login:", str(e))
        raise HTTPException(status_code=500, detail=str(e))

# GET endpoint to fetch emails from Gmail and index them
@app.get("/fetch_emails")
async def fetch_emails():
    global creds, emails_data, email_embeddings, index
    if not creds:
        raise HTTPException(status_code=401, detail="Unauthorized, please login first")
    try:
        print("Building Gmail API service...")
        service = build('gmail', 'v1', credentials=creds)
        results = service.users().messages().list(userId='me', maxResults=100).execute()
        messages = results.get('messages', [])
        print(f"Fetched {len(messages)} messages")
        emails_data = []  # Reset stored emails
        texts = []  # To collect text for embedding
        for msg in messages:
            msg_data = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
            snippet = msg_data.get('snippet', '')
            headers = msg_data['payload'].get('headers', [])
            subject = "N/A"
            sender = "N/A"
            date_str = "N/A"
            time_str = "N/A"
            for header in headers:
                if header['name'].lower() == 'subject':
                    subject = header['value']
                elif header['name'].lower() == 'from':
                    sender = header['value']
                elif header['name'].lower() == 'date':
                    try:
                        parsed_date = email.utils.parsedate_to_datetime(header['value'])
                        date_str = parsed_date.strftime("%Y-%m-%d")
                        time_str = parsed_date.strftime("%H:%M:%S")
                    except Exception:
                        date_str = header['value']
            email_entry = {"subject": subject, "sender": sender, "date": date_str, "time": time_str, "snippet": snippet}
            emails_data.append(email_entry)
            texts.append(subject + ". " + snippet)
        print("Generating embeddings for emails...")
        email_embeddings = model.encode(texts, convert_to_numpy=True)
        d = email_embeddings.shape[1]  # Dimensionality of embeddings
        print(f"Embedding dimension: {d}")
        index = faiss.IndexFlatL2(d)
        index.add(email_embeddings)  # Add embeddings to the index
        print(f"FAISS index built with {index.ntotal} vectors")
        return {"status": "Emails fetched and indexed", "count": len(emails_data)}
    except Exception as e:
        print("Error fetching emails:", str(e))
        raise HTTPException(status_code=500, detail=str(e))

# POST endpoint to search for emails based on a query
@app.post("/search")
async def search(query: SearchQuery):
    global emails_data, index
    if index is None:
        raise HTTPException(status_code=400, detail="Emails not indexed. Please fetch emails first.")
    try:
        print(f"Searching emails with query: {query.query}")
        query_vec = model.encode([query.query], convert_to_numpy=True)
        k = 10  # Number of nearest neighbors to return
        D, I = index.search(query_vec, k)  # Perform the vector search
        print(f"Search distances: {D}")
        results = []
        for idx in I[0]:
            if idx < len(emails_data):
                results.append(emails_data[idx])
        # Sort results so the newest email is at the bottom (by date)
        results.sort(key=lambda x: x['date'])
        print("Search results sorted")
        return {"results": results}
    except Exception as e:
        print("Error during search:", str(e))
        raise HTTPException(status_code=500, detail=str(e))

# GET endpoint to disconnect and clear credentials/index
@app.get("/disconnect")
async def disconnect():
    global creds, emails_data, email_embeddings, index
    try:
        if os.path.exists('token.pickle'):
            os.remove('token.pickle')
            print("Deleted token.pickle")
        creds = None
        emails_data = []  # Clear stored emails
        email_embeddings = None  # Clear embeddings
        index = None  # Reset vector index
        print("Cleared credentials and indices")
        return {"status": "Disconnected"}
    except Exception as e:
        print("Error during disconnect:", str(e))
        raise HTTPException(status_code=500, detail=str(e))

# Main block to run the server when executing this script directly in an IDE
if __name__ == "__main__":
    import uvicorn
    # Run the server on localhost with reload enabled (useful during development)
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)
