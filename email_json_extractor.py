#!/usr/bin/env python3
"""
This script builds a conversation-centric email dataset from your Gmail account.
It uses a two-step sampling approach:
  1. It retrieves emails from your sent mailbox (label "SENT") and processes each one.
  2. For each sent email, it extracts recipient addresses from the "To" header and then searches
     for all emails (both sent and received) involving those addresses.

Additional filtering rules:
  - If a sent email is a self-email (i.e. the recipient equals the sender's email address),
    skip all conversation emails for that recipient.
  - For all conversations (threads), if the thread has more than 1000 emails, skip it.

All emails are preprocessed to decode their text (handling base64, quoted-printable, and HTML cleaning)
and to extract text-based attachments (for allowed types smaller than 10 MB). The final output is saved
incrementally in "emails.json", and will contain at most 2000 emails.

Each run requires you to log in via OAuth (no token caching).

Before running:
  1. Enable the Gmail API and create OAuth credentials at https://console.cloud.google.com/
  2. Download your credentials as 'credentials.json' into the same folder as this script.
  3. Install required libraries:
       pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib beautifulsoup4 python-docx PyPDF2
"""

import os
import json
import random
import base64
import quopri
import io
import re

from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# ----------------------- Helper to Normalize Email Addresses -----------------------
def extract_email_address(address_str):
    """
    Extracts and normalizes an email address from a header string.
    For example, given '<some name> <some_name@gmail.com>' it returns 'some_name@gmail.com'.
    """
    match = re.search(r'[\w\.-]+@[\w\.-]+', address_str)
    if match:
        return match.group(0).lower()
    return address_str.lower()

# ----------------------- Authentication & Query Helpers -----------------------
def get_gmail_service():
    flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
    creds = flow.run_local_server(port=0)
    return build('gmail', 'v1', credentials=creds)

def list_messages_by_label(service, label_id):
    message_ids = []
    user_id = 'me'
    try:
        response = service.users().messages().list(userId=user_id, labelIds=[label_id]).execute()
    except Exception as e:
        print(f"Error listing messages for label {label_id}:", e)
        return message_ids

    if 'messages' in response:
        message_ids.extend(response['messages'])
    while 'nextPageToken' in response:
        try:
            page_token = response['nextPageToken']
            response = service.users().messages().list(userId=user_id, labelIds=[label_id], pageToken=page_token).execute()
            if 'messages' in response:
                message_ids.extend(response['messages'])
        except Exception as e:
            print("Error retrieving next page:", e)
            break
    return message_ids

def search_messages_by_query(service, query):
    message_ids = []
    user_id = 'me'
    try:
        response = service.users().messages().list(userId=user_id, q=query).execute()
    except Exception as e:
        print(f"Error executing search query '{query}':", e)
        return message_ids

    if 'messages' in response:
        message_ids.extend(response['messages'])
    while 'nextPageToken' in response:
        try:
            page_token = response['nextPageToken']
            response = service.users().messages().list(userId=user_id, q=query, pageToken=page_token).execute()
            if 'messages' in response:
                message_ids.extend(response['messages'])
        except Exception as e:
            print("Error retrieving next page for query:", e)
            break
    return message_ids

# ----------------------- Decoding & Extraction Helpers -----------------------
def get_header_value(headers, key):
    for header in headers:
        if header.get('name', '').lower() == key.lower():
            return header.get('value', '')
    return None

def decode_part_data(data, encoding):
    try:
        decoded_bytes = base64.urlsafe_b64decode(data.encode('UTF-8'))
    except Exception as e:
        print("Base64 decoding error:", e)
        return ""
    if encoding and encoding.lower() == 'quoted-printable':
        try:
            decoded_bytes = quopri.decodestring(decoded_bytes)
        except Exception as e:
            print("Quoted-printable decoding error:", e)
    try:
        return decoded_bytes.decode('utf-8', errors='replace')
    except Exception as e:
        return decoded_bytes.decode('latin1', errors='replace')

def extract_plain_text(payload):
    text_parts = []
    mime = payload.get('mimeType', '')
    headers = payload.get('headers', [])
    encoding = get_header_value(headers, "Content-Transfer-Encoding")
    if mime in ['text/plain', 'text/html']:
        data = payload.get('body', {}).get('data')
        if data:
            decoded_text = decode_part_data(data, encoding)
            if mime == 'text/html':
                soup = BeautifulSoup(decoded_text, "html.parser")
                for tag in soup(["script", "style"]):
                    tag.decompose()
                cleaned_text = soup.get_text(separator="\n")
                text_parts.append(cleaned_text)
            else:
                text_parts.append(decoded_text)
    if 'parts' in payload:
        for part in payload['parts']:
            part_text = extract_plain_text(part)
            if part_text:
                text_parts.append(part_text)
    return "\n".join(text_parts)

def extract_docx_text(bytes_data):
    try:
        from docx import Document
    except ImportError:
        print("python-docx not installed; skipping DOCX extraction.")
        return ""
    try:
        f = io.BytesIO(bytes_data)
        document = Document(f)
        return "\n".join(para.text for para in document.paragraphs)
    except Exception as e:
        print("Error extracting DOCX text:", e)
        return ""

def extract_pdf_text(bytes_data):
    try:
        import PyPDF2
    except ImportError:
        print("PyPDF2 not installed; skipping PDF extraction.")
        return ""
    try:
        reader = PyPDF2.PdfReader(io.BytesIO(bytes_data))
        text = []
        for page in reader.pages:
            page_text = page.extract_text() or ""
            text.append(page_text)
        return "\n".join(text)
    except Exception as e:
        print("Error extracting PDF text:", e)
        return ""

def extract_attachment_texts(service, message_id, payload):
    texts = []
    allowed_extensions = ['.txt', '.csv', '.json', '.docx', '.pdf']
    max_size = 10 * 1024 * 1024  # 10 MB

    if 'parts' in payload:
        for part in payload['parts']:
            if 'parts' in part:
                texts.extend(extract_attachment_texts(service, message_id, part))
            else:
                filename = part.get('filename', '')
                if filename and any(filename.lower().endswith(ext) for ext in allowed_extensions):
                    body = part.get('body', {})
                    attachment_size = body.get('size', 0)
                    if attachment_size > max_size:
                        print(f"Skipping attachment {filename} in message {message_id} (size {attachment_size} bytes > 10MB).")
                        continue
                    if 'attachmentId' in body:
                        attachment_id = body['attachmentId']
                        try:
                            attachment = service.users().messages().attachments().get(
                                userId='me', messageId=message_id, id=attachment_id
                            ).execute()
                            data = attachment.get('data')
                            if data:
                                try:
                                    raw_bytes = base64.urlsafe_b64decode(data.encode('UTF-8'))
                                except Exception as e:
                                    print(f"Error decoding attachment {filename} in message {message_id}: {e}")
                                    continue
                                ext = filename.lower().split('.')[-1]
                                if ext in ['txt', 'csv', 'json']:
                                    att_encoding = get_header_value(part.get('headers', []), "Content-Transfer-Encoding")
                                    text = decode_part_data(data, att_encoding)
                                elif ext == 'docx':
                                    text = extract_docx_text(raw_bytes)
                                elif ext == 'pdf':
                                    text = extract_pdf_text(raw_bytes)
                                else:
                                    text = ""
                                if text:
                                    texts.append(text)
                        except Exception as e:
                            print(f"Error processing attachment {filename} in message {message_id}: {e}")
    return texts

def extract_essential_info(email, service):
    essential = {}
    msg_id = email.get('id', '')
    essential['id'] = msg_id

    headers = {}
    for header in email.get('payload', {}).get('headers', []):
        name = header.get('name', '').lower()
        value = header.get('value', '')
        headers[name] = value

    essential['subject'] = headers.get('subject', '')
    essential['from'] = headers.get('from', '')
    essential['date'] = headers.get('date', '')

    payload = email.get('payload', {})
    plain_text = extract_plain_text(payload)
    attachments_text = extract_attachment_texts(service, msg_id, payload)

    content_parts = []
    if plain_text:
        content_parts.append(plain_text)
    if attachments_text:
        content_parts.append("\n\n".join(attachments_text))
    if not content_parts:
        content_parts.append(email.get('snippet', ''))

    essential['content'] = "\n\n".join(content_parts).strip()
    return essential

# ----------------------- Incremental Saving -----------------------
def save_progress(emails_list, output_file):
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(emails_list, f, ensure_ascii=False, indent=4)
        print(f"Saved progress: {len(emails_list)} emails.")
    except Exception as e:
        print(f"Error saving emails to file: {e}")

# ----------------------- New Conversation Sampling -----------------------
def fetch_and_save_conversation_emails(service, output_file="emails.json", max_output_emails=701):
    """
    For each email in the sent box, process and save the email.
    Then, extract recipient addresses from that email and search for all emails
    (both sent and received) involving those addresses.

    Additional filtering:
      - If the recipient is the user's email address, skip all conversation emails for that recipient.
      - For any conversation (thread) longer than 1000 emails, skip it.

    The final output JSON will contain at most max_output_emails emails.
    Incremental saving is done every 100 new emails.
    """
    user_id = 'me'
    emails_list = []

    if os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                emails_list = json.load(f)
        except Exception as e:
            print(f"Error reading existing file {output_file}: {e}")
            emails_list = []
    processed_ids = {email.get('id') for email in emails_list if email.get('id')}

    thread_size_cache = {}

    sent_messages = list_messages_by_label(service, "SENT")
    print(f"Found {len(sent_messages)} sent emails as seeds...")

    for idx, msg in enumerate(sent_messages, start=1):
        if len(emails_list) >= max_output_emails:
            print("Reached maximum output emails limit.")
            break
        sent_id = msg['id']
        try:
            sent_email = service.users().messages().get(userId=user_id, id=sent_id, format='full').execute()
        except Exception as e:
            print(f"Skipping sent email ID {sent_id} due to error: {e}")
            continue
        # Extract user's email address (normalized).
        user_email_raw = get_header_value(sent_email.get('payload', {}).get('headers', []), "From") or ""
        user_email = extract_email_address(user_email_raw)

        if sent_id not in processed_ids:
            try:
                essential = extract_essential_info(sent_email, service)
                emails_list.append(essential)
                processed_ids.add(sent_id)
                print(f"Saved sent email ID {sent_id}.")
            except Exception as e:
                print(f"Error processing sent email ID {sent_id}: {e}")
                continue

        to_field = get_header_value(sent_email.get('payload', {}).get('headers', []), "To")
        if not to_field:
            continue
        recipients = [addr.strip() for addr in re.split(r'[;,]', to_field) if addr.strip()]

        for recipient in recipients:
            if len(emails_list) >= max_output_emails:
                break
            normalized_recipient = extract_email_address(recipient)
            # Skip if the recipient is the user's email address.
            if normalized_recipient == user_email:
                print(f"Skipping conversation for recipient {recipient} as it matches the user's email.")
                continue
            query = f'from:"{recipient}" OR to:"{recipient}"'
            conversation_msgs = search_messages_by_query(service, query)
            print(f"Found {len(conversation_msgs)} messages for recipient {recipient}.")
            for conv_msg in conversation_msgs:
                if len(emails_list) >= max_output_emails:
                    break
                conv_id = conv_msg['id']
                if conv_id in processed_ids:
                    continue

                thread_id = conv_msg.get('threadId')
                if thread_id:
                    if thread_id not in thread_size_cache:
                        try:
                            thread = service.users().threads().get(userId=user_id, id=thread_id, format='minimal').execute()
                            thread_count = len(thread.get('messages', []))
                            thread_size_cache[thread_id] = thread_count
                        except Exception as e:
                            print(f"Error getting thread {thread_id}: {e}")
                            continue
                    else:
                        thread_count = thread_size_cache[thread_id]

                    if thread_count > 1000:
                        print(f"Skipping conversation thread {thread_id} because it has {thread_count} emails (> 1000).")
                        continue

                try:
                    conv_email = service.users().messages().get(userId=user_id, id=conv_id, format='full').execute()
                    essential_conv = extract_essential_info(conv_email, service)
                    emails_list.append(essential_conv)
                    processed_ids.add(conv_id)
                    print(f"Saved conversation email ID {conv_id} for recipient {recipient}.")
                except Exception as e:
                    print(f"Error processing conversation email ID {conv_id}: {e}")
                    continue

        if len(emails_list) % 100 < 5:
            save_progress(emails_list, output_file)

    save_progress(emails_list, output_file)

def main():
    service = get_gmail_service()
    fetch_and_save_conversation_emails(service)

if __name__ == '__main__':
    main()
