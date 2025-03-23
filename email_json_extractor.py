#!/usr/bin/env python3
"""
This script samples up to 2000 random emails from your Gmail account using the Gmail API.
It extracts only essential information needed for embedding:
  - Minimal metadata: id, subject, from, date
  - Combined content: plain text body and allowed attachments (txt, csv, json, pdf)
    (Attachments larger than 10 MB are skipped.)
The resulting minimal email objects are saved incrementally in a file named "emails.json".

Each time the script is run, you will be prompted to log in.
Emails already present in emails.json are skipped.

Before running:
  1. Enable the Gmail API and create OAuth credentials at https://console.cloud.google.com/
  2. Download your credentials as 'credentials.json' into the same folder as this script.
  3. Install required libraries:
       pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
"""

import os
import json
import random
import base64
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def get_gmail_service():
    """
    Prompts the user to log in via OAuth2 every time the script runs and returns an authorized Gmail API service instance.
    """
    flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
    creds = flow.run_local_server(port=0)
    return build('gmail', 'v1', credentials=creds)

def list_all_message_ids(service):
    """Retrieve all message IDs from the user's Gmail account."""
    message_ids = []
    user_id = 'me'
    try:
        response = service.users().messages().list(userId=user_id).execute()
    except Exception as e:
        print("Error listing messages:", e)
        return message_ids

    if 'messages' in response:
        message_ids.extend(response['messages'])

    while 'nextPageToken' in response:
        try:
            page_token = response['nextPageToken']
            response = service.users().messages().list(userId=user_id, pageToken=page_token).execute()
            if 'messages' in response:
                message_ids.extend(response['messages'])
        except Exception as e:
            print("Error retrieving next page of messages:", e)
            break
    return message_ids

def extract_attachment_texts(service, message_id, payload):
    """
    Recursively process the payload to find allowed attachments.
    For attachments with allowed extensions (txt, csv, json, pdf),
    if the attachment's size is less than or equal to 10 MB, fetch and decode
    the attachment and return its text content.
    """
    texts = []
    allowed_extensions = ['.txt', '.csv', '.json', '.pdf']
    max_size = 10 * 1024 * 1024  # 10 MB in bytes

    if 'parts' in payload:
        for part in payload['parts']:
            if 'parts' in part:
                texts.extend(extract_attachment_texts(service, message_id, part))
            else:
                filename = part.get('filename', '')
                if filename and any(filename.lower().endswith(ext) for ext in allowed_extensions):
                    body = part.get('body', {})
                    # Check if the attachment size is reported and skip if it exceeds 10 MB.
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
                                decoded_data = base64.urlsafe_b64decode(data.encode('UTF-8'))
                                text = decoded_data.decode('utf-8', errors='replace')
                                texts.append(text)
                        except Exception as e:
                            print(f"Error decoding attachment {filename} in message {message_id}: {e}")
    return texts

def extract_plain_text(payload):
    """
    Recursively extract plain text content from the payload.
    If a part has mimeType 'text/plain', decode and return its content.
    """
    text_parts = []
    if payload.get('mimeType') == 'text/plain':
        body = payload.get('body', {})
        data = body.get('data')
        if data:
            try:
                decoded_text = base64.urlsafe_b64decode(data.encode('UTF-8')).decode('utf-8', errors='replace')
                text_parts.append(decoded_text)
            except Exception as e:
                print("Error decoding plain text:", e)
    if 'parts' in payload:
        for part in payload['parts']:
            extracted = extract_plain_text(part)
            if extracted:
                text_parts.append(extracted)
    return "\n".join(text_parts)

def extract_essential_info(email, service):
    """
    Extract and return a minimal dictionary with essential information from an email.
    This includes: id, subject, from, date, and content (plain text body + allowed attachments).
    """
    essential = {}
    msg_id = email.get('id', '')
    essential['id'] = msg_id

    # Extract headers into a dict for easy lookup.
    headers = {}
    for header in email.get('payload', {}).get('headers', []):
        name = header.get('name', '').lower()
        value = header.get('value', '')
        headers[name] = value

    essential['subject'] = headers.get('subject', '')
    essential['from'] = headers.get('from', '')
    essential['date'] = headers.get('date', '')

    # Extract plain text content from the email body.
    payload = email.get('payload', {})
    plain_text = extract_plain_text(payload)

    # Extract allowed attachment texts.
    attachments_text = extract_attachment_texts(service, msg_id, payload)

    # Combine plain text, attachments text, and fallback snippet if necessary.
    content_parts = []
    if plain_text:
        content_parts.append(plain_text)
    if attachments_text:
        content_parts.append("\n\n".join(attachments_text))
    if not content_parts:
        content_parts.append(email.get('snippet', ''))

    essential['content'] = "\n\n".join(content_parts).strip()
    return essential

def save_progress(emails_list, output_file):
    """Saves the current emails list to the specified JSON file."""
    try:
        with open(output_file, 'w') as f:
            json.dump(emails_list, f, indent=4)
        print(f"Saved progress: {len(emails_list)} emails.")
    except Exception as e:
        print(f"Error saving emails to file: {e}")

def fetch_and_save_emails_single_file(service, message_ids, output_file="emails.json", sample_size=2000):
    """
    Randomly sample up to sample_size emails, extract minimal essential information,
    and update a single JSON file incrementally.
    Each email is processed individually; errors are logged and skipped.
    Saves progress every 100 processed emails (and at the end).
    """
    total = len(message_ids)
    print(f"Found {total} emails in your account.")
    num_to_sample = min(sample_size, total)
    print(f"Sampling {num_to_sample} emails...")

    # Load previously saved emails if available.
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r') as f:
                emails_list = json.load(f)
        except Exception as e:
            print(f"Error reading existing file {output_file}: {e}")
            emails_list = []
    else:
        emails_list = []

    # Create a set of already processed email IDs.
    processed_ids = {email.get('id') for email in emails_list if email.get('id')}

    # Filter out already processed emails.
    unprocessed_messages = [msg for msg in message_ids if msg['id'] not in processed_ids]
    if len(unprocessed_messages) < num_to_sample:
        num_to_sample = len(unprocessed_messages)
        print(f"Adjusting sample size to {num_to_sample} due to already processed emails.")

    sampled_messages = random.sample(unprocessed_messages, num_to_sample)

    for idx, msg in enumerate(sampled_messages, start=1):
        msg_id = msg['id']
        try:
            email = service.users().messages().get(userId='me', id=msg_id, format='full').execute()
        except Exception as e:
            print(f"Skipping email ID {msg_id} due to error fetching email: {e}")
            continue

        print(f"Processing {idx}/{num_to_sample} - email ID {msg_id}")

        try:
            essential_info = extract_essential_info(email, service)
        except Exception as e:
            print(f"Error extracting essential info for email ID {msg_id}: {e}")
            continue

        emails_list.append(essential_info)

        # Save progress every 100 processed emails.
        if len(emails_list) % 100 == 0:
            save_progress(emails_list, output_file)

    # Final save after processing all emails.
    save_progress(emails_list, output_file)

def main():
    service = get_gmail_service()
    all_messages = list_all_message_ids(service)
    fetch_and_save_emails_single_file(service, all_messages)

if __name__ == '__main__':
    main()
