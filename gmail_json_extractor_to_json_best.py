"""
a simple script to build a conversation-centric email dataset from gmail.
uses oauth every time and processes emails, attachments, and html.
"""

import os
import re
import json
import base64
import email
import datetime
import random

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from bs4 import BeautifulSoup  # for cleaning html content

from docx import Document  # to process docx attachments
import PyPDF2  # to process pdf attachments

# scopes and constants
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
ALLOWED_EXTENSIONS = ['.txt', '.csv', '.json', '.docx', '.pdf']
MAX_ATTACHMENT_SIZE = 10 * 1024 * 1024  # 10 mb
OUTPUT_FILE = 'server_client_local_files/emails.json'
INCREMENTAL_SAVE_COUNT = 100
MAX_EMAILS = 2000

def get_credentials():
    # run oauth flow every time; no caching tokens
    # this will open a local server to complete the oauth process
    flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
    creds = flow.run_local_server(port=0)
    return creds

def build_service():
    # build the gmail api service using the oauth credentials
    creds = get_credentials()
    service = build('gmail', 'v1', credentials=creds)
    print("gmail service built successfully")
    return service

def normalize_email(addr):
    # normalize an email address using a regex and return lower-case version
    try:
        normalized = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', addr).group(0).lower()
    except Exception:
        normalized = addr.strip().lower()
    return normalized

def remove_quoted_text(text):
    # remove quoted text lines (starting with '>' or reply markers) from email body
    lines = text.splitlines()
    new_lines = []
    for line in lines:
        # if line matches reply marker pattern, stop reading further
        if re.match(r'^on .+ wrote:$', line, re.IGNORECASE):
            break
        # skip lines that are quoted (starting with '>')
        if line.strip().startswith('>'):
            continue
        new_lines.append(line)
    return "\n".join(new_lines).strip()

def clean_html(html_content):
    # remove script and style tags and return plain text from html
    soup = BeautifulSoup(html_content, 'html.parser')
    for tag in soup(["script", "style"]):
        tag.decompose()  # remove unwanted tags
    text = soup.get_text(separator='\n')
    return text.strip()

def extract_text_from_message(message):
    # recursively extract plain text from email message parts
    content = ""
    try:
        payload = message.get('payload', {})
        if 'parts' in payload:
            # iterate over all parts in the payload
            for part in payload['parts']:
                mime_type = part.get('mimeType', '')
                if mime_type == 'text/plain':
                    data = part.get('body', {}).get('data')
                    if data:
                        # decode base64 encoded plain text
                        text = base64.urlsafe_b64decode(data.encode('ASCII')).decode('utf-8', errors='ignore')
                        content += text + "\n"
                elif mime_type == 'text/html':
                    data = part.get('body', {}).get('data')
                    if data:
                        # decode and clean html content
                        html_content = base64.urlsafe_b64decode(data.encode('ASCII')).decode('utf-8', errors='ignore')
                        content += clean_html(html_content) + "\n"
                elif mime_type.startswith('multipart/'):
                    # handle nested multiparts recursively
                    sub_msg = {'payload': part}
                    content += extract_text_from_message(sub_msg)
        else:
            # if no parts, process the single payload
            mime_type = payload.get('mimeType', '')
            data = payload.get('body', {}).get('data')
            if data:
                if mime_type == 'text/plain':
                    content += base64.urlsafe_b64decode(data.encode('ASCII')).decode('utf-8', errors='ignore')
                elif mime_type == 'text/html':
                    html_content = base64.urlsafe_b64decode(data.encode('ASCII')).decode('utf-8', errors='ignore')
                    content += clean_html(html_content)
    except Exception as e:
        print(f"error extracting text: {e}")
    return content

def process_attachment(service, message_id, part):
    # process allowed attachments if they are within size limit and supported type
    attachment_text = ""
    try:
        attachment_id = part.get('body', {}).get('attachmentId')
        if not attachment_id:
            return ""
        # retrieve attachment from gmail api
        att_data = service.users().messages().attachments().get(
            userId='me', messageId=message_id, id=attachment_id
        ).execute()
        data = att_data.get('data')
        if not data:
            return ""
        file_data = base64.urlsafe_b64decode(data.encode('ASCII'))
        if len(file_data) > MAX_ATTACHMENT_SIZE:
            print(f"attachment too large in msg {message_id}")
            return ""
        filename = part.get('filename', '')
        ext = os.path.splitext(filename)[1].lower()
        if ext not in ALLOWED_EXTENSIONS:
            print(f"skipping unsupported attachment: {filename}")
            return ""
        # process plain text attachments directly
        if ext in ['.txt', '.csv', '.json']:
            attachment_text = file_data.decode('utf-8', errors='ignore')
        elif ext == '.docx':
            # write temporary file to process docx
            temp_file = f"temp_{message_id}_{attachment_id}.docx"
            with open(temp_file, "wb") as f:
                f.write(file_data)
            try:
                doc = Document(temp_file)
                # join all paragraph texts
                attachment_text = "\n".join([p.text for p in doc.paragraphs])
            except Exception as e:
                print(f"error processing docx: {e}")
                attachment_text = ""
            os.remove(temp_file)
        elif ext == '.pdf':
            # use PyPDF2 to extract text from pdf attachment
            try:
                from io import BytesIO
                pdf_reader = PyPDF2.PdfReader(BytesIO(file_data))
                pages = []
                for page in pdf_reader.pages:
                    pages.append(page.extract_text() or "")
                attachment_text = "\n".join(pages)
            except Exception as e:
                print(f"error processing pdf: {e}")
                attachment_text = ""
    except Exception as e:
        print(f"attachment error: {e}")
    return attachment_text

def extract_email_content(service, message):
    # extract main email body and append processed attachment text
    content = ""
    try:
        content += extract_text_from_message(message)
        payload = message.get('payload', {})
        if 'parts' in payload:
            # check each part for attachments
            for part in payload['parts']:
                if part.get('filename') and part.get('body', {}).get('attachmentId'):
                    attachment = process_attachment(service, message.get('id'), part)
                    content += "\n" + attachment
    except Exception as e:
        print(f"error extracting content: {e}")
    return content

def extract_email_data(service, message):
    # extract key fields from a message: id, subject, sender, date, thread id, and cleaned content
    data = {}
    try:
        data['id'] = message.get('id')
        headers = message.get('payload', {}).get('headers', [])
        # build a dict of headers with lower-case keys
        hdrs = {h['name'].lower(): h['value'] for h in headers}
        data['subject'] = hdrs.get('subject', '')
        data['from'] = hdrs.get('from', '')
        data['date'] = hdrs.get('date', '')
        data['conversation_id'] = message.get('threadId', '')
        # extract and clean the main content of the email
        content = extract_email_content(service, message)
        data['content'] = remove_quoted_text(content).strip()
    except Exception as e:
        print(f"error extracting email {message.get('id')}: {e}")
    return data

def incremental_save(emails_list):
    # group emails by conversation id and save them to a json file
    conversations = {}
    for email_obj in emails_list:
        conv = email_obj.get('conversation_id')
        conversations.setdefault(conv, []).append(email_obj)
    conv_list = []
    for conv, emails in conversations.items():
        # sort emails in each conversation by date
        try:
            emails.sort(key=lambda x: email.utils.parsedate_to_datetime(x['date']) if x.get('date') else datetime.datetime.min)
        except Exception:
            pass
        # assign an order number within each conversation
        for i, em in enumerate(emails, start=1):
            em['order'] = i
        conv_list.append({'conversation_id': conv, 'emails': emails})
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(conv_list, f, ensure_ascii=False, indent=2)
    print(f"saved {len(emails_list)} emails so far")

def main():
    # main function to build the email dataset
    try:
        service = build_service()
    except Exception as e:
        print("failed to build service")
        return

    processed_ids = set()  # track processed email ids to avoid duplicates
    emails_list = []       # list to hold all processed email objects
    skip_threads = set()   # threads to skip (too many messages)
    total = 0              # total processed emails counter

    try:
        profile = service.users().getProfile(userId='me').execute()
        user_email = profile.get('emailAddress').lower()
        print(f"user email: {user_email}")
    except Exception as e:
        print("failed to get user profile")
        return

    # retrieve all sent emails using pagination
    sent_msgs = []
    page_token = None
    try:
        while True:
            results = service.users().messages().list(userId='me', labelIds=['SENT'], pageToken=page_token).execute()
            batch = results.get('messages', [])
            sent_msgs.extend(batch)
            print(f"retrieved {len(batch)} sent emails in current page")
            page_token = results.get('nextPageToken')
            if not page_token:
                break
        print(f"total sent emails retrieved: {len(sent_msgs)}")
    except Exception as e:
        print("failed to get sent emails")
        sent_msgs = []

    recipients = set()
    if sent_msgs:
        print("processing sent emails to extract recipients...")
        for idx, msg in enumerate(sent_msgs, start=1):
            if idx % 50 == 0:
                print(f"processed {idx} of {len(sent_msgs)} sent emails")
            try:
                message = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
                headers = message.get('payload', {}).get('headers', [])
                hdrs = {h['name'].lower(): h['value'] for h in headers}
                to_field = hdrs.get('to', '')
                if to_field:
                    # split by comma if multiple recipients
                    for addr in to_field.split(','):
                        norm = normalize_email(addr)
                        if norm and norm != user_email:
                            recipients.add(norm)
            except Exception as e:
                print(f"error processing sent msg {msg['id']}")
        print(f"found {len(recipients)} recipients from sent emails")
    else:
        print("no sent emails found; will sample from inbox")

    # process emails for each recipient from sent emails
    rec_count = 0
    for recipient in recipients:
        rec_count += 1
        print(f"processing recipient {rec_count}/{len(recipients)}: {recipient}")
        try:
            query = f"to:{recipient} OR from:{recipient}"
            search = service.users().messages().list(userId='me', q=query).execute()
            msgs = search.get('messages', [])
            while 'nextPageToken' in search:
                token = search['nextPageToken']
                search = service.users().messages().list(userId='me', q=query, pageToken=token).execute()
                msgs.extend(search.get('messages', []))
            print(f"  found {len(msgs)} messages for {recipient}")
        except Exception as e:
            print(f"error searching for {recipient}")
            continue

        for msg in msgs:
            if total >= MAX_EMAILS:
                break
            msg_id = msg.get('id')
            if msg_id in processed_ids:
                continue
            try:
                message = service.users().messages().get(userId='me', id=msg_id, format='full').execute()
            except Exception as e:
                print(f"error getting msg {msg_id}")
                continue

            thread_id = message.get('threadId')
            if thread_id in skip_threads:
                continue
            try:
                thread = service.users().threads().get(userId='me', id=thread_id).execute()
                if len(thread.get('messages', [])) > 1000:
                    print(f"skipping thread {thread_id} (too many messages)")
                    skip_threads.add(thread_id)
                    continue
            except Exception as e:
                print(f"error getting thread {thread_id}")
                continue

            email_data = extract_email_data(service, message)
            if not email_data.get('content'):
                continue
            emails_list.append(email_data)
            processed_ids.add(msg_id)
            total += 1
            print(f"processed msg {msg_id} (total: {total})")
            if total % INCREMENTAL_SAVE_COUNT == 0:
                try:
                    incremental_save(emails_list)
                except Exception as e:
                    print("error during incremental save")
        if total >= MAX_EMAILS:
            break

    # if total emails processed is less than the cap, sample more from the inbox
    if total < MAX_EMAILS:
        print("sampling additional emails from inbox")
        inbox_msgs = []
        page_token = None
        try:
            while total < MAX_EMAILS:
                results = service.users().messages().list(userId='me', labelIds=['INBOX'], maxResults=500, pageToken=page_token).execute()
                batch = results.get('messages', [])
                if not batch:
                    break
                inbox_msgs.extend(batch)
                print(f"retrieved {len(batch)} inbox emails in current page")
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
            print(f"total inbox messages retrieved for sampling: {len(inbox_msgs)}")
        except Exception as e:
            print("failed to get inbox messages")
            inbox_msgs = []
        random.shuffle(inbox_msgs)
        for msg in inbox_msgs:
            if total >= MAX_EMAILS:
                break
            msg_id = msg.get('id')
            if msg_id in processed_ids:
                continue
            try:
                message = service.users().messages().get(userId='me', id=msg_id, format='full').execute()
            except Exception as e:
                print(f"error getting inbox msg {msg_id}")
                continue

            thread_id = message.get('threadId')
            if thread_id in skip_threads:
                continue
            try:
                thread = service.users().threads().get(userId='me', id=thread_id).execute()
                if len(thread.get('messages', [])) > 1000:
                    print(f"skipping thread {thread_id} (too many messages)")
                    skip_threads.add(thread_id)
                    continue
            except Exception as e:
                print(f"error getting thread {thread_id}")
                continue

            email_data = extract_email_data(service, message)
            if not email_data.get('content'):
                continue
            emails_list.append(email_data)
            processed_ids.add(msg_id)
            total += 1
            print(f"processed inbox msg {msg_id} (total: {total})")
            if total % INCREMENTAL_SAVE_COUNT == 0:
                try:
                    incremental_save(emails_list)
                except Exception as e:
                    print("error during incremental save")
        print("completed sampling from inbox")


    # group emails into conversations and perform final save
    print("grouping emails into conversations and final save")
    conversations = {}
    for em in emails_list:
        conv = em.get('conversation_id')
        conversations.setdefault(conv, []).append(em)
    conv_list = []
    for conv, ems in conversations.items():
        try:
            ems.sort(key=lambda x: email.utils.parsedate_to_datetime(x['date']) if x.get('date') else datetime.datetime.min)
        except Exception:
            pass
        for i, em in enumerate(ems, start=1):
            em['order'] = i
        conv_list.append({'conversation_id': conv, 'emails': ems})
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(conv_list, f, ensure_ascii=False, indent=2)
        print(f"final save complete, {len(conv_list)} conversations saved")
    except Exception as e:
        print("error saving final output")

if __name__ == '__main__':
    main()
