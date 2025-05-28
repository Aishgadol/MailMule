#!/usr/bin/env python3
"""
email_pipeline.py - unified script for fetching and preprocessing gmail messages
then bulk-upserting cleaned emails into a per-user database via db_handler
embeddings happen later in a separate script
"""
import os
import re
import json
import base64
import logging
from pathlib import Path
from email.utils import parsedate_to_datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from bs4 import BeautifulSoup

import db_handler

# configure logger for progress and errors
logger = logging.getLogger('email_pipeline')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger.addHandler(handler)

# oauth scope and token caching directory
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
TOKEN_DIR = Path('tokens')
TOKEN_DIR.mkdir(exist_ok=True)
# limit number of emails per retrieval to avoid huge payloads
MAX_EMAILS = 5000


def get_credentials(user_id: str) -> Credentials:
    # load or refresh oauth tokens for the given user
    token_file = TOKEN_DIR / f"{user_id}_token.json"
    creds = None
    # attempt to load saved credentials
    if token_file.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(token_file), SCOPES)
        except Exception:
            # malformed or invalid token file
            creds = None
    # if no valid creds, refresh or run full oauth
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info('attempting token refresh')
            try:
                creds.refresh(Request())
            except Exception:
                logger.warning('refresh failed, running full oauth flow')
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
        else:
            logger.info('running full oauth flow')
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # save credentials for next run
        with open(token_file, 'w') as tf:
            tf.write(creds.to_json())
    return creds


def build_service(user_id: str):
    # create authenticated gmail api client
    creds = get_credentials(user_id)
    service = build('gmail', 'v1', credentials=creds)
    logger.info('gmail service ready')
    return service


def clean_text(text: str) -> str:
    # strip html tags and collapse whitespace
    if not text:
        return ''
    # get plain text via bs4
    plain = BeautifulSoup(text, 'html.parser').get_text()
    return re.sub(r'\s+', ' ', plain).strip()


def remove_quoted_text(text: str) -> str:
    # remove lines starting with '>' and reply headers
    lines = text.splitlines()
    kept = []
    for line in lines:
        if re.match(r'^on .+ wrote:$', line, re.IGNORECASE):
            break
        if line.strip().startswith('>'):
            continue
        kept.append(line)
    return '\n'.join(kept).strip()


def extract_text_from_message(message: dict) -> str:
    # pull text/plain and text/html parts, decode with charset fallback, then strip quotes
    content = ''
    payload = message.get('payload', {})
    parts = payload.get('parts') or [payload]
    for part in parts:
        data = part.get('body', {}).get('data')
        if not data:
            continue
        raw_bytes = base64.urlsafe_b64decode(data.encode('ASCII'))
        # try utf-8 then windows-1255 for hebrew support
        try:
            decoded = raw_bytes.decode('utf-8')
        except UnicodeDecodeError:
            decoded = raw_bytes.decode('windows-1255', errors='ignore')
        mime = part.get('mimeType', '')
        if mime in ('text/plain', 'text/html'):
            content += decoded
    return remove_quoted_text(content)


def extract_message_data(raw: dict) -> dict:
    # parse headers and build content string including attachment filenames
    headers = raw.get('payload', {}).get('headers', [])
    hdr_map = {h['name'].lower(): h['value'] for h in headers}
    # base cleaned content
    body = extract_text_from_message(raw)
    # find attachments filenames
    filenames = []
    for part in raw.get('payload', {}).get('parts', []):
        fn = part.get('filename')
        if fn and part.get('body', {}).get('attachmentId'):
            filenames.append(fn)
    # append filenames to content if any
    if filenames:
        body += '\n\nfiles:' + ''.join(f"\n{fn}" for fn in filenames)
    return {
        'id': raw.get('id'),
        'subject': hdr_map.get('subject', ''),
        'from': hdr_map.get('from', ''),
        'date': hdr_map.get('date', ''),
        'conversation_id': raw.get('threadId', ''),
        'content': clean_text(body)
    }


def fetch_all_message_ids(service) -> list[str]:
    # get up to MAX_EMAILS message ids from mailbox
    ids = []
    token = None
    while len(ids) < MAX_EMAILS:
        resp = service.users().messages().list(
            userId='me', pageToken=token, maxResults=500
        ).execute()
        batch = resp.get('messages', [])
        for m in batch:
            if len(ids) >= MAX_EMAILS:
                break
            ids.append(m.get('id'))
        token = resp.get('nextPageToken')
        if not token:
            break
    logger.info(f'fetched {len(ids)} message ids (capped at {MAX_EMAILS})')
    return ids


def fetch_new_message_ids(service, min_ts: float | None) -> list[str]:
    # get up to MAX_EMAILS new message ids after timestamp
    query = f"after:{int(min_ts)}" if min_ts else ''
    ids = []
    token = None
    while len(ids) < MAX_EMAILS:
        resp = service.users().messages().list(
            userId='me', q=query, pageToken=token, maxResults=500
        ).execute()
        batch = resp.get('messages', [])
        for m in batch:
            if len(ids) >= MAX_EMAILS:
                break
            ids.append(m.get('id'))
        token = resp.get('nextPageToken')
        if not token:
            break
    logger.info(f'fetched {len(ids)} new message ids (capped at {MAX_EMAILS})')
    return ids


def hydrate_messages(service, ids: list[str], seen_ids: set[str]) -> list[dict]:
    # download and parse each id not already seen
    fresh = []
    for mid in ids:
        if mid in seen_ids:
            continue
        try:
            raw = service.users().messages().get(
                userId='me', id=mid, format='full'
            ).execute()
            msg = extract_message_data(raw)
            if msg.get('content'):
                fresh.append(msg)
        except Exception:
            logger.warning(f'skipping {mid} due to fetch error')
    logger.info(f'hydrated {len(fresh)} messages')
    return fresh


def run(mode: str, user_id: str) -> None:
    """
    mode create for full pull update for delta
    user_id database name
    """
    try:
        # ensure user database
        if not db_handler.check_if_exists(db_id=user_id):
            db_handler.create_db(db_id=user_id)
            logger.info(f'created database for {user_id}')
            existing = []
            actual = 'create'
        else:
            existing = db_handler.get_db(db_id=user_id)
            logger.info(f'loaded {len(existing)} existing emails')
            actual = mode

        # build gmail client\	service = build_service(user_id)

        # collect seen ids and newest timestamp
        seen, times = set(), []
        for em in existing:
            mid = em.get('id')
            if mid:
                seen.add(mid)
            try:
                ts = parsedate_to_datetime(em['date']).timestamp()
                times.append(ts)
            except Exception:
                pass
        newest = max(times) if times else None

        # fetch ids based on mode
        if actual == 'create':
            ids = fetch_all_message_ids(service)
        else:
            ids = fetch_new_message_ids(service, newest)

        # download and parse
        fresh = hydrate_messages(service, ids, seen)
        if not fresh:
            logger.info('no new emails to process')
            return

        # clean each email record and prepare for db
        to_insert = []
        for em in fresh:
            to_insert.append({
                'id': em['id'],
                'conversation_id': em['conversation_id'],
                'subject': clean_text(em['subject']),
                'from': clean_text(em['from']),
                'date': em['date'],
                'content': clean_text(em['content']),
            })

        # bulk insert all cleaned emails, let db_handler handle conversations
        db_handler.db_insert(to_insert, db_id=user_id)
        logger.info(f'bulk-upserted {len(to_insert)} emails for {user_id}')
        logger.info('email pipeline completed successfully')

    except Exception:
        logger.error('pipeline error occurred', exc_info=True)
        raise
