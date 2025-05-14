#!/usr/bin/env python3
"""
incremental_email_updater.py - Updates existing email database with new messages
"""

import json
import sys
from email.utils import parsedate_to_datetime

# Shared functions from gmail_json_extractor_to_json_best.py
from gmail_json_extractor_to_json_best import build_service, extract_email_data

# Preprocessing function from preprocess_emails_for_embeddings.py
from preprocess_emails_for_embeddings import main as preprocess_main

# Configuration
EXISTING_EMAILS_FILE = "server_client_local_files/emails.json"


def load_existing_data():
    """Load existing emails, build ID lookup and conversation map."""
    try:
        with open(EXISTING_EMAILS_FILE, 'r', encoding='utf-8') as f:
            existing_convs = json.load(f)
        print(f"[INFO] Loaded {len(existing_convs)} conversations from {EXISTING_EMAILS_FILE}")
    except FileNotFoundError:
        print(f"[WARN] {EXISTING_EMAILS_FILE} not found. Starting with empty database.")
        existing_convs = []
    except json.JSONDecodeError as e:
        print(f"[ERROR] Failed to parse {EXISTING_EMAILS_FILE}: {e}")
        sys.exit(1)

    existing_ids = set()
    conv_map = {}
    for conv in existing_convs:
        cid = conv.get('conversation_id')
        conv_map[cid] = conv
        for em in conv.get('emails', []):
            eid = em.get('id')
            if eid:
                existing_ids.add(eid)
    print(f"[INFO] Found {len(existing_ids)} existing email IDs")
    return existing_convs, existing_ids, conv_map


def find_new_messages(service, existing_ids):
    """Fetch new messages until an existing ID is encountered."""
    new_msgs = []
    page_token = None
    stop = False

    while not stop:
        print(f"[INFO] Listing Gmail messages (page_token={page_token})")
        try:
            resp = service.users().messages().list(
                userId='me', labelIds=['INBOX','SENT'], maxResults=100, pageToken=page_token
            ).execute()
        except Exception as e:
            print(f"[ERROR] Gmail list failed: {e}")
            break

        msgs = resp.get('messages', [])
        if not msgs:
            print("[INFO] No more messages returned.")
            break

        for m in msgs:
            mid = m.get('id')
            print(f"  Checking message id={mid}...", end=' ')
            if mid in existing_ids:
                print("exists, stopping fetch.")
                stop = True
                break

            print("new, fetching details.")
            try:
                full = service.users().messages().get(
                    userId='me', id=mid, format='full'
                ).execute()
                data = extract_email_data(service, full)
            except Exception as e:
                print(f"[ERROR] Failed to fetch/process {mid}: {e}")
                continue

            if not data.get('content'):
                print(f"[WARN] id={mid} has empty content, skipping.")
                continue

            subj = data.get('subject','')
            print(f"  → Queued new email id={mid}, subject='{subj[:50]}'")
            new_msgs.append(data)

        page_token = resp.get('nextPageToken')
        if stop or not page_token:
            break

    print(f"[INFO] Collected {len(new_msgs)} new message(s)")
    return list(reversed(new_msgs))  # oldest first


def merge_conversations(conv_map, new_msgs):
    """Insert new messages into conversation map, sort & renumber."""
    for msg in new_msgs:
        cid = msg.get('conversation_id')
        if not cid:
            continue
        if cid not in conv_map:
            conv_map[cid] = {'conversation_id': cid, 'emails': []}
        conv_map[cid]['emails'].append(msg)

    merged = []
    for conv in conv_map.values():
        emails = conv.get('emails', [])
        try:
            emails.sort(key=lambda x: parsedate_to_datetime(x.get('date','')))
        except Exception:
            pass
        # renumber order
        for idx, em in enumerate(emails, start=1):
            em['order'] = idx
        merged.append({'conversation_id': conv['conversation_id'], 'emails': emails})

    print(f"[INFO] Merged into {len(merged)} conversations")
    return merged


def save_updated_data(updated_convs):
    """Save raw JSON and run preprocessing pipeline."""
    try:
        with open(EXISTING_EMAILS_FILE, 'w', encoding='utf-8') as f:
            json.dump(updated_convs, f, ensure_ascii=False, indent=2)
        print(f"[INFO] Saved updated database with {len(updated_convs)} conversations")
    except Exception as e:
        print(f"[ERROR] Cannot write {EXISTING_EMAILS_FILE}: {e}")
        sys.exit(1)

    # regenerate preprocessed_emails.json in-process
    try:
        print("[INFO] Regenerating preprocessed_emails.json ...")
        preprocess_main()
        print("[INFO] Preprocessing completed successfully.")
    except Exception as e:
        print(f"[ERROR] Preprocessing failed: {e}")
        sys.exit(1)


def main():
    try:
        existing_convs, existing_ids, conv_map = load_existing_data()
        service = build_service()
        new_msgs = find_new_messages(service, existing_ids)
        if not new_msgs:
            print("[INFO] No new messages to add.")
            return
        updated_convs = merge_conversations(conv_map, new_msgs)
        save_updated_data(updated_convs)
        print("[INFO] Update complete.")
    except Exception as e:
        print(f"[CRITICAL] Unexpected error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
