#!/usr/bin/env python3
"""
email_json_database_updater.py
Incrementally appends any brand-new Gmail messages to
server_client_local_files/emails.json, preserving the
conversation structure created by gmail_json_extractor_to_json_best.py.

First-run ingestion:   gmail_json_extractor_to_json_best.py
Subsequent updates:    email_json_database_updater.py   ← this file
"""

from __future__ import annotations

import json
import sys
import time
import datetime as dt
from pathlib import Path
from email.utils import parsedate_to_datetime

# heavy-duty helpers (build_service, extract_email_data, etc.)
from gmail_json_extractor_to_json_best import build_service, extract_email_data

# downstream embedding pre-processor
from preprocess_emails_for_embeddings import main as preprocess_main

# ────────────────────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────────────────────
DATA_FILE = Path("server_client_local_files/emails.json")
BATCH_SIZE = 200          # how many message IDs to pull per API page
MAX_PAGES  = 50           # absolute safety cap
LOG_PREFIX = "[UPDATER]"


# ────────────────────────────────────────────────────────────────────────────────
# Helper functions
# ────────────────────────────────────────────────────────────────────────────────
def load_existing() -> tuple[list[dict], set[str], dict[str, dict], int | None]:
    """
    Returns:
        conversations : list[dict]   – decoded top-level JSON (may be empty)
        existing_ids  : set[str]     – every Gmail message id already stored
        conv_map      : {threadId → conversation-dict}
        newest_epoch  : int | None   – unix time of newest stored email (UTC)
    """
    if not DATA_FILE.exists():
        print(f"{LOG_PREFIX} {DATA_FILE} not found. Creating new DB.")
        return [], set(), {}, None

    try:
        conversations = json.loads(DATA_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        print(f"{LOG_PREFIX} ERROR – cannot parse {DATA_FILE}: {e}")
        sys.exit(1)

    existing_ids: set[str] = set()
    conv_map: dict[str, dict] = {}
    newest_dt: dt.datetime | None = None

    for conv in conversations:
        cid = conv.get("conversation_id")
        conv_map[cid] = conv
        for em in conv.get("emails", []):
            mid = em.get("id")
            if mid:
                existing_ids.add(mid)
            try:
                em_dt = parsedate_to_datetime(em["date"])
                if newest_dt is None or em_dt > newest_dt:
                    newest_dt = em_dt
            except Exception:
                pass

    newest_epoch = int(newest_dt.timestamp()) if newest_dt else None
    print(
        f"{LOG_PREFIX} loaded {len(conversations)} conversations "
        f"({len(existing_ids)} messages, newest={newest_dt})"
    )
    return conversations, existing_ids, conv_map, newest_epoch


def gmail_search_query(newest_epoch: int | None) -> str:
    """
    Build a Gmail search string:
        after:<unix-time>  AND  (in:inbox OR in:sent)
    """
    date_filter = f"after:{newest_epoch}" if newest_epoch else ""
    # 'OR' works in Gmail search queries
    label_filter = "(in:inbox)"
    query = " ".join(x for x in (date_filter, label_filter) if x)
    return query or label_filter  # never return ""


def fetch_new_message_ids(service, q: str) -> list[str]:
    """
    Retrieve *only* the message IDs matching the search query.
    """
    new_ids: list[str] = []
    page_token = None
    page = 0

    while page < MAX_PAGES:
        page += 1
        try:
            resp = (
                service.users()
                .messages()
                .list(
                    userId="me",
                    q=q,
                    maxResults=BATCH_SIZE,
                    pageToken=page_token,
                    includeSpamTrash=False,
                )
                .execute()
            )
        except Exception as e:
            print(f"{LOG_PREFIX} ERROR – Gmail list API failed: {e}")
            break

        ids_in_page = [m["id"] for m in resp.get("messages", [])]
        print(f"{LOG_PREFIX} page {page}: {len(ids_in_page)} msg-ids")
        new_ids.extend(ids_in_page)

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return new_ids


def hydrate_messages(service, ids: list[str], existing_ids: set[str]) -> list[dict]:
    """
    For every id not yet stored, download full message & parse with extract_email_data().
    """
    fresh_messages: list[dict] = []
    for mid in ids:
        if mid in existing_ids:
            continue
        try:
            raw = (
                service.users()
                .messages()
                .get(userId="me", id=mid, format="full")
                .execute()
            )
            data = extract_email_data(service, raw)
        except Exception as e:
            print(f"{LOG_PREFIX} WARN – could not fetch {mid}: {e}")
            continue

        if not data.get("content"):
            # skip completely empty bodies
            continue

        fresh_messages.append(data)

    print(f"{LOG_PREFIX} fetched {len(fresh_messages)} new full messages")
    return fresh_messages


def merge_into_conversations(
    conv_map: dict[str, dict], new_msgs: list[dict]
) -> list[dict]:
    """Insert new messages into conv_map, sort, and re-assign `order` indices."""
    for msg in new_msgs:
        cid = msg.get("conversation_id")
        if not cid:
            # should not happen, but don't crash updater
            continue
        conv = conv_map.setdefault(cid, {"conversation_id": cid, "emails": []})
        conv["emails"].append(msg)

    merged: list[dict] = []
    for conv in conv_map.values():
        emails = conv.get("emails", [])
        try:
            emails.sort(key=lambda e: parsedate_to_datetime(e["date"]))
        except Exception:
            pass
        for idx, em in enumerate(emails, start=1):
            em["order"] = idx
        merged.append({"conversation_id": conv["conversation_id"], "emails": emails})

    print(f"{LOG_PREFIX} merged total conversations: {len(merged)}")
    return merged


def save_and_preprocess(conversations: list[dict]) -> None:
    """Overwrite emails.json and re-run preprocessing step."""
    try:
        DATA_FILE.write_text(
            json.dumps(conversations, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(
            f"{LOG_PREFIX} wrote {len(conversations)} conversations → {DATA_FILE.name}"
        )
    except OSError as e:
        print(f"{LOG_PREFIX} CRITICAL – cannot save JSON: {e}")
        sys.exit(1)

    # regenerate preprocessed_emails.json
    try:
        print(f"{LOG_PREFIX} running downstream preprocessing …")
        preprocess_main()
    except Exception as e:
        print(f"{LOG_PREFIX} CRITICAL – preprocessing failed: {e}")
        sys.exit(1)


# ────────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────────
def main() -> None:
    # 1) read what we already have
    conversations, existing_ids, conv_map, newest_epoch = load_existing()

    # 2) connect to Gmail
    service = build_service()

    # 3) build query & fetch IDs
    query = gmail_search_query(newest_epoch)
    print(f"{LOG_PREFIX} Gmail query → {query!r}")
    candidate_ids = fetch_new_message_ids(service, query)

    # nothing at all?
    unseen_ids = [mid for mid in candidate_ids if mid not in existing_ids]
    if not unseen_ids:
        print(f"{LOG_PREFIX} up-to-date – no new messages.")
        return

    # 4) hydrate & parse
    fresh_messages = hydrate_messages(service, unseen_ids, existing_ids)

    if not fresh_messages:
        print(f"{LOG_PREFIX} no parsable new messages.")
        return

    # 5) merge & persist
    merged_conversations = merge_into_conversations(conv_map, fresh_messages)
    save_and_preprocess(merged_conversations)
    print(f"{LOG_PREFIX} update complete – added {len(fresh_messages)} messages.")


if __name__ == "__main__":
    start = time.perf_counter()
    try:
        main()
    finally:
        elapsed = time.perf_counter() - start
        print(f"{LOG_PREFIX} finished in {elapsed:0.1f}s")
