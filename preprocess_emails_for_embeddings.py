# preprocess emails for embedding – clean html/whitespace, drop labels

import json
import re
from bs4 import BeautifulSoup

def remove_html_tags(text):
    # strip html tags
    return BeautifulSoup(text or "", "html.parser").get_text()

def remove_newlines_tabs(text):
    # collapse whitespace into single spaces
    text = text.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
    return re.sub(r'\s+', ' ', text).strip()

def clean_text(text):
    # clean html and extra whitespace
    if text is None:
        return None
    return remove_newlines_tabs(remove_html_tags(text))

def preprocess_email(email):
    # clean individual email fields, keep raw text and numeric order
    out = {}
    if 'id' in email:
        out['id'] = clean_text(email['id'])
    if 'subject' in email:
        out['subject'] = clean_text(email['subject'])
    # sometimes the sender field is named 'from' or 'sender'
    if 'from' in email:
        out['from'] = clean_text(email['from'])
    elif 'sender' in email:
        out['from'] = clean_text(email['sender'])
    if 'date' in email:
        out['date'] = clean_text(email['date'])
    if 'content' in email:
        out['content'] = clean_text(email['content'])
    if 'order' in email:
        # keep numeric order for proper ingestion
        try:
            out['order'] = int(email['order'])
        except Exception:
            out['order'] = None
    return out

def preprocess_conversations(conversations):
    # iterate over each conversation and its emails
    for conv in conversations:
        if 'conversation_id' in conv:
            conv['conversation_id'] = clean_text(conv['conversation_id'])
        if 'emails' in conv and isinstance(conv['emails'], list):
            conv['emails'] = [preprocess_email(e) for e in conv['emails']]
    return conversations

def main():
    input_file  = "server_client_local_files/emails.json"
    output_file = "server_client_local_files/preprocessed_emails.json"

    print("loading raw emails…")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print("cleaning conversations and emails…")
    cleaned = preprocess_conversations(data)

    print("writing cleaned JSON…")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)

    print("done!")

if __name__ == "__main__":
    main()
