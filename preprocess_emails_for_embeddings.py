# #!/usr/bin/env python3
# # script to preprocess emails from emails.json for embedding, outputting preprocessed_emails.json
#
# import json  # for reading and writing json files
# import re    # for regex operations
# from bs4 import BeautifulSoup  # for removing html tags
#
# def remove_html_tags(text):
#     # remove html tags using beautifulsoup and return plain text
#     return BeautifulSoup(text, "html.parser").get_text()
#
# def remove_newlines_tabs(text):
#     # replace newline, tab, and carriage returns with a space and collapse extra spaces
#     text = text.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
#     return re.sub(r'\s+', ' ', text).strip()
#
# def preprocess_field(text, semantic_label):
#     # remove html tags and extra whitespace then prepend a semantic label
#     cleaned = remove_html_tags(text)  # strip html
#     cleaned = remove_newlines_tabs(cleaned)  # clean whitespace characters
#     return f"{semantic_label}: {cleaned}"  # add label and return
#
# def preprocess_email(email):
#     # process each email field and add semantic labels
#     preprocessed = {}
#     if 'id' in email:
#         preprocessed['id'] = preprocess_field(email['id'], "ID")
#     if 'subject' in email:
#         preprocessed['subject'] = preprocess_field(email['subject'], "Title")
#     if 'from' in email:
#         preprocessed['from'] = preprocess_field(email['from'], "From")
#     if 'date' in email:
#         preprocessed['date'] = preprocess_field(email['date'], "Date")
#     if 'conversation_id' in email:
#         preprocessed['conversation_id'] = preprocess_field(email['conversation_id'], "Conversation")
#     if 'content' in email:
#         preprocessed['content'] = preprocess_field(email['content'], "Content")
#     if 'order' in email:
#         # convert numeric order to string with label
#         preprocessed['order'] = f"Order: {email['order']}"
#     return preprocessed
#
# def preprocess_conversations(conversations):
#     # loop through each conversation and preprocess the conversation id and emails
#     for conv in conversations:
#         if 'conversation_id' in conv:
#             conv['conversation_id'] = preprocess_field(conv['conversation_id'], "Conversation")
#         if 'emails' in conv:
#             conv['emails'] = [preprocess_email(email) for email in conv['emails']]
#     return conversations
#
# def main():
#     input_file = "server_client_local_files/emails.json"  # input file name
#     output_file = "server_client_local_files/preprocessed_emails.json"  # output file name
#
#     print("loading emails.json...")  # progress printing
#     with open(input_file, 'r', encoding='utf-8') as f:
#         data = json.load(f)  # load original emails json
#     print("emails.json loaded successfully")
#
#     print("preprocessing conversations and emails...")  # progress printing
#     preprocessed_data = preprocess_conversations(data)  # process the data
#
#     print("saving preprocessed data to preprocessed_emails.json...")  # progress printing
#     with open(output_file, 'w', encoding='utf-8') as f:
#         json.dump(preprocessed_data, f, ensure_ascii=False, indent=2)  # write output json
#     print("preprocessed data saved successfully, script complete!")  # final print
#
# if __name__ == '__main__':
#     main()  # run the main function


#!/usr/bin/env python3
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
