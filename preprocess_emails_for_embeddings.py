#!/usr/bin/env python3
# script to preprocess emails from emails.json for embedding, outputting preprocessed_emails.json

import json  # for reading and writing json files
import re    # for regex operations
from bs4 import BeautifulSoup  # for removing html tags

def remove_html_tags(text):
    # remove html tags using beautifulsoup and return plain text
    return BeautifulSoup(text, "html.parser").get_text()

def remove_newlines_tabs(text):
    # replace newline, tab, and carriage returns with a space and collapse extra spaces
    text = text.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
    return re.sub(r'\s+', ' ', text).strip()

def preprocess_field(text, semantic_label):
    # remove html tags and extra whitespace then prepend a semantic label
    cleaned = remove_html_tags(text)  # strip html
    cleaned = remove_newlines_tabs(cleaned)  # clean whitespace characters
    return f"{semantic_label}: {cleaned}"  # add label and return

def preprocess_email(email):
    # process each email field and add semantic labels
    preprocessed = {}
    if 'id' in email:
        preprocessed['id'] = preprocess_field(email['id'], "ID")
    if 'subject' in email:
        preprocessed['subject'] = preprocess_field(email['subject'], "Title")
    if 'from' in email:
        preprocessed['from'] = preprocess_field(email['from'], "From")
    if 'date' in email:
        preprocessed['date'] = preprocess_field(email['date'], "Date")
    if 'conversation_id' in email:
        preprocessed['conversation_id'] = preprocess_field(email['conversation_id'], "Conversation")
    if 'content' in email:
        preprocessed['content'] = preprocess_field(email['content'], "Content")
    if 'order' in email:
        # convert numeric order to string with label
        preprocessed['order'] = f"Order: {email['order']}"
    return preprocessed

def preprocess_conversations(conversations):
    # loop through each conversation and preprocess the conversation id and emails
    for conv in conversations:
        if 'conversation_id' in conv:
            conv['conversation_id'] = preprocess_field(conv['conversation_id'], "Conversation")
        if 'emails' in conv:
            conv['emails'] = [preprocess_email(email) for email in conv['emails']]
    return conversations

def main():
    input_file = "server_client_local_files/emails.json"  # input file name
    output_file = "server_client_local_files/preprocessed_emails.json"  # output file name

    print("loading emails.json...")  # progress printing
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)  # load original emails json
    print("emails.json loaded successfully")

    print("preprocessing conversations and emails...")  # progress printing
    preprocessed_data = preprocess_conversations(data)  # process the data

    print("saving preprocessed data to preprocessed_emails.json...")  # progress printing
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(preprocessed_data, f, ensure_ascii=False, indent=2)  # write output json
    print("preprocessed data saved successfully, script complete!")  # final print

if __name__ == '__main__':
    main()  # run the main function
