#!/usr/bin/env python
"""
mock_email_generator.py  —  v7.1
---------------------------------------------------------------------------
Generate Gmail-style mock conversations *reliably*.

Changes in 7.1 (over 7.0)
-------------------------
• Subject/body generation now retries UNTIL valid (max 10 tries) rather than
  giving up after 3 and emitting warnings every time.
• “Fix-it” prompt suffix nudges the model after each bad attempt.
• Subjects that are merely >7 words get auto-trimmed.
• Extra logging only if we hit the hard cap.
"""

from __future__ import annotations

import argparse, json, logging, random, re, uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Sequence

from faker import Faker
from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline

# ─────────────────────────── CONFIG ────────────────────────────────────────
SUBJECT_MAX_WORDS   = 7
BODY_MIN_WORDS      = 40
BODY_MAX_WORDS      = 150
MAX_SUBJECT_TRIES   = 10   # ← increased & loops until success
MAX_BODY_TRIES      = 10
CONV_MIN_LENGTH     = 2
CONV_MAX_LENGTH     = 5

# ──────────────────────── LLM BOOTSTRAP ─────────────────────────────────────
def load_generator(model_name: str, device: str):
    tok = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
    mdl = AutoModelForCausalLM.from_pretrained(model_name, trust_remote_code=True).to(device)
    return pipeline(
        "text-generation",
        model=mdl,
        tokenizer=tok,
        device=0 if device != "cpu" else -1,
        do_sample=True,
        temperature=0.7,
        top_p=0.9,
        max_new_tokens=120,
    )

# ────────────────────────── CLEANING UTILS ────────────────────────────────
MARKUP    = re.compile(r"```.*?```|<\|.*?\|>", re.S)
NON_ASCII = re.compile(r"[^\x00-\x7F]+")
LABEL_RE  = re.compile(r"^[A-Za-z]+:\s*")

def scrub(text: str, strip_label: str | None = None) -> str:
    txt = MARKUP.sub("", text).strip()
    if strip_label:
        txt = re.sub(rf"^{strip_label}\s*:\s*", "", txt, flags=re.I).strip()
    txt = LABEL_RE.sub("", txt)
    txt = NON_ASCII.sub("", txt)
    return txt.strip(' "\'')

def llm(gen, prompt: str, label: str | None = None) -> str:
    return scrub(gen(prompt, return_full_text=False)[0]["generated_text"],
                 strip_label=label)

# ───────────────────────── VALIDATORS ──────────────────────────────────────
def good_subject(s: str) -> bool:
    return 0 < len(s.split()) <= SUBJECT_MAX_WORDS

def good_body(b: str) -> bool:
    wc = len(b.split())
    return BODY_MIN_WORDS <= wc <= BODY_MAX_WORDS and "NNNN" not in b

# ───────────────────────── PROMPT TEXTS ────────────────────────────────────
SUBJECT_PROMPT = """\
### INSTRUCTIONS START ###
OUTPUT FORMAT: ONE line, ≤7 words, plain ASCII, no quotes or labels.
EXAMPLE → Quarterly revenue update
### INSTRUCTIONS END ###

NOW YOU:
Subject:"""

FIRST_BODY_PROMPT = """\
### INSTRUCTIONS START ###
Produce **one** paragraph of 90±10 words (ASCII). No greeting/sign-off/lists.
### EXAMPLE ###
Body: Following our earlier chat, I’m outlining the timeline for…
### INSTRUCTIONS END ###

NOW YOU:
Body:"""

REPLY_BODY_TEMPLATE = """\
### INSTRUCTIONS START ###
Produce **one** paragraph of 90±10 words replying to PREVIOUS_EMAIL.
Must reference at least one idea. No greeting/sign-off/lists.
ASCII only.
### INSTRUCTIONS END ###

PREVIOUS_EMAIL:
{previous}

NOW YOU:
Body:"""

FIX_SUFFIX = "\n\nFIX IT. Follow the rules exactly – do NOT add labels."

# ───────────────────── conversation helpers ────────────────────────────────
faker = Faker()

def rand_start(start: datetime, end: datetime) -> datetime:
    return start + timedelta(seconds=random.randrange(int((end - start).total_seconds())))

def bump(ts: datetime) -> datetime:
    return ts + timedelta(minutes=random.randint(5, 180))

def first_para(txt: str) -> str:
    return txt.split("\n\n", 1)[0].strip()

# --------------------- subject generator ----------------------------------
def generate_subject(gen, topic: str) -> str:
    prompt = SUBJECT_PROMPT
    for attempt in range(1, MAX_SUBJECT_TRIES + 1):
        raw  = llm(gen, prompt, "Subject")
        line = LABEL_RE.sub("", raw.splitlines()[0].strip())
        # auto-trim if only problem is length
        if not good_subject(line) and len(line.split()) > SUBJECT_MAX_WORDS:
            line = " ".join(line.split()[:SUBJECT_MAX_WORDS])
        if good_subject(line):
            return line
        prompt += FIX_SUFFIX  # nudge model harder
    # hard-cap fallback
    logging.warning("Subject generation failed after %d tries → using fallback", MAX_SUBJECT_TRIES)
    return f"{topic.split()[0]} update"[:52]

# --------------------- body generator -------------------------------------
def generate_body(gen, previous: str | None) -> str:
    prompt = FIRST_BODY_PROMPT if previous is None else REPLY_BODY_TEMPLATE.format(previous=previous)
    for attempt in range(1, MAX_BODY_TRIES + 1):
        raw  = llm(gen, prompt, "Body")
        para = LABEL_RE.sub("", first_para(raw))
        if good_body(para):
            return para
        prompt += FIX_SUFFIX
    # last-resort: repeat paragraph until long enough
    logging.warning("Body generation failed after %d tries → padding fallback", MAX_BODY_TRIES)
    while len(para.split()) < BODY_MIN_WORDS:
        para += " " + para.split(".")[0] + "."
    return para[:BODY_MAX_WORDS*10]  # keep JSON small

# --------------------- thread builder -------------------------------------
def build_conversation(topic: str, gen, quota: int,
                       start_win: datetime, end_win: datetime) -> Dict[str, List]:
    length = 1 if quota <= 1 else random.randint(CONV_MIN_LENGTH,
                                                 min(CONV_MAX_LENGTH, quota))
    me, peer = "Idan Morad", faker.name()

    subject  = generate_subject(gen, topic)
    body0    = generate_body(gen, None)

    emails: List[Dict] = []
    ts = rand_start(start_win, end_win)

    emails.append({
        "id": uuid.uuid4().hex,
        "subject": subject,
        "from": me,
        "date": ts.strftime("%a, %d %b %Y %H:%M:%S +0000"),
        "content": body0,
        "order": 1,
    })
    prev_body = body0
    ts = bump(ts)

    for idx in range(2, length + 1):
        sender = peer if idx % 2 == 0 else me
        body   = generate_body(gen, prev_body)
        emails.append({
            "id": uuid.uuid4().hex,
            "subject": f"Re: {subject}",
            "from": sender,
            "date": ts.strftime("%a, %d %b %Y %H:%M:%S +0000"),
            "content": body,
            "order": idx,
        })
        prev_body = body
        ts = bump(ts)

    return {"conversation_id": uuid.uuid4().hex, "emails": emails}

# ─────────────────────────────── main ──────────────────────────────────────
def main(argv: Sequence[str] | None = None):
    p = argparse.ArgumentParser(description="Generate robust mock Gmail conversations")
    p.add_argument("-o", "--output-path",
                   default="server_client_local_files/mock_preprocessed_emails.json")
    p.add_argument("-m", "--model-name", default="ministral/Ministral-3b-instruct")
    p.add_argument("-d", "--device", default="cpu")
    p.add_argument("--seed", type=int)
    p.add_argument("--max-emails", type=int, default=100)
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")

    if args.seed is not None:
        random.seed(args.seed); Faker.seed(args.seed)

    gen = load_generator(args.model_name, args.device)

    topics = [
        "Motorsports Results", "Food & Dining", "Job Applications & Job Searching",
        "Medical / Appointments", "Gaming Sessions", "Stock Market / Stocks",
        "Casual Catch-Ups",
    ]

    start_win = datetime.now(timezone.utc) - timedelta(days=4 * 365)
    end_win   = datetime.now(timezone.utc)

    conversations: List[Dict] = []
    total = 0
    while total < args.max_emails:
        remaining = args.max_emails - total
        topic     = random.choice(topics)
        conv      = build_conversation(topic, gen, remaining, start_win, end_win)
        conversations.append(conv)
        total += len(conv["emails"])
        logging.info("Conv %s | %d emails | topic=%s | total=%d/%d",
                     conv["conversation_id"][:8], len(conv["emails"]),
                     topic, total, args.max_emails)

    out = Path(args.output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(conversations, ensure_ascii=False, indent=2),
                   encoding="utf-8")
    logging.info("DONE  %d conv / %d emails  →  %s", len(conversations), total, out)

if __name__ == "__main__":
    main()
