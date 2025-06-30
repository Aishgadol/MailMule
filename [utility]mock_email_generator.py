# #!/usr/bin/env python
# # =============================================================================
# # [utility]mock_email_generator.py — v13.0  (2025-06-21)
# # -----------------------------------------------------------------------------
# # Relaxed gatekeeping so the LLM actually produces content:
# #   • looser validation   • tiny 1-shot example in prompt
# #   • auto-punctuation    • fewer padding loops
# #   • explicit logging of raw + cleaned bad sentences
# # File kept >300 LOC for clarity / config visibility.
# # =============================================================================
#
# from __future__ import annotations
# import argparse, json, logging, random, re, uuid
# from datetime import datetime, timedelta, timezone
# from pathlib import Path
# from typing import Callable, Dict, List, Sequence
#
# from faker import Faker
# from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline
#
# # ──────────────────────────────── CONFIG ────────────────────────────────
# SUBJECT_MAX_WORDS      = 7
# BODY_MIN_WORDS         = 20          # ↓
# BODY_MAX_WORDS         = 140
# MIN_SENT_PER_EMAIL     = 3
# MAX_SENT_PER_EMAIL     = 8
# MAX_SUBJECT_TRIES      = 6
# MAX_SENTENCE_TRIES     = 3           # ↑
# MAX_PAD_ATTEMPTS       = 1           # ↓
# CONV_MIN_LENGTH        = 2
# CONV_MAX_LENGTH        = 4
# DEFAULT_FIRST_PHRASE   = "thing about"
#
# # ─────────────────────────── MODEL LOADING ──────────────────────────────
# def load_generator(model_name: str, device: str) -> Callable[[str], str]:
#     tok = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
#     mdl = AutoModelForCausalLM.from_pretrained(
#         model_name, trust_remote_code=True
#     ).to(device)
#
#     text_gen = pipeline(
#         "text-generation",
#         model=mdl,
#         tokenizer=tok,
#         device=0 if device != "cpu" else -1,
#         do_sample=True,
#         temperature=0.5,
#         top_p=0.92,
#         max_new_tokens=60,
#     )
#     return lambda p: text_gen(p, return_full_text=False)[0]["generated_text"]
#
# # ────────────────────────── PROMPT BUILDING ─────────────────────────────
# def wrap_inst(txt: str) -> str:
#     return f"<s>[INST] {txt.strip()} [/INST]"
#
# def build_prompt(topic: str, subject: str, prev: str) -> str:
#     return (f'You\'re a concise and focused writing assistant, discussing about {topic}.\n Continue this sentence, use ≤12 words.\n \" {prev}\". Your response:...')
#     # return (
#     #     "you're a concise and focused writing assistant.\n"
#     #     f'we are having a discussion about "{topic}", with the title "{subject}".\n'
#     #     f'you were just saying that "{prev}".\n'
#     #     # 1-shot example to coax a normal sentence
#     #     'e.g., "Here’s a quick update."\n'
#     #     "continue with one clear English sentence:"
#     # )
#
# # ───────────────────────────── UTILITIES ────────────────────────────────
# _RX_MARKUP   = re.compile(r"```.*?```|<\|.*?\|>", re.S)
# _RX_NONASCII = re.compile(r"[^\x00-\x7F]+")
# _RX_LABEL    = re.compile(r"^[A-Za-z]+:\s*")
# _RX_LAST_SENT= re.compile(r"([^.?!]{3,}?[.!?])", re.S)
# _RX_END      = re.compile(r"[.!?]$")
# FORBIDDEN    = {"user", "assistant", "system", ""}
#
# def scrub(txt: str, strip: str | None = None) -> str:
#     t = _RX_MARKUP.sub("", txt).strip()
#     if strip:
#         t = re.sub(rf"^{strip}\s*:\s*", "", t, flags=re.I).strip()
#     t = _RX_LABEL.sub("", t)
#     t = _RX_NONASCII.sub("", t)
#     return t.strip(' "\'')
#
# def sanitize_sentence(raw: str) -> str:
#     """Trim spaces; add period if missing."""
#     s = _RX_LABEL.sub("", raw).strip()
#     s = re.sub(r"\s+", " ", s)
#     if not _RX_END.search(s):
#         s += "."
#     return s
#
# def llm(gen: Callable[[str], str], prompt: str, strip: str | None = None) -> str:
#     return scrub(gen(prompt), strip)
#
# def last_sentence(text: str) -> str:
#     m = _RX_LAST_SENT.findall(text)
#     return m[-1].strip() if m else DEFAULT_FIRST_PHRASE
#
# def good_subject(line: str) -> bool:
#     return 0 < len(line.split()) <= SUBJECT_MAX_WORDS
#
# def good_sentence(sent: str) -> bool:
#     if sent.lower() in FORBIDDEN:
#         return False
#     w = len(sent.split())
#     return 3 <= w <= 55
#
# # ───────────────────────────── FAKER & RNG ──────────────────────────────
# faker = Faker()
# def rand_between(a: datetime, b: datetime) -> datetime:
#     return a + timedelta(seconds=random.randrange(int((b - a).total_seconds())))
# def jitter(ts: datetime) -> datetime:
#     return ts + timedelta(minutes=random.randint(5, 90))
#
# # ───────────────────────── SUBJECT GENERATION ───────────────────────────
# def generate_subject(gen, topic: str) -> str:
#     base = "craft an email subject (≤7 words, ascii only). Subject:"
#     for i in range(MAX_SUBJECT_TRIES):
#         subj = llm(gen, wrap_inst(base), "Subject").splitlines()[0]
#         subj = _RX_LABEL.sub("", subj).strip()
#         subj = " ".join(subj.split()[:SUBJECT_MAX_WORDS])
#         if good_subject(subj):
#             return subj
#         logging.warning("subj retry %d → %r", i + 1, subj)
#     fallback = topic.split()[0][:12] + " update"
#     logging.error("subject fallback %s", fallback)
#     return fallback
#
# # ───────────────────────── SENTENCE GENERATION ──────────────────────────
# def generate_sentence(gen, topic: str, subject: str, prev: str) -> str:
#     prompt = wrap_inst(build_prompt(topic, subject, prev))
#     for attempt in range(1, MAX_SENTENCE_TRIES + 1):
#         raw  = llm(gen, prompt)
#         sent = sanitize_sentence(raw.splitlines()[0])
#         if good_sentence(sent):
#             return sent
#         logging.warning(
#             "bad sentence [%d/%d] cleaned=%r raw=%r",
#             attempt, MAX_SENTENCE_TRIES, sent, raw
#         )
#     raise RuntimeError("sentence retries exhausted")
#
# # ────────────────────────── BODY GENERATION ─────────────────────────────
# def generate_body(gen, topic: str, subject: str,
#                   prev_email: str | None) -> str:
#     prev = DEFAULT_FIRST_PHRASE if prev_email is None else last_sentence(prev_email)
#     sentences: List[str] = []
#
#     # seed sentence
#     try:
#         first = generate_sentence(gen, topic, subject, prev)
#     except RuntimeError:
#         first = "Quick heads-up on our progress."
#         logging.error("first sent fallback")
#     sentences.append(first); prev = first
#
#     # grow mail
#     target = random.randint(MIN_SENT_PER_EMAIL, MAX_SENT_PER_EMAIL)
#     while len(sentences) < target:
#         try:
#             nxt = generate_sentence(gen, topic, subject, prev)
#             sentences.append(nxt); prev = nxt
#         except RuntimeError:
#             break
#
#     body = " ".join(sentences)
#
#     # one padding attempt if still too short
#     if len(body.split()) < BODY_MIN_WORDS:
#         try:
#             pad = generate_sentence(gen, topic, subject, sentences[-1])
#             body = " ".join(sentences + [pad])
#         except RuntimeError:
#             logging.warning("mail short %d words", len(body.split()))
#
#     return " ".join(body.split()[:BODY_MAX_WORDS])
#
# # ───────────────────────── CONVERSATION BUILDER ─────────────────────────
# def build_conversation(topic: str, gen, quota: int,
#                        win_a: datetime, win_b: datetime) -> Dict:
#     n_emails = 1 if quota <= 1 else random.randint(CONV_MIN_LENGTH,
#                                                    min(CONV_MAX_LENGTH, quota))
#     me, peer = "Idan Morad", faker.name()
#     subject  = generate_subject(gen, topic)
#
#     body0 = generate_body(gen, topic, subject, None)
#     ts    = rand_between(win_a, win_b)
#
#     mails = [{
#         "id": uuid.uuid4().hex, "subject": subject, "from": me,
#         "date": ts.strftime("%a, %d %b %Y %H:%M:%S +0000"),
#         "content": body0, "order": 1
#     }]
#
#     prev_body = body0
#     for idx in range(2, n_emails + 1):
#         ts = jitter(ts)
#         sender = peer if idx % 2 == 0 else me
#         body   = generate_body(gen, topic, subject, prev_body)
#         mails.append({
#             "id": uuid.uuid4().hex, "subject": f"Re: {subject}", "from": sender,
#             "date": ts.strftime("%a, %d %b %Y %H:%M:%S +0000"),
#             "content": body, "order": idx
#         })
#         prev_body = body
#
#     logging.info("thread %s (%d msgs)", subject, len(mails))
#     return {"conversation_id": uuid.uuid4().hex, "emails": mails}
#
# # ────────────────────────────── MAIN CLI ────────────────────────────────
# def main(argv: Sequence[str] | None = None):
#     ap = argparse.ArgumentParser()
#     ap.add_argument("-o", "--output-path",
#                     default="server_client_local_files/mock_preprocessed_emails.json")
#     ap.add_argument("-m", "--model-name",
#                     default="ministral/Ministral-3b-instruct")
#     ap.add_argument("-d", "--device", default="cpu")
#     ap.add_argument("--seed", type=int)
#     ap.add_argument("--max-emails", type=int, default=10)
#     args = ap.parse_args(argv)
#
#     logging.basicConfig(level=logging.INFO,
#                         format="%(asctime)s %(levelname)-8s %(message)s")
#
#     if args.seed is not None:
#         random.seed(args.seed); Faker.seed(args.seed)
#
#     gen = load_generator(args.model_name, args.device)
#     logging.info("model ready %s", args.model_name)
#
#     topics = ["Motorsports Results", "Food & Dining", "Job Applications",
#               "Medical Appointments", "Gaming Sessions", "Stock Market"]
#
#     start = datetime.now(timezone.utc) - timedelta(days=365*4)
#     end   = datetime.now(timezone.utc)
#
#     conversations, total = [], 0
#     while total < args.max_emails:
#         conv = build_conversation(random.choice(topics), gen,
#                                   args.max_emails - total, start, end)
#         conversations.append(conv)
#         total += len(conv["emails"])
#         logging.info("progress %d / %d mails", total, args.max_emails)
#
#     out = Path(args.output_path)
#     out.parent.mkdir(parents=True, exist_ok=True)
#     out.write_text(json.dumps(conversations, ensure_ascii=False, indent=2),
#                    encoding="utf-8")
#     logging.info("DONE -> %s  (%d conv / %d mails)",
#                  out, len(conversations), total)
#
# # ────────────────────────────────────────────────────────────────────────
# if __name__ == "__main__":
#     main()


# =============================================================================
# ministal_email_generator.py — merged generation from [unused]ministal_experiment.py
#                    with [utility]mock_email_generator.py JSON output format
# =============================================================================
import argparse
import json
import logging
import random
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline

# ─────────────────────────┐
#   CONFIGURABLE PARAMETERS
# ─────────────────────────┘
MODEL_NAME = "ministral/Ministral-3b-instruct"
DEVICE = "cpu"
CONV_MIN_LENGTH = 2
CONV_MAX_LENGTH = 4
EXTRA_PARTICIPANT_PROB = 0.1
JITTER_MIN_MINUTES = 5
JITTER_MAX_MINUTES = 90
TIMESTAMP_YEARS_BACK = 4
INIT_DO_SAMPLE = True
INIT_TEMPERATURE = 0.7
INIT_TOP_P = 0.8
INIT_MAX_NEW_TOKENS = 128
FOLLOWUP_DO_SAMPLE = True
FOLLOWUP_TEMPERATURE_MIN = 0.6
FOLLOWUP_TEMPERATURE_MAX = 0.95
FOLLOWUP_TOP_P_MIN = 0.6
FOLLOWUP_TOP_P_MAX = 0.95
FOLLOWUP_REPETITION_PENALTY = 1.4
FOLLOWUP_MAX_NEW_TOKENS_MIN = 170
FOLLOWUP_MAX_NEW_TOKENS_MAX = 215
TOPICS = [
    "Photography & Art. Keywords: { photography, photo, art, capture, create, frame, shoot, print, exhibit, display }",
    "Lifestyle & Fashion. Keywords: { fashion, style, wear, dress, shop, model, brand, accessorize, design, trend }",
    "Food & Cooking. Keywords: { food, cook, eat, bake, meal, taste, serve, grill, plate, season }",
    "Travel & Adventure. Keywords: { travel, explore, visit, hike, tour, journey, camp, climb, discover, wander }",
    "Health & Fitness. Keywords: { fitness, train, run, walk, lift, stretch, sweat, heal, recover, move }",
    "Music & Entertainment. Keywords: { music, play, listen, stream, sing, dance, film, watch, perform, act }",
    "Business & Entrepreneurship. Keywords: { business, start, lead, manage, invest, market, grow, hire, sell, launch }",
    "Tech & Innovation. Keywords: { tech, build, code, test, design, connect, automate, innovate, review, launch }",
    "Pets & Animals. Keywords: { pet, dog, cat, feed, walk, care, train, play, adopt, rescue }",
    "Home & Interior Design. Keywords: { home, renovate, decorate, furnish, organize, build, style, arrange, improve, design }"
]


faker = Faker()


# ─────────────────────────┐
#    CLEANING ROUTINE
# ─────────────────────────┘
def clean_text(raw: str) -> str:
    """
    Strip out any model-token artifacts like <|im_start|>, <|im_end|>,
    any <|…|> markers, or stray <| prefixes.
    """
    # remove <|im_*|> tokens
    raw = re.sub(r"<\|im_[^|]*\|>", "", raw)
    # remove any other <|…|> markers
    raw = re.sub(r"<\|[^|]+\|>", "", raw)
    # remove stray <| prefixes
    raw = raw.replace("<|", "")
    return raw


def rand_between(a: datetime, b: datetime) -> datetime:
    sec = random.randrange(int((b - a).total_seconds()))
    return a + timedelta(seconds=sec)


def jitter(ts: datetime) -> datetime:
    return ts + timedelta(minutes=random.randint(JITTER_MIN_MINUTES, JITTER_MAX_MINUTES))


def build_chat_prompt(messages: list, tokenizer: AutoTokenizer) -> str:
    use_chat = hasattr(tokenizer, "chat_template") and tokenizer.chat_template is not None
    if use_chat:
        return tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    prompt = ""
    for m in messages:
        prompt += f"<s>{m['role']}\n{m['content']}</s>\n"
    prompt += "<s>assistant\n"
    return prompt


def generate_initial_email(topic: str) -> str:
    print("\n===== INITIAL EMAIL GENERATION =====")
    print(f"Loading model {MODEL_NAME} on {DEVICE}...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(MODEL_NAME, trust_remote_code=True).to(DEVICE)
    pad_token_id = tokenizer.eos_token_id
    print("Model loaded. Building prompt...")
    messages = [{
        "role": "user",
        "content": f"Write an email about {topic}. Discuss the {topic} subject. Focus on {topic}"
    }]
    prompt = build_chat_prompt(messages, tokenizer)
    print(f"Prompt:\n{prompt}\n")
    pipe = pipeline(
        "text-generation", model=model, tokenizer=tokenizer,
        device=model.device, do_sample=INIT_DO_SAMPLE,
        temperature=INIT_TEMPERATURE, top_p=INIT_TOP_P,
        pad_token_id=pad_token_id
    )
    out = pipe(prompt, max_new_tokens=INIT_MAX_NEW_TOKENS)[0]["generated_text"]
    raw = out[len(prompt):].strip()
    print(f"Generated initial email text (raw):\n{raw}\n")
    # ── apply stripping of any <|…|> artifacts before parsing
    cleaned = clean_text(raw)
    print(f"Cleaned initial email text:\n{cleaned}\n")
    return cleaned


def generate_followup_email(topic: str, prev_reply: str) -> str:
    print("\n----- FOLLOW-UP EMAIL GENERATION -----")
    print(f"Re-loading model {MODEL_NAME} on {DEVICE}...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(MODEL_NAME, trust_remote_code=True).to(DEVICE)
    pad_token_id = tokenizer.eos_token_id
    temp = random.uniform(FOLLOWUP_TEMPERATURE_MIN, FOLLOWUP_TEMPERATURE_MAX)
    top_p = random.uniform(FOLLOWUP_TOP_P_MIN, FOLLOWUP_TOP_P_MAX)
    max_tok = random.randint(FOLLOWUP_MAX_NEW_TOKENS_MIN, FOLLOWUP_MAX_NEW_TOKENS_MAX)
    print(f"Using temp={temp:.2f}, top_p={top_p:.2f}, max_new_tokens={max_tok}")
    messages = [
        {"role": "system", "content": f"You are a professional email writer, discussing on the {topic} subject."},
        {"role": "user", "content": f"I said:\n\"{prev_reply}\""},
        {"role": "user", "content": f"Write a follow-up to the previous email. Make sure to discuss about {topic}."}
    ]
    prompt = build_chat_prompt(messages, tokenizer)+"\nDear [Recipient],\n\n"
    print(f"Prompt for follow-up:\n{prompt}\n")
    pipe = pipeline(
        "text-generation", model=model, tokenizer=tokenizer,
        device=model.device, do_sample=FOLLOWUP_DO_SAMPLE,
        temperature=temp, top_p=top_p,
        pad_token_id=pad_token_id,
        repetition_penalty=FOLLOWUP_REPETITION_PENALTY
    )
    out = pipe(prompt, max_new_tokens=max_tok)[0]["generated_text"]
    raw = out[len(prompt):].strip()
    print(f"Generated follow-up text (raw):\n{raw}\n")
    # ── apply stripping of any <|…|> artifacts before parsing
    cleaned = clean_text(raw)
    print(f"Cleaned follow-up text:\n{cleaned}\n")
    return cleaned


def parse_subject_and_body(raw_text: str) -> tuple[str, str]:
    print("Parsing subject and body...")
    lines = raw_text.splitlines()
    if lines:
        m = re.match(r"^[Ss]ubject\s*:\s*(.+)$", lines[0])
        if m:
            subj = m.group(1).strip()
            body = "\n".join(lines[1:]).strip()
            print(f"Extracted subject: {subj}")
            return subj, body
    words = raw_text.split()
    subj = " ".join(words[:6]) + ("..." if len(words) > 6 else "")
    print(f"No explicit subject found, fallback subject: {subj}")
    return subj, raw_text


def build_conversation(topic: str, start: datetime, end: datetime) -> dict:
    print("\n================ BUILDING CONVERSATION ================")
    print(f"Topic: {topic}")
    n_emails = random.randint(CONV_MIN_LENGTH, CONV_MAX_LENGTH)
    print(f"Number of emails to generate in thread: {n_emails}")
    participants = ["Idan Morad"]
    extra_count = 2 if random.random() < EXTRA_PARTICIPANT_PROB else 1
    for _ in range(extra_count):
        participants.append(faker.name())
    print(f"Participants: {participants}")
    ts = rand_between(start, end)
    mails = []

    # ── First email
    raw0 = generate_initial_email(topic)
    subject, body0 = parse_subject_and_body(raw0)
    mails.append({
        "id": uuid.uuid4().hex,
        "subject": subject,
        "from": participants[0],
        "date": ts.strftime("%a, %d %b %Y %H:%M:%S +0000"),
        "content": body0,
        "order": 1
    })
    prev_reply = raw0
    print(f"First email added with subject: {subject}\n")

    # ── Follow-ups
    for idx in range(2, n_emails + 1):
        print(f"--- Generating follow-up email #{idx} ---")
        ts = jitter(ts)
        sender = participants[idx % len(participants)]
        raw = generate_followup_email(topic, prev_reply)
        _, body = parse_subject_and_body(raw)
        mails.append({
            "id": uuid.uuid4().hex,
            "subject": f"Re: {subject}",
            "from": sender,
            "date": ts.strftime("%a, %d %b %Y %H:%M:%S +0000"),
            "content": body,
            "order": idx
        })
        prev_reply = raw
        print(f"Follow-up email #{idx} added from {sender}\n")

    logging.info("Thread '%s' built with %d emails", subject, len(mails))
    return {"conversation_id": uuid.uuid4().hex, "emails": mails}


def main(argv=None):
    global MODEL_NAME, DEVICE

    # ─────────────────────────┐
    #     ARGUMENT PARSING
    # ─────────────────────────┘
    print("=== Starting script: Ministal Email Generator ===")
    ap = argparse.ArgumentParser(
        description="Generate mock email threads with Ministal-style content and JSON output"
    )
    ap.add_argument("-o", "--output-path",
                    default="server_client_local_files/mock_preprocessed_emails.json",
                    help="where to write the JSON")
    ap.add_argument("-m", "--model-name",
                    default=None,
                    help="Hugging Face model ID (overrides default)")
    ap.add_argument("-d", "--device", default=None,
                    help="device for model (e.g., cpu or cuda:0) (overrides default)")
    ap.add_argument("--seed", type=int,
                    help="random seed for reproducibility")
    ap.add_argument("--max-emails", type=int, default=1500,
                    help="total emails to generate across all threads")
    args = ap.parse_args(argv)
    print(f"Parsed arguments: {args}\n")

    # ─────────────────────────┐
    #   ASSIGN GLOBAL SETTINGS
    # ─────────────────────────┘
    if args.model_name:
        MODEL_NAME = args.model_name
        print(f"Global MODEL_NAME set to {MODEL_NAME}")
    else:
        print(f"No model-name arg, using default MODEL_NAME: {MODEL_NAME}")
    if args.device:
        DEVICE = args.device
        print(f"Global DEVICE set to {DEVICE}")
    else:
        print(f"No device arg, using default DEVICE: {DEVICE}")

    # ─────────────────────────┐
    #     LOGGING & SEEDING
    # ─────────────────────────┘
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")
    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)
    print("Logging initialized. Random seed applied (if provided).\n")

    # ─────────────────────────┐
    #     TIME WINDOW SETUP
    # ─────────────────────────┘
    start = datetime.now(timezone.utc) - timedelta(days=365 * TIMESTAMP_YEARS_BACK)
    end = datetime.now(timezone.utc)
    print(f"Time window: {start.isoformat()} to {end.isoformat()}\n")

    # ─────────────────────────┐
    #    CONVERSATION GENERATION LOOP
    # ─────────────────────────┘
    print("=== Entering conversation generation loop ===")
    conversations, total = [], 0
    while total < args.max_emails:
        print(f"### Progress: {total}/{args.max_emails} emails generated so far ###")
        conv = build_conversation(random.choice(TOPICS), start, end)
        conversations.append(conv)
        total += len(conv["emails"])
    print(f"=== Finished generation: total emails = {total} ===\n")

    # ─────────────────────────┐
    #     WRITE OUTPUT JSON
    # ─────────────────────────┘
    print(f"Writing output JSON to {args.output_path}...")
    out = Path(args.output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(conversations, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Output written. {len(conversations)} conversations saved to {out}\n")

    print("=== Script completed successfully ===")


if __name__ == "__main__":
    main()
