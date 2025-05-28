



"""
Script to generate mock preprocessed emails in the same format as
`preprocessed_emails.json`, using an LLM to create
short (6–9 sentences) context-rich email threads across our topics.
Ensures consistent participants, dates, and context.
"""



import argparse
import json
import logging
import os
import random
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from faker import Faker
from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline


def random_date(start: datetime, end: datetime) -> datetime:
    """Return a random datetime between `start` and `end` (inclusive)."""
    span = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randrange(span))


def generate_thread(
    topic: str,
    num_emails: int,
    participants_info: List[Dict[str, str]],
    generator,
    max_retries: int = 2,
    retry_delay: float = 1.0,
) -> List[Dict[str, Any]]:
    """
    Use the LLM to generate a JSON array of `num_emails` messages
    among the fixed `participants_info` about `topic`.
    """
    # build a pre-prompt listing our participants exactly once
    ppl_list = "\n".join(
        f"- {p['name']} <{p['email']}>" for p in participants_info
    )
    prompt = (
        f"You are generating a {len(participants_info)}-person email thread "
        f"of {num_emails} messages about \"{topic}\".\n\n"
        f"Participants (you must use these names & emails exactly):\n"
        f"{ppl_list}\n\n"
        "- Keep each message coherent, referencing earlier emails if relevant.\n"
        "- Subject: realistic, ≤8 words.\n"
        "- Body: 6–9 sentences, rich on topic details.\n\n"
        "Output ONLY valid JSON: a single top-level list of objects, each with\n"
        "  \"sender\",  \"recipients\" (list),  \"subject\",  \"body\"."
    )

    for attempt in range(1, max_retries + 1):
        try:
            out = generator(prompt, max_new_tokens=512, temperature=0.7)
            text = out[0].get("generated_text", "")
            return json.loads(text)
        except json.JSONDecodeError:
            logging.warning("JSON parse failed (attempt %d), extracting block…", attempt)
            m = re.search(r"(\[.*\])", text, flags=re.DOTALL)
            if m:
                try:
                    return json.loads(m.group(1))
                except json.JSONDecodeError:
                    logging.warning("Retry %d: still invalid JSON.", attempt)
        except Exception as e:
            logging.error("Generation error on attempt %d: %s", attempt, e)

        if attempt < max_retries:
            time.sleep(retry_delay)

    raise RuntimeError(f"Failed to generate thread for '{topic}' after {max_retries} attempts")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate mock preprocessed emails.")
    parser.add_argument(
        "-o", "--output-path",
        default="server_client_local_files/mock_preprocessed_emails.json",
        help="Where to write the output JSON"
    )
    parser.add_argument(
        "-m", "--model-name",
        default="mistralai/Mistral-3B-Instruct-v0.1",
        help="HuggingFace model identifier"
    )
    parser.add_argument(
        "-d", "--device",
        default="cpu",
        help="Device to run on ('cpu' or GPU ID)"
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Random seed for reproducibility"
    )
    parser.add_argument(
        "--max-emails", type=int, default=None,
        help="Stop after generating approx this many emails"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)
        logging.info("Random seed set to %d", args.seed)

    logging.info("Loading model %s on %s", args.model_name, args.device)
    tokenizer = AutoTokenizer.from_pretrained(args.model_name, trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(args.model_name, trust_remote_code=True).to(args.device)
    generator = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer,
        device=0 if args.device != "cpu" else -1
    )

    faker = Faker()
    subjects = {
        "Motorsports Results": 3,
        "Food & Dining": 3,
        "Job Applications & Job Searching": 2,
        "Medical / Appointments": 2,
        "Gaming Sessions": 3,
        "Stock Market / Stocks": 2,
        "Casual Catch-Ups": 3,
    }

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=4 * 365)
    conversations: List[Dict[str, Any]] = []
    total_emails = 0

    for topic, n_threads in subjects.items():
        for _ in range(n_threads):
            if args.max_emails and total_emails >= args.max_emails:
                break

            # 1) pick participant count & create them once per thread
            pcount = random.choice([2, 3])
            participants_info = [
                {"name": faker.name(), "email": faker.email()}
                for _ in range(pcount)
            ]

            # 2) decide thread length
            length = random.randint(5, 8) if pcount == 3 else random.randint(4, 7)

            # 3) generate raw messages
            try:
                raw_msgs = generate_thread(topic, length, participants_info, generator)
            except RuntimeError as e:
                logging.error(e)
                continue

            # 4) assign strictly increasing dates
            dates = sorted(random_date(start, now) for _ in raw_msgs)

            # 5) build JSON
            conv_id = uuid.uuid4().hex
            conv = {"conversation_id": conv_id, "emails": []}

            for idx, (msg, dt) in enumerate(zip(raw_msgs, dates), start=1):
                if args.max_emails and total_emails >= args.max_emails:
                    break

                em = {
                    "id": uuid.uuid4().hex,
                    "subject": msg.get("subject", "").strip(),
                    "from": msg.get("sender", "").strip(),
                    "date": dt.strftime("%a, %d %b %Y %H:%M:%S +0000"),
                    "content": msg.get("body", "").strip(),
                    "order": idx,
                }
                conv["emails"].append(em)
                total_emails += 1

            conversations.append(conv)

        else:
            continue
        break  # out if max reached

    logging.info("Generated %d threads with %d emails total.", len(conversations), total_emails)

    os.makedirs(os.path.dirname(args.output_path), exist_ok=True)
    with open(args.output_path, "w", encoding="utf-8") as f:
        json.dump(conversations, f, ensure_ascii=False, indent=2)

    logging.info("Written mock data to %s", args.output_path)


if __name__ == "__main__":
    main()
