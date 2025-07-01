import random
from transformers import pipeline, AutoTokenizer, AutoModelForCausalLM

topic= random.choice(["Motorsports & Vehicles","Health & Fitness", "Online Gaming", "Travel & Lifestyle", "Food & Flavors","Parties & Celebrations","Nuclear Physics","Artificial Intelligence","Space Exploration","Environmental Conservation","Fashion & Trends","Music & Entertainment","Education & Learning","Finance & Investments","Technology & Gadgets"])
print(f"topic is: {topic}\n"+"-----"*15+"\n")
# === 1. Define messages ===
messages = [
    {"role": "user", "content": f"Write an email about {topic}."}
]
all_text=[]
# === 2. Try loading tokenizer with chat template support ===
tokenizer = AutoTokenizer.from_pretrained("ministral/Ministral-3b-instruct")
use_chat = hasattr(tokenizer, "chat_template") and tokenizer.chat_template is not None

# === 3. Create a single string prompt ===
if use_chat:
    # Applies the MLX-style built-in template
    prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
else:
    # Fallback: manual template via TensorBlock/GGUF format
    # Use system tag only if present in messages
    prompt = ""
    for m in messages:
        prompt += f"<s>{m['role']}\n{m['content']}</s>\n"
    prompt += "<s>assistant\n"

# === 4. Load model & pipeline ===
model = AutoModelForCausalLM.from_pretrained("ministral/Ministral-3b-instruct")
pipe = pipeline("text-generation", model=model, tokenizer=tokenizer)

# === 5. Generate response ===
outputs = pipe(
    prompt,
    max_new_tokens=128,
    do_sample=True,
    temperature=0.5,
    top_p=0.85,
    pad_token_id=tokenizer.eos_token_id  # avoid warnings
)

# === 6. Extract and print assistant reply ===
text = outputs[0]["generated_text"]

# Trim the prompt part to isolate assistant-only text
assistant_reply = text[len(prompt):].strip()
print(assistant_reply)
all_text.append(assistant_reply)
for i in range(3):
    # === 1. Define messages ===
    bef, sep, aft= assistant_reply.partition("Subject:")

    msg_content=(f"Topic is {topic}.\n"
                 f"Write a follow-up to the previous email.\n Make sure to discuss about {topic}.")
    print(f"\n"+"-----"*15+"\nprompt is:\n"+msg_content)

    messages = [
        {"role":"system", "content": f"You are a professional email writer, discussing on the {topic} subject."},
        {"role":"user", "content": f"I said: \n\"{assistant_reply}\"\n"},
        {"role": "user", "content": msg_content}
    ]

    # === 2. Try loading tokenizer with chat template support ===
    tokenizer = AutoTokenizer.from_pretrained("ministral/Ministral-3b-instruct")
    use_chat = hasattr(tokenizer, "chat_template") and tokenizer.chat_template is not None

    # === 3. Create a single string prompt ===
    if use_chat:
        # Applies the MLX-style built-in template
        prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    else:
        # Fallback: manual template via TensorBlock/GGUF format
        # Use system tag only if present in messages
        prompt = ""
        for m in messages:
            prompt += f"<s>{m['role']}\n{m['content']}</s>\n"
        prompt += "<s>assistant\n"

    # === 4. Load model & pipeline ===
    model = AutoModelForCausalLM.from_pretrained("ministral/Ministral-3b-instruct")
    pipe = pipeline("text-generation", model=model, tokenizer=tokenizer)
    temp,topp= random.uniform(0.60,0.95),random.uniform(0.60,0.95)
    max_tokens= random.randint(170,215)
    print("\n"+"-----"*15+f"\nParameters: temperature={temp}, top_p={topp}, max_new_tokens={max_tokens}\n"+"-----"*15)
    # === 5. Generate response ===
    outputs = pipe(
        prompt,
        max_new_tokens=max_tokens,
        do_sample=True,
        temperature=temp,
        top_p=topp,
        pad_token_id=tokenizer.eos_token_id,  # avoid warnings
        repetition_penalty=1.4,
    )

    # === 6. Extract and print assistant reply ===
    text = outputs[0]["generated_text"]

    # Trim the prompt part to isolate assistant-only text
    assistant_reply = text[len(prompt):].strip()
    print("\n"+"----"*15+"\n"+assistant_reply)
    all_text.append(assistant_reply)

for i,texty in enumerate(all_text):
    print(f"{"----"*15}\n"
        f"email with index {i}:\n\" - {texty} - \"\n")