import requests

LM_STUDIO_API_URL = "http://localhost:1234/v1/chat/completions"

def query_tinyllama(messages, temperature=0.7):
    payload = {
        "model": "TinyLlama",
        "messages": messages,
        "temperature": temperature,
        "stream": False
    }
    response = requests.post(LM_STUDIO_API_URL, json=payload)
    response.raise_for_status()
    return response.json()['choices'][0]['message']['content'].strip()

def main():
    print("Chatting locally with TinyLlama! Type 'exit' to quit.\n")

    while True:
        user_input = input("You: ")
        if user_input.lower() in ['exit', 'quit']:
            print("Exiting chat. Goodbye!")
            break

        messages = [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": user_input}
        ]

        try:
            print("TinyLlama is thinking...\n")
            reply = query_tinyllama(messages)
            print(f"TinyLlama: {reply}\n")
        except requests.exceptions.RequestException as e:
            print(f"Error connecting to LM Studio locally: {e}")
            break

if __name__ == "__main__":
    main()
