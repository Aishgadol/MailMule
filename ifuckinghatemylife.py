from huggingface_hub import login, whoami

# Paste your token here (the fine-grained one is fine for this method)
login(token="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", add_to_git_credential=False)

# Check whether login worked
user = whoami()
print("Logged in as:", user["name"])
