import json
import os

input_file = 'emails.json'
output_file_1 = 'emails1.json'
output_file_2 = 'emails2.json'

# Load all emails
with open(input_file, 'r') as f:
    emails = json.load(f)

# Estimate size of each email individually
email_sizes = [len(json.dumps(email)) for email in emails]
total_size = sum(email_sizes)
half_size = total_size / 2

emails1 = []
emails2 = []
accumulated_size = 0

# Smart split by size
for email, size in zip(emails, email_sizes):
    if accumulated_size + size <= half_size:
        emails1.append(email)
        accumulated_size += size
    else:
        emails2.append(email)

# Save both files
with open(output_file_1, 'w') as f1:
    json.dump(emails1, f1, indent=4)

with open(output_file_2, 'w') as f2:
    json.dump(emails2, f2, indent=4)

# Output stats
def size_in_mb(path):
    return os.path.getsize(path) / (1024 * 1024)

print(f"Split {len(emails)} emails based on size.")
print(f"{output_file_1}: {len(emails1)} emails, {size_in_mb(output_file_1):.2f} MB")
print(f"{output_file_2}: {len(emails2)} emails, {size_in_mb(output_file_2):.2f} MB")
