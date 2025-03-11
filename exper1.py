import tkinter as tk
from tkinter import messagebox, scrolledtext
import os
import pickle

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

class GmailApp:
    def __init__(self, root):
        self.root = root
        root.title("Desktop Gmail App")
        root.geometry("600x400")

        # Create a login button
        self.login_button = tk.Button(root, text="Login to Google", command=self.login)
        self.login_button.pack(pady=10)

        # Button to fetch emails (initially disabled)
        self.fetch_button = tk.Button(root, text="Fetch Emails", command=self.fetch_emails, state=tk.DISABLED)
        self.fetch_button.pack(pady=10)

        # A scrolled text widget to display emails
        self.text_area = scrolledtext.ScrolledText(root, width=80, height=20)
        self.text_area.pack(padx=10, pady=10)

        self.creds = None

    def login(self):
        """Authenticate the user using OAuth2 and enable email fetching."""
        try:
            # Check if token.pickle exists (for stored credentials)
            if os.path.exists('token.pickle'):
                with open('token.pickle', 'rb') as token:
                    self.creds = pickle.load(token)
            # If no valid credentials available, prompt the user to log in.
            if not self.creds or not self.creds.valid:
                if self.creds and self.creds.expired and self.creds.refresh_token:
                    self.creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                    self.creds = flow.run_local_server(port=0)
                # Save the credentials for the next run.
                with open('token.pickle', 'wb') as token:
                    pickle.dump(self.creds, token)
            self.fetch_button.config(state=tk.NORMAL)
            messagebox.showinfo("Success", "Logged in successfully!")
        except Exception as e:
            messagebox.showerror("Error during login", str(e))

    def fetch_emails(self):
        """Fetch and display the latest emails using the Gmail API."""
        try:
            service = build('gmail', 'v1', credentials=self.creds)
            # Retrieve a list of messages (maxResults can be adjusted)
            results = service.users().messages().list(userId='me', maxResults=10).execute()
            messages = results.get('messages', [])
            self.text_area.delete(1.0, tk.END)
            if not messages:
                self.text_area.insert(tk.END, "No messages found.\n")
            else:
                for msg in messages:
                    msg_data = service.users().messages().get(userId='me', id=msg['id']).execute()
                    snippet = msg_data.get('snippet', '')
                    self.text_area.insert(tk.END, f"{snippet}\n{'-'*40}\n")
        except Exception as e:
            messagebox.showerror("Error fetching emails", str(e))


if __name__ == '__main__':
    root = tk.Tk()
    app = GmailApp(root)
    root.mainloop()
