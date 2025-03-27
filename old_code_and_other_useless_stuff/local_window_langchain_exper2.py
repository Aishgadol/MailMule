import sys
import os
import pickle
import email.utils  # For parsing email date strings
import openai     # OpenAI API for natural language processing

from PyQt5 import QtWidgets, QtGui, QtCore

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Set your OpenAI API key (alternatively, set the OPENAI_API_KEY environment variable)
openai.api_key = os.getenv("OPENAI_API_KEY", "your_openai_api_key_here")

# Define the scope for read-only access to Gmail
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

class GmailApp(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.creds = None
        self.initUI()

    def initUI(self):
        # Set window title and size
        self.setWindowTitle("Desktop Gmail Client")
        self.resize(800, 600)

        # Create central widget and vertical layout
        centralWidget = QtWidgets.QWidget(self)
        self.setCentralWidget(centralWidget)
        layout = QtWidgets.QVBoxLayout(centralWidget)

        # Title label at the top
        titleLabel = QtWidgets.QLabel("Gmail Desktop Client", self)
        titleLabel.setAlignment(QtCore.Qt.AlignCenter)
        titleFont = QtGui.QFont("Helvetica", 16, QtGui.QFont.Bold)
        titleLabel.setFont(titleFont)
        layout.addWidget(titleLabel)

        # Top button layout: Login (always visible) and Disconnect (hidden until login)
        topButtonLayout = QtWidgets.QHBoxLayout()
        self.loginButton = QtWidgets.QPushButton("Login to Google")
        self.loginButton.setStyleSheet("background-color: #4caf50; color: white; padding: 5px 10px;")
        self.loginButton.clicked.connect(self.login)
        topButtonLayout.addWidget(self.loginButton)

        self.disconnectButton = QtWidgets.QPushButton("Disconnect")
        self.disconnectButton.setStyleSheet("background-color: #f44336; color: white; padding: 5px 10px;")
        self.disconnectButton.hide()  # Hidden until login
        self.disconnectButton.clicked.connect(self.disconnect)
        topButtonLayout.addWidget(self.disconnectButton)

        layout.addLayout(topButtonLayout)

        # Chat area: prompt input field and submit button (hidden until login)
        chatLayout = QtWidgets.QHBoxLayout()
        self.promptInput = QtWidgets.QLineEdit()
        self.promptInput.setPlaceholderText("Enter your query related to your emails...")
        self.promptInput.hide()  # Hidden until login
        chatLayout.addWidget(self.promptInput)

        self.submitButton = QtWidgets.QPushButton("Submit")
        self.submitButton.hide()  # Hidden until login
        self.submitButton.clicked.connect(self.process_prompt)
        chatLayout.addWidget(self.submitButton)
        layout.addLayout(chatLayout)

        # Email display area: a tree view that shows email summary and can expand to show full snippet
        self.emailTree = QtWidgets.QTreeWidget()
        self.emailTree.setColumnCount(4)
        self.emailTree.setHeaderLabels(["Subject", "From/To", "Date", "Time"])
        self.emailTree.hide()  # Hidden until login
        layout.addWidget(self.emailTree)

    def login(self):
        """Handles user authentication via Google OAuth."""
        try:
            # Check for existing credentials in token.pickle
            if os.path.exists('token.pickle'):
                with open('token.pickle', 'rb') as token:
                    self.creds = pickle.load(token)
            # If credentials are missing or invalid, start the OAuth flow
            if not self.creds or not self.creds.valid:
                if self.creds and self.creds.expired and self.creds.refresh_token:
                    self.creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file('../credentials.json', SCOPES)
                    self.creds = flow.run_local_server(port=0)
                # Save credentials for future use
                with open('token.pickle', 'wb') as token:
                    pickle.dump(self.creds, token)
            # After login, show chat controls, email display, and disconnect button
            self.disconnectButton.show()
            self.promptInput.show()
            self.submitButton.show()
            self.emailTree.show()
            QtWidgets.QMessageBox.information(self, "Success", "Logged in successfully!")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error during login", str(e))

    def process_prompt(self):
        """
        Processes the user's natural language prompt.
        The prompt is passed to the OpenAI API to extract search criteria.
        """
        prompt_text = self.promptInput.text().strip()
        if not prompt_text:
            return
        # Call OpenAI API to interpret the prompt
        criteria = self.parse_prompt(prompt_text)
        query = criteria.get("query", prompt_text)  # Fallback to the raw prompt
        self.search_emails(query)

    def parse_prompt(self, prompt):
        """
        Calls the OpenAI GPT model to extract structured search criteria from the natural language prompt.
        For example, it could return a JSON-like structure with keywords and optional filters.
        """
        try:
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are an assistant that extracts search keywords from a natural language prompt about emails. Return a JSON with a key 'query' that contains the keywords for searching Gmail."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=50
            )
            # The model's response should be in JSON format. Here, we try to parse it.
            result_text = response.choices[0].message.content.strip()
            # For simplicity, we assume the response is something like: {"query": "dark chocolates"}
            import json
            criteria = json.loads(result_text)
            return criteria
        except Exception as e:
            # In case of error, fallback to using the raw prompt as the query
            return {"query": prompt}

    def search_emails(self, query):
        """
        Uses the Gmail API to search for emails matching the query.
        The results are then displayed in the tree view.
        Emails are sorted such that the newest mail appears at the bottom.
        """
        try:
            service = build('gmail', 'v1', credentials=self.creds)
            # The 'q' parameter supports Gmail search queries.
            results = service.users().messages().list(userId='me', q=query, maxResults=10).execute()
            messages = results.get('messages', [])
            # Reverse the list to have the newest email at the bottom.
            messages = list(reversed(messages))
            self.emailTree.clear()
            if not messages:
                QtWidgets.QMessageBox.information(self, "No Results", "No emails matched your query.")
            else:
                for msg in messages:
                    msg_data = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
                    snippet = msg_data.get('snippet', '')
                    headers = msg_data['payload'].get('headers', [])
                    subject = "N/A"
                    sender = "N/A"
                    date_str = "N/A"
                    time_str = "N/A"
                    for header in headers:
                        if header['name'].lower() == 'subject':
                            subject = header['value']
                        elif header['name'].lower() == 'from':
                            sender = header['value']
                        elif header['name'].lower() == 'date':
                            # Parse the date header into date and time
                            try:
                                parsed_date = email.utils.parsedate_to_datetime(header['value'])
                                date_str = parsed_date.strftime("%Y-%m-%d")
                                time_str = parsed_date.strftime("%H:%M:%S")
                            except Exception:
                                date_str = header['value']
                    # Create a top-level tree item with summary info
                    item = QtWidgets.QTreeWidgetItem([subject, sender, date_str, time_str])
                    # Add a child item that holds the full snippet (or full content if available)
                    child = QtWidgets.QTreeWidgetItem(["", "", "", snippet])
                    item.addChild(child)
                    self.emailTree.addTopLevelItem(item)
                # Expand all tree items so details are visible upon expansion
                self.emailTree.expandAll()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error fetching emails", str(e))

    def disconnect(self):
        """Disconnects the user by clearing stored credentials and hiding the chat and email display."""
        try:
            if os.path.exists('token.pickle'):
                os.remove('token.pickle')
            self.creds = None
            # Hide chat controls and email tree after disconnecting
            self.promptInput.hide()
            self.submitButton.hide()
            self.emailTree.hide()
            self.disconnectButton.hide()
            QtWidgets.QMessageBox.information(self, "Disconnected", "You have been disconnected. Please log in again.")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error during disconnect", str(e))

if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    window = GmailApp()
    window.show()
    sys.exit(app.exec_())
