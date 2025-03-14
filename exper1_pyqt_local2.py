import sys
import os
import pickle

from PyQt5 import QtWidgets, QtGui, QtCore

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Define the scope for read-only access to Gmail
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

class GmailApp(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.creds = None
        self.initUI()

    def initUI(self):
        self.setWindowTitle("Desktop Gmail Client")
        self.resize(700, 500)

        # Central widget and layout
        centralWidget = QtWidgets.QWidget(self)
        self.setCentralWidget(centralWidget)
        layout = QtWidgets.QVBoxLayout(centralWidget)

        # Title label
        titleLabel = QtWidgets.QLabel("Gmail Desktop Client", self)
        titleLabel.setAlignment(QtCore.Qt.AlignCenter)
        titleFont = QtGui.QFont("Helvetica", 16, QtGui.QFont.Bold)
        titleLabel.setFont(titleFont)
        layout.addWidget(titleLabel)

        # Horizontal layout for buttons
        buttonLayout = QtWidgets.QHBoxLayout()

        # Login Button (always visible)
        self.loginButton = QtWidgets.QPushButton("Login to Google")
        self.loginButton.setStyleSheet("background-color: #4caf50; color: white; padding: 5px 10px;")
        self.loginButton.clicked.connect(self.login)
        buttonLayout.addWidget(self.loginButton)

        # Fetch Emails Button (hidden until login)
        self.fetchButton = QtWidgets.QPushButton("Fetch Emails")
        self.fetchButton.setStyleSheet("background-color: #2196f3; color: white; padding: 5px 10px;")
        self.fetchButton.hide()  # Hidden by default
        self.fetchButton.clicked.connect(self.fetch_emails)
        buttonLayout.addWidget(self.fetchButton)

        # Disconnect Button (hidden until login)
        self.disconnectButton = QtWidgets.QPushButton("Disconnect")
        self.disconnectButton.setStyleSheet("background-color: #f44336; color: white; padding: 5px 10px;")
        self.disconnectButton.hide()  # Hidden by default
        self.disconnectButton.clicked.connect(self.disconnect)
        buttonLayout.addWidget(self.disconnectButton)

        layout.addLayout(buttonLayout)

        # Text area to display emails
        self.textEdit = QtWidgets.QTextEdit(self)
        self.textEdit.setReadOnly(True)
        layout.addWidget(self.textEdit)

    def login(self):
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
                    flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                    self.creds = flow.run_local_server(port=0)
                # Save the credentials for future use
                with open('token.pickle', 'wb') as token:
                    pickle.dump(self.creds, token)
            # Make fetch and disconnect buttons visible upon successful login
            self.fetchButton.show()
            self.disconnectButton.show()
            QtWidgets.QMessageBox.information(self, "Success", "Logged in successfully!")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error during login", str(e))

    def fetch_emails(self):
        try:
            service = build('gmail', 'v1', credentials=self.creds)
            results = service.users().messages().list(userId='me', maxResults=10).execute()
            messages = results.get('messages', [])
            self.textEdit.clear()
            if not messages:
                self.textEdit.append("No messages found.")
            else:
                for msg in messages:
                    msg_data = service.users().messages().get(
                        userId='me', id=msg['id'], format='full'
                    ).execute()
                    snippet = msg_data.get('snippet', '')
                    headers = msg_data['payload'].get('headers', [])

                    # Extract Subject, From, and Date from headers
                    subject = "N/A"
                    sender = "N/A"
                    date = "N/A"
                    for header in headers:
                        if header['name'].lower() == 'subject':
                            subject = header['value']
                        elif header['name'].lower() == 'from':
                            sender = header['value']
                        elif header['name'].lower() == 'date':
                            date = header['value']

                    # Build an HTML block for each email with colors
                    email_html = f"""
                    <p>
                      <span style="color: blue; font-weight: bold;">Subject:</span> {subject}<br>
                      <span style="color: green;">Date:</span> {date}<br>
                      <span style="color: purple;">From:</span> {sender}<br>
                      <span style="color: black;">Snippet:</span> {snippet}
                    </p>
                    <hr>
                    """
                    self.textEdit.append(email_html)
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error fetching emails", str(e))

    def disconnect(self):
        try:
            if os.path.exists('token.pickle'):
                os.remove('token.pickle')
            self.creds = None
            # Hide fetch and disconnect buttons after disconnecting
            self.fetchButton.hide()
            self.disconnectButton.hide()
            self.textEdit.clear()
            QtWidgets.QMessageBox.information(self, "Disconnected", "You have been disconnected. Please log in again.")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error during disconnect", str(e))

if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    window = GmailApp()
    window.show()
    sys.exit(app.exec_())
