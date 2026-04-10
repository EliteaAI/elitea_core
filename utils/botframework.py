import requests
import datetime
from datetime import timedelta

class BotConnectorClient:
    def __init__(self, app_id, app_password, incoming_activity):
        self.app_id = app_id
        self.app_password = app_password
        self.token = None
        self.token_expiration = datetime.datetime.now(datetime.timezone.utc)
        self.incoming_activity = incoming_activity
        self.base_url = self.incoming_activity['serviceUrl']
        self.conversation_id = incoming_activity['conversation']['id']
        self.activity_id = incoming_activity.get('id', None)

    def request_bot_token(self):
        """Retrieve authentication token for the Bot Framework API."""
        token_url = 'https://login.microsoftonline.com/botframework.com/oauth2/v2.0/token'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.app_id,
            'client_secret': self.app_password,
            'scope': 'https://api.botframework.com/.default'
        }
        response = requests.post(token_url, headers=headers, data=data)
        if response.status_code == 200:
            token_data = response.json()
            self.token = token_data['access_token']
            expires_in = int(token_data['expires_in'])
            self.token_expiration = datetime.datetime.now(datetime.timezone.utc) + timedelta(seconds=expires_in)
        else:
            raise Exception(f"Error obtaining bot token: {response.status_code} {response.text}")

    def get_headers(self):
        """Get headers for API requests, refreshing token if necessary."""
        if self.token is None or datetime.datetime.now(datetime.timezone.utc) >= self.token_expiration:
            self.request_bot_token()
        return {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }

    def send_to_conversation(self, activity):
        """Send an activity to the conversation extracted from incoming_activity."""
        url = f'{self.base_url}/v3/conversations/{self.conversation_id}/activities'
        print(url)
        headers = self.get_headers()
        response = requests.post(url, headers=headers, json=activity)
        if response.status_code in (200, 201):
            return response.json()
        else:
            raise Exception(f"Error sending activity: {response.status_code} {response.text}")

    def send_message(self, text, **kwargs):
        """Send a text message to the conversation."""
        activity = {
            "type": "message",
            "text": text,
            "from": self.incoming_activity["recipient"],
            "recipient": self.incoming_activity["from"],
            "replyToId": self.activity_id
        }
        activity.update(kwargs)
        return self.send_to_conversation(activity)

    def reply_to_activity(self, activity):
        """Reply to the specific activity extracted from incoming_activity."""
        if not self.activity_id:
            raise Exception("No activity_id found in incoming_activity.")
        url = f'{self.base_url}/v3/conversations/{self.conversation_id}/activities/{self.activity_id}'
        headers = self.get_headers()
        print(headers)
        print(url)
        activity["from"] = self.incoming_activity["recipient"]
        activity["recipient"] = self.incoming_activity["from"]
        response = requests.post(url, headers=headers, json=activity)
        if response.status_code in (200, 201):
            return response.json()
        else:
            raise Exception(f"Error replying to activity: {response.status_code} {response.text}")

    def send_typing_event(self):
        """Send a typing event to indicate that the bot is typing."""
        activity = {
            "type": "typing",
            "from": self.incoming_activity["recipient"],
            "recipient": self.incoming_activity["from"]
        }
        return self.send_to_conversation(activity)

    def get_activity_members(self):
        """Get members of the specific activity extracted from incoming_activity."""
        if not self.activity_id:
            raise Exception("No activity_id found in incoming_activity.")
        url = f'{self.base_url}/v3/conversations/{self.conversation_id}/activities/{self.activity_id}/members'
        headers = self.get_headers()
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error retrieving activity members: {response.status_code} {response.text}")

    def upload_attachment(self, attachment):
        """Upload an attachment to the conversation."""
        url = f'{self.base_url}/v3/conversations/{self.conversation_id}/attachments'
        headers = self.get_headers()
        response = requests.post(url, headers=headers, json=attachment)
        if response.status_code in (200, 201):
            return response.json()
        else:
            raise Exception(f"Error uploading attachment: {response.status_code} {response.text}")