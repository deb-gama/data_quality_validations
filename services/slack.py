from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

def send_slack_message(token: str, channel: str, message: str):
    """
    Send a message to a Slack channel using a Slack bot.
    
    :param token: The bot token from Slack.
    :param channel: The Slack channel ID or name (e.g., '#general' or channel ID 'C12345678').
    :param message: The message to post.
    """
    client = WebClient(token=token)
    
    try:
        response = client.chat_postMessage(channel=channel, text=message)
        return response
    
    except SlackApiError as e:
        print(f"Error sending message: {e.response['error']}")

