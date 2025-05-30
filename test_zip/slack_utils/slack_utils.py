from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

def slack_send(token, channel, text):   
    client = WebClient(token=token)
    response = client.chat_postMessage(
        channel=channel,
        text=text
    )
    return True