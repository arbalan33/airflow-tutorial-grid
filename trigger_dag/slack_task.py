from datetime import datetime

from airflow.decorators import task
from airflow.models import Variable


from slack_utils import slack_utils

@task
def slack_send_task(**context):
    slack_token = Variable.get("slack_token")

    return slack_utils.slack_send(slack_token,
                                  "social",
                                  "Hello from your app! :tada:")
