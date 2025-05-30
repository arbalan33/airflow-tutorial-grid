from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.http.hooks.http import HttpHook


from slack_utils import slack_utils

@task
def slack_send_task(**context):
    hook = HttpHook(http_conn_id='slack_conn')
    extra = hook.get_connection(hook.http_conn_id).extra_dejson
    slack_token = extra['token']

    return slack_utils.slack_send(slack_token,
                                  "social",
                                  "Hello from your app! :tada:")
