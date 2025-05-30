from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.variable import Variable
from airflow.decorators import task_group
from airflow.sensors.external_task import ExternalTaskSensor

from slack_task import slack_send_task


TRIGGERED_DAG = "update_likes2"
DEFAULT_TRIGGER_FILE = "run"

# TODO: fix the hardcoding of /tmp/ here,
# we're assuming that the fs_default connection points to /tmp/


@task.bash
def rm_file(path) -> str:
    return "rm /tmp/" + path


def start_of_prev_year(execution_date, **_):
    return execution_date.replace(year=execution_date.year-1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)


@task
def pull_run_id(**context):
    execution_date = context['execution_date']
    val = context['ti'].xcom_pull(key='run_id',
                                  dag_id=TRIGGERED_DAG,
                                  # For some reason doesn't work without this,
                                  # even though both DAG runs have the same execution_date
                                  include_prior_dates=True)
    print(val)
    print(context)


@task.bash
def create_finished_file():
    return 'touch "/tmp/finished_{{ ts_nodash }}.txt"'


with DAG(dag_id="trigger_likes_dag2", schedule="0 0 1 1 *", start_date=datetime(2018, 5, 3), catchup=False):
    trigger_file = Variable.get(
        'trigger_file', default_var=DEFAULT_TRIGGER_FILE)
    sensor = FileSensor(task_id="wait_for_file",
                        filepath=trigger_file,
                        poke_interval=1)
    trigger = TriggerDagRunOperator(
        task_id="test_trigger_dagrun",
        trigger_dag_id=TRIGGERED_DAG,
    )

    @task_group()
    def group1():
        external_task_sensor = ExternalTaskSensor(
            task_id="external_task_sensor",
            external_dag_id=TRIGGERED_DAG,
            execution_date_fn=start_of_prev_year,  # both DAGs run yearly
            poke_interval=3,
            check_existence=True,
        )
        rm_op = rm_file(trigger_file)
        pull_op = pull_run_id()
        create_file_op = create_finished_file()
        external_task_sensor >> pull_op >> rm_op >> create_file_op


    slack_op = slack_send_task()

    sensor >> trigger >> group1() >> slack_op
