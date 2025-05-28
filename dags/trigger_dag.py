from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.variable import Variable


# TODO: fix the hardcoding of /tmp/ here,
# we're assuming that the fs_default connection points to /tmp/
@task.bash
def rm_file(path) -> str:
    return "rm /tmp/" + path


TRIGGERED_DAG = "update_table_likes"
DEFAULT_TRIGGER_FILE = "temporary_file_for_testing"

with DAG(dag_id="trigger_dag"):
    trigger_file = Variable.get('trigger_file', default_var=DEFAULT_TRIGGER_FILE)
    sensor = FileSensor(task_id="wait_for_file",
                        filepath=trigger_file,
                        poke_interval=1)
    trigger = TriggerDagRunOperator(
        task_id="test_trigger_dagrun",
        trigger_dag_id=TRIGGERED_DAG,
    )
    op = rm_file(trigger_file)

    sensor >> trigger >> op
