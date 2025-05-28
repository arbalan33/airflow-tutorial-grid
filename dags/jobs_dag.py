import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

config = {
    'dag_id_1': {'schedule_interval': "@daily", "start_date": datetime.datetime(2018, 11, 11)},
    'dag_id_2': {'schedule_interval': "@monthly", "start_date": datetime.datetime(2018, 11, 11)},
    'dag_id_3': {'schedule_interval': "0 0 2 2 *", "start_date": datetime.datetime(2018, 5, 2)}
}


for dag_id, conf_dict in config.items():
    with DAG(
            dag_id=dag_id,
            start_date=conf_dict["start_date"],
            schedule=conf_dict["schedule_interval"],
    ):
        EmptyOperator(task_id="task")
