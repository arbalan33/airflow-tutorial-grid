import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

config = {
    'update_table_users': {'schedule_interval': "@daily",
                 "start_date": datetime.datetime(2018, 11, 11),
                 "table_name": "table_users"},
    'update_table_posts': {'schedule_interval': "@monthly",
                 "start_date": datetime.datetime(2018, 11, 11),
                 "table_name": "tables_posts"},
    'update_table_likes': {'schedule_interval': "0 0 2 3 *",
                 "start_date": datetime.datetime(2018, 5, 3),
                 "table_name": "table_likes"}
}


def db_task_info(dag_id, database):
    print(f"{dag_id} start processing tables in database: {database}")


for dag_id, conf_dict in config.items():
    with DAG(
            dag_id=dag_id,
            start_date=conf_dict["start_date"],
            schedule=conf_dict["schedule_interval"],
    ):
        op1 = PythonOperator(task_id="print_the_context",
                             python_callable=db_task_info,
                             op_kwargs={"dag_id": dag_id,
                                        "database": "test"})
        op2 = EmptyOperator(task_id="insert_new_row")
        op3 = EmptyOperator(task_id="query_the_table")
        op1 >> op2 >> op3
