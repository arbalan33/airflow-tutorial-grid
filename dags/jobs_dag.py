import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

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

INSERT_ROW_TASK_ID = "insert_new_row"
CREATE_TABLE_TASK_ID = 'create_table'

def db_task_info(dag_id, table):
    print(f"{dag_id} start processing table: {table}")


@task.branch(task_id="branch_task")
def check_table_exist():
    """ method to check that table exists """
    if True:
	    return INSERT_ROW_TASK_ID
    return CREATE_TABLE_TASK_ID


def create_db_dag(dag_id, schedule, start_date, table_name):
    @dag(dag_id=dag_id, schedule=schedule, start_date=start_date, catchup=False)
    def db_dag():
        op1 = PythonOperator(task_id="print_the_context",
                             python_callable=db_task_info,
                             op_kwargs={"dag_id": dag_id,
                                        "table": table_name})
        branch = check_table_exist()
        create_table = EmptyOperator(task_id=CREATE_TABLE_TASK_ID)
        op2 = EmptyOperator(task_id=INSERT_ROW_TASK_ID)
        op3 = EmptyOperator(task_id="query_the_table")
        op1 >> branch >> op2 >> op3
        branch >> create_table >> op2

    return db_dag()


for dag_id, conf_dict in config.items():
    globals()[dag_id] = create_db_dag(dag_id,
                                   conf_dict["schedule_interval"],
                                   conf_dict["start_date"],
                                   conf_dict["table_name"])
