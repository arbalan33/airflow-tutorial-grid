import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

config = {
    'update_table_users': {'schedule_interval': "@daily",
                           "start_date": datetime.datetime(2018, 11, 11),
                           "table_name": "table_users"},
    'update_table_posts': {'schedule_interval': "@monthly",
                           "start_date": datetime.datetime(2018, 11, 11),
                           "table_name": "tables_posts"},
    'update_likes2': {'schedule_interval': "0 0 1 1 *",
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
    # return CREATE_TABLE_TASK_ID


@task
def push_run_id(**context):
    context['ti'].xcom_push(key='run_id', value="{{ run_id }} ended")


def create_db_dag(dag_id, schedule, start_date, table_name):
    

    @dag(dag_id=dag_id, schedule=schedule, start_date=start_date, catchup=False)
    def db_dag():
        op1 = PythonOperator(task_id="print_the_context",
                             python_callable=db_task_info,
                             op_kwargs={"dag_id": dag_id,
                                        "table": table_name})
        bash_op = BashOperator(task_id="bash_whoami",
                               bash_command="whoami")
        branch = check_table_exist()
        create_table = EmptyOperator(task_id=CREATE_TABLE_TASK_ID)
        op2 = EmptyOperator(task_id=INSERT_ROW_TASK_ID, trigger_rule="none_failed_min_one_success")
        op3 = EmptyOperator(task_id="query_the_table")
        xcom_op = push_run_id()

        op1 >> bash_op >> branch >> op2 >> op3 >> xcom_op
        branch >> create_table >> op2

    return db_dag()


for dag_id, conf_dict in config.items():
    globals()[dag_id] = create_db_dag(dag_id,
                                   conf_dict["schedule_interval"],
                                   conf_dict["start_date"],
                                   conf_dict["table_name"])
