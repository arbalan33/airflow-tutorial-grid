import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# PostgresOperator operator is outdated??
# https://stackoverflow.com/questions/79526704/trying-to-use-mysqloperator-and-postgresoperator-of-airflow
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from custom_operators.postgres_operator import PostgreSQLCountRows


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

PARAMS_TASK_ID = "create_params_task"
CREATE_TABLE_TASK_ID = 'create_table'
DB_CONN_ID = "postgres_default"  # make sure this connection exists
SCHEMA_NAME = 'airflow'


def sql_create_table(table):
    # SQL to create table in specified schema
    return f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{table} (
            custom_id INTEGER NOT NULL,
            user_name VARCHAR(50) NOT NULL,
            timestamp TIMESTAMP NOT NULL
        );"""


def db_task_info(dag_id, table):
    print(f"{dag_id} start processing table: {table}")


@task.branch(task_id="branch_task")
def check_table_exist(table_name: str) -> bool:
    hook = PostgresHook()

    sql = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
        );
    """
    result = hook.get_first(sql, parameters=(SCHEMA_NAME, table_name))
    print(f"Table '{SCHEMA_NAME}.{table_name}' exists: {result[0]}")
    if result and result[0]:
        return PARAMS_TASK_ID
    else:
        return CREATE_TABLE_TASK_ID


@task
def push_run_id(**context):
    run_id = context["run_id"]  # already rendered
    context['ti'].xcom_push(key='run_id', value=f"{run_id} ended")


@task
def generate_insert_params(ti=None):
    import random
    custom_id_value = random.randint(1, 1000000)
    user_name_value = ti.xcom_pull(task_ids='bash_whoami', key='return_value')
    timestamp_value = datetime.datetime.now()
    return {
        "custom_id": custom_id_value,
        "user_name": user_name_value,
        "timestamp": timestamp_value,
    }


def create_db_dag(dag_id, schedule, start_date, table_name):
    @dag(dag_id=dag_id, schedule=schedule, start_date=start_date, catchup=False)
    def db_dag():
        op1 = PythonOperator(task_id="print_the_context",
                             python_callable=db_task_info,
                             op_kwargs={"dag_id": dag_id,
                                        "table": table_name})
        bash_op = BashOperator(task_id="bash_whoami",
                               bash_command="whoami")
        branch = check_table_exist(table_name)

        create_table = SQLExecuteQueryOperator(
            task_id="create_table",
            conn_id=DB_CONN_ID,
            sql=sql_create_table(table_name)
        )

        params_task = generate_insert_params.override(
            task_id=PARAMS_TASK_ID, trigger_rule="none_failed")()

        insert_task = SQLExecuteQueryOperator(
            task_id="inserts_task",
            conn_id=DB_CONN_ID,
            sql=f"""INSERT INTO {table_name} (custom_id, user_name, timestamp)
                    VALUES (%(custom_id)s, %(user_name)s, %(timestamp)s);""",
            parameters=params_task,  # This automatically passes the dict as parameters
        )

        query_op = PostgreSQLCountRows(
            task_id="count_rows",
            table_name=table_name,
            conn_id=DB_CONN_ID
        )

        xcom_op = push_run_id()

        op1 >> bash_op >> branch >> params_task >> insert_task >> query_op >> xcom_op
        branch >> create_table >> params_task

    return db_dag()


for dag_id, conf_dict in config.items():
    globals()[dag_id] = create_db_dag(dag_id,
                                      conf_dict["schedule_interval"],
                                      conf_dict["start_date"],
                                      conf_dict["table_name"])
