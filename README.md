This repo contains two DAG files testing many features of Airflow.

The first one `dags/jobs_dag.py` creates three similar DAGs that insert rows into a Postgres DB.

![First DAG](./img/db_dag.png)

The second one `test_zip/trigger_dag.py` creates a DAG that waits for the file `/tmp/run` to be created,
then triggers the first DAG, waits for it to finish, deletes the file and sends a Slack notification.

![Secnd DAG](./img/trigger_dag.png)


## Usage

Create a directory for Airflow (e.g. `~/airflow`)
and copy the file `docker-compose.yaml` there.

Modify the volume binding `- ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags`
to point to the `./dags` directory from this repo.
E.g. `- /home/user/Airflow_Tutorial_Grid/dags:/opt/airflow/dags`.

Build and run the containers:
```sh
docker compose up
```

Set up the three connections as shown (the filesystem connection is expected to map to `/tmp`): 
![Connections](./img/conn.png)

The second DAG is in a separate directory to test Airflow's packaging feature.
The contents of `./test_zip` are zipped to `dags/trigger_dag.zip`.

The second DAG waits for the file `/tmp/run`.
To create it, you can run a command in the `worker` container:
```sh
$ docker-compose exec airflow-worker bash
airflow@98fd6e672d41:/opt/airflow$ echo test > /tmp/run
```

The second DAG expects a Slack token in the `slack_token` Variable.
It can be set like this (it doesn't persist across runs):
```sh
docker exec -it VAULT_DOCKER_ID sh
vault login ZyrP7NtNw0hbLUqu7N3IlTdO
vault secrets enable -path=airflow -version=2 kv
vault kv put airflow/variables/slack_token value=YOUR_SLACK_TOKEN
```
