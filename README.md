# Stack to run GL Pipeline with Airflow

This repo is an ETL pipeline for Globalise in Airflow.

### How to run
1. Setup external network
   - Create a docker network named `globalise`:
     ```bash
     docker network create traefik-public
     ```
2. Setup API locally
  - Clone the [Globalise API](https://github.com/globalise-huygens/data-editor-pilot.git) repository, branch `develop` is recommended.
  - Go into the folder and run `docker compose up -d` to start the API. It runs on `http://localhost/api`.
  - Get a latest db dump of globalise and restore it in the local postgres database. `pg_restore -U globalise -d globalise </path/to/dump/file>`
3. Setup Airflow in shis stack
  - Clone this repo, branch `main` is recommended. 
  - Run `docker compose up -d`, the Airflow webserver will be available at `http://localhost:38080` with the default username and password `airflow`.
  - Change the permission of the output folder `/opt/airflow/output` to allow Airflow to write to it:
    ```bash
    # on host
    docker exec -it -u root <airflow-worker-container-name> /bin/bash
    # on container
    chown -R airflow:root /opt/airflow/output
    ```
4. Go to the Airflow webserver at `http://localhost:38080` and trigger the DAG.
### Notes
- The `communica` task will fail if the first step, create public network, is not done.
- The API fetcher will fail if the API stack is not running. 
- For errors and logs, check the airflow logs tab
