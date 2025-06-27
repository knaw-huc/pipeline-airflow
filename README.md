# Stack to run SD Backup with Airflow

**KEEP THIS REPO PRIVATE, IT CONTAINS CREDENTIALS**
This repository contains the necessary files to run the SD Backup with Airflow on portainer and configure it to backup databases.

The `docker-compose.yml` file contains the services to be run on portainer. 

And there 2 configuration files `.env` configs `Airflow` and `backup_config.yml` defines which databases to back and how, they will be elaborated below:

`.env` file contains 3 variables:
- `TZ` - timezone, set to `Europe/Amsterdam`, default from airflow is `UTC`
- `FERNET_KEY` - key to encrypt/decrypt data in Airflow
- `BACKUP_CONFIG_URL` - url to download backup_config.yml file, if not found will fall back to `backup_config.yml` file in the same directory
- `BACKUP_CONFIG_TOKEN` - gitlab token to download backup_config.yml file

`backuo_config.yml` file contains configuration for databases to back up. It has a structure like this:
```yaml
databases:
  test.com_db:
    type: postgresql 
    host: mydbserver
    port: 5432
    database: mybackup
    user: mybackup
    password: "mybackup" 
    backup_type: "base" 
    tags: ["backup", "postgresql"]
    notification_provider: "slack" 
    notification_type: "all"
    notification_url: ""
  mysqltest:
    type: mysql
    host: mysqltest
    port: 3306
    database: mysqltest
    user: mysqltest
    password: "mysqltest"
    backup_type: "base"
    tags: ["backup", "mysql"]
    notification_provider: "slack" 
    notification_type: "failure" 
    notification_url: ""


backup_settings:
  base_backup_schedule: "0 23 * * *"
  retention_days: 7
  backup_location: /backup/data/postgresql1/mydb
  parallel_jobs: 2
```
Under `databases` key you can add as many databases as you want. Each database has the following keys:
- `type` - type of database (postgresql or mysql)
- `host` - host of database
- `port` - port of database
- `database` - database name
- `user` - user to connect to database
- `password` - password to connect to database
- `backup_type` - type of backup (base or incremental, supports only base now)
- `tags` - tags to shown on airflow panel under the specific job for specific database
- `notification_provider` - provider to send notification (slack only now)
- `notification_type` - type of notification (all or failure)
- `notification_url` - url to send notification (slack webhook only now)

Under `backup_settings` key you can set the following keys:
- `base_backup_schedule` - schedule for base backup, follow cron format
- `retention_days` - days to keep backups
- `backup_location` - location to store backups, each database will have a separate folder
- `parallel_jobs` - number of parallel jobs to run

Run comunica inside worker container to query the data from the nginx server.
```shell
docker run -it --rm -v sample_data_vol:/tmp --network traefik-public comunica/query-sparql http://nginx/locations.ttl -f /tmp/query.sparql -t "text/csv"
```