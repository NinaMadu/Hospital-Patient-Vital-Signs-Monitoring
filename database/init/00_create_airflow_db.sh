#!/bin/bash
# Runs once, on the first start of an empty Postgres volume.
# Airflow keeps its metadata in its own database, separate from the ward serving data.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE USER ${AIRFLOW_DB_USER} WITH PASSWORD '${AIRFLOW_DB_PASSWORD}';
  CREATE DATABASE ${AIRFLOW_DB} OWNER ${AIRFLOW_DB_USER};
EOSQL
