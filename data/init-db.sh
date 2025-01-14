#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "{$POSTGRES_USER}" <<-EOSQL
    CREATE USER "{$_AIRFLOW_WWW_USER_USERNAME}" WITH PASSWORD '{$_AIRFLOW_WWW_USER_PASSWORD}';
    CREATE USER "{$POSTGRES_USER{}" WITH SUPERUSER PASSWORD '{$POSTGRES_PASSWORD}';
    CREATE DATABASE airflow;
    GRANT ALL PRIVILEGES ON DATABASE airflow TO {POSTGRES_USER};
    -- PostgreSQL 15 requires additional privileges:
    USE airflow;
    GRANT ALL ON SCHEMA public TO {$POSTGRES_USER};

    CREATE DATABASE football;
    GRANT ALL PRIVILEGES ON DATABASE football TO {$POSTGRES_USER};
    -- PostgreSQL 15 requires additional privileges:
    USE football;
    GRANT ALL ON SCHEMA public TO {$POSTGRES_USER};
EOSQL