"echo

psql -v ON_ERROR_STOP=1 --username \"sa_airflow\" <<-EOSQL
GRANT ALL PRIVILEGES ON DATABASE youthbase TO sa_powerbi;
\c youthbase
GRANT SELECT ON ALL TABLES IN SCHEMA public TO sa_powerbi;
EOSQL"