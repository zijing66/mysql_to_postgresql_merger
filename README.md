# mysql_to_postgresql_merger
Merges a MySQL dump data and a PgSQL dump data with the right format so it can be directly
imported to a new PostgreSQL database.

Data Dump using:
```
mysqldump -t --compatible=postgresql --default-character-set=utf8 -uroot -p gitlabhq_production -r mysql_gitlabhq_production_dataonly.mysql
pg_dump -a -U postgres gitlabhq_production -f postgres_gitlabhq_production_dataonly.dump
```

DDL Dump using:
```
pg_dump -s -U postgres gitlabhq_production -f postgres_gitlabhq_production_ddlonly.dump
```

Example:
```
# Execcute this script using:  
db_data_merge.py mysql_gitlabhq_production_dataonly.mysql postgres_gitlabhq_production_ddlonly.dump gitlabhq_production_merged.psql

# PostgreSQL Restore
psql -U postgres -d gitlabhq_production -f gitlabhq_production_merged.psql
```
