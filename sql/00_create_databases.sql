select 'create database manufacturing_dw'
where not exists (select from pg_database where datname = 'manufacturing_dw')\gexec

