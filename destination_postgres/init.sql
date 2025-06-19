CREATE USER airbyte_user WITH PASSWORD 'Mop-391811';
GRANT CREATE, TEMPORARY ON DATABASE destination_db TO airbyte_user;
GRANT ALL ON SCHEMA public TO airbyte_user;