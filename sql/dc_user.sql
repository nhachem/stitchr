-- this is for a postgres deployment
create if not exists database dc;
drop if exists user dc;
create user dc with encrypted password 'dc';
-- alter user dc with password 'dc';
grant all privileges on database data_catalog to dc;

-- making superuser
-- These options range from CREATEDB, CREATEROLE, CREATEUSER, and even SUPERUSER. Additionally, most options also have a negative counterpart, informing the system that you wish to deny the user that particular permission. These option names are the same as their assignment counterpart, but are prefixed with NO (e.g. NOCREATEDB, NOCREATEROLE, NOSUPERUSER).

-- should technically not be needed
alter user dc with superuser;

-- connect as test and 
create schema data_catalog authorization dc;

set search_path=data_catalog,public;
