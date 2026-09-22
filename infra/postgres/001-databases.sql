-- CREATEDB is local-only: the integration suite creates isolated disposable databases.
CREATE ROLE wager_owner LOGIN PASSWORD 'wager-owner-local' NOSUPERUSER CREATEDB NOCREATEROLE;
CREATE ROLE wager LOGIN PASSWORD 'wager-local' NOSUPERUSER NOCREATEDB NOCREATEROLE;
CREATE DATABASE wager OWNER wager_owner;
REVOKE ALL ON DATABASE wager FROM PUBLIC;
GRANT CONNECT ON DATABASE wager TO wager;

\connect wager
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO wager;
-- Table/column privileges belong to the versioned application migrations.

\connect postgres
CREATE ROLE keycloak LOGIN PASSWORD 'keycloak-local' NOSUPERUSER NOCREATEDB NOCREATEROLE;
CREATE DATABASE keycloak OWNER keycloak;
REVOKE ALL ON DATABASE keycloak FROM PUBLIC;
