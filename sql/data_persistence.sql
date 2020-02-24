CREATE SEQUENCE data_persistence_id1_seq;

CREATE TABLE
    data_persistence
    (
        id INTEGER DEFAULT nextval('data_persistence_id1_seq'::regclass) NOT NULL,
        name CHARACTER VARYING(64),
        persistence_type CHARACTER VARYING(20),
        storage_type CHARACTER VARYING(64),
        host CHARACTER VARYING(64),
        port INTEGER,
        db CHARACTER VARYING(64),
        "user" CHARACTER VARYING(64),
        pwd CHARACTER VARYING(64),
        driver CHARACTER VARYING(64),
        fetchsize INTEGER,
        sslmode CHARACTER VARYING(16) DEFAULT 'prefer'::CHARACTER VARYING
    );


alter table data_persistence add constraint data_persistence_pk primary key (id);
create unique index ak_name_data_persistence_id on data_persistence(name);
alter table data_persistence add constraint name_uk unique using index  ak_name_data_persistence_id;

-- dummy insert needed for FKs
INSERT INTO data_persistence (name, persistence_type, storage_type, host, port, db, "user", pwd, driver, fetchsize, id) VALUES ('dummy', 'NA', 'NA', 'NA', -1, 'NA', 'NA', 'NA', 'NA', -1, -1);

-- tmp space (default)
INSERT INTO data_persistence (name, persistence_type, storage_type, host, port, db, "user", pwd, driver, fetchsize, id) VALUES ('file0', 'file', 'file://', '/tmp/data', -1, 'stitchr', 'NA', 'NA', 'NA', -1, 0);

-- other for demo
INSERT INTO data_persistence (name, persistence_type, storage_type, host, port, db, "user", pwd, driver, fetchsize, id) VALUES ('postgresql4', 'jdbc', 'postgresql', '10.20.255.73', 5432, 'tpcds', 'tpcds', 'tpcds', 'org.postgresql.Driver', 10000, 4);
INSERT INTO data_persistence (name, persistence_type, storage_type, host, port, db, "user", pwd, driver, fetchsize, id) VALUES ('postgresql1', 'jdbc', 'postgresql', '216.195.29.110', 5432, 'tpcds', 'nabil', 'nabil', 'org.postgresql.Driver', 10000, 1);
INSERT INTO data_persistence (name, persistence_type, storage_type, host, port, db, "user", pwd, driver, fetchsize, id) VALUES ('postgresql5', 'jdbc', 'postgresql', 'localhost', 5432, 'publish', 'pub', 'pub', 'org.postgresql.Driver', 10000, 5);
INSERT INTO data_persistence (name, persistence_type, storage_type, host, port, db, "user", pwd, driver, fetchsize, id) VALUES ('datalake0', 'file', 'file://', '/tmp/demo/stitchr', -1, 'datalake', 'NA', 'NA', 'NA', -1, 2);
INSERT INTO data_persistence (name, persistence_type, storage_type, host, port, db, "user", pwd, driver, fetchsize, id) VALUES ('file3', 'file', 'file://', '/Users/nabilhachem', -1, 'data/demo', 'NA', 'NA', 'NA', -1, 3);
INSERT INTO data_persistence (name, persistence_type, storage_type, host, port, db, "user", pwd, driver, fetchsize, id) VALUES ('postgresql6', 'jdbc', 'postgresql', 'localhost', 5432, 'airflow', 'airflow', 'airflow', 'org.postgresql.Driver', 10000, 6);

