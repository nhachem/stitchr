CREATE SEQUENCE data_persistence_id_seq;

CREATE TABLE
    data_persistence
    (
        id INTEGER DEFAULT nextval('data_persistence_id_seq'::regclass) NOT NULL,
        name CHARACTER VARYING(64),
        persistence_type CHARACTER VARYING(20),
        storage_type CHARACTER VARYING(64),
        host CHARACTER VARYING(128),
        port INTEGER,
        db CHARACTER VARYING(128),
        "user" CHARACTER VARYING(64),
        pwd CHARACTER VARYING(64),
        driver CHARACTER VARYING(64),
        fetchsize INTEGER,
        sslmode CHARACTER VARYING(16) DEFAULT 'prefer'::CHARACTER VARYING,
        db_scope CHARACTER VARYING(16) DEFAULT 'open'::CHARACTER VARYING
    );


alter table data_persistence add constraint data_persistence_pk primary key (id);
create unique index ak_name_data_persistence_id on data_persistence(name);
alter table data_persistence add constraint ak_name_data_persistence_id unique using index  ak_name_data_persistence_id;


