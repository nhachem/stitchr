DROP TABLE IF EXISTS dataset_schema;
CREATE TABLE IF NOT EXISTS
    dataset_schema
    (
        id SERIAL NOT NULL,
        name CHARACTER VARYING(64)
    );

alter table dataset_schema add constraint dataset_schema_pk primary key(id);
create unique index concurrently name_schema_idx on dataset_schema(name);
create unique index name_schema_idx on dataset_schema(name);


insert into dataset_schema values(-1, 'fromSource');
