CREATE TABLE
    dataset_schema
    (
        id SERIAL NOT NULL,
        name CHARACTER VARYING(64)
    );

alter table dataset_schema add constraint dataset_schema_pk primary key(id);
create unique index concurrently name_schema_idx on dataset_schema(name);


insert into dataset_schema values(-1, 'fromSource');
