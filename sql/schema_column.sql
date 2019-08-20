-- can be used to hget new ids 
CREATE SEQUENCE schema_column_id_seq start with 20;

CREATE TABLE
    schema_column
    (
        id INTEGER,
        column_name CHARACTER VARYING(64),
        column_position INTEGER,
        column_type CHARACTER VARYING(64),
        column_precision INTEGER,
        string_length INTEGER,
        is_nullable BOOLEAN
    );

alter table schema_column add constraint schema_column_pk  primary key (id, column_position);
create unique index concurrently uk_name_id_idx  on schema_column(column_name, id);
alter table schema_column add constraint name_id_uk unique using index uk_name_id_idx;

comment on table schema_column is 'holds the description of the schema and is referenced from dataset.schema_id';
comment on column schema_column.column_precision is 'holds the precision of the numeric type';
comment on column schema_column.string_length is 'length of varchar in general';
